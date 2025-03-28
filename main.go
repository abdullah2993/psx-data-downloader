package main

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	dbPath := flag.String("db", "market_data.db", "SQLite database path")
	backloadFrom := flag.String("backloadFrom", "", "Backload data from this date (YYYY-MM-DD)")
	backloadTo := flag.String("backloadTo", time.Now().Format("2006-01-02"), "Backload data to this date (YYYY-MM-DD)")
	flag.Parse()

	if *backloadFrom != "" {
		startDate, endDate, err := parseDateRange(*backloadFrom, *backloadTo)
		if err != nil {
			slog.Error("Invalid date range", "error", err)
			os.Exit(1)
		}

		slog.Info("Starting backload", "from", startDate, "to", endDate)
		backloadData(startDate, endDate, *dbPath)
		slog.Info("Backload completed")
	}

	pakistanLocation, err := loadPakistanTimeZone()
	if err != nil {
		slog.Error("Failed to load timezone", "error", err)
		os.Exit(1)
	}

	for {
		now := time.Now().In(pakistanLocation)
		nextRun := time.Date(now.Year(), now.Month(), now.Day(), 23, 0, 0, 0, pakistanLocation)
		if now.After(nextRun) {
			nextRun = nextRun.Add(24 * time.Hour)
		}

		slog.Info("Next scheduled run", "time", nextRun)
		time.Sleep(time.Until(nextRun))

		err = processMarketData(time.Now().In(pakistanLocation), *dbPath)
		if err != nil {
			slog.Error("Market data processing failed", "error", err)
		}
	}
}

func parseDateRange(from, to string) (time.Time, time.Time, error) {
	startDate, err := time.Parse("2006-01-02", from)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid start date: %w", err)
	}

	endDate := time.Now()
	if to != "" {
		endDate, err = time.Parse("2006-01-02", to)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid end date: %w", err)
		}
	}

	if startDate.After(endDate) {
		return time.Time{}, time.Time{}, fmt.Errorf("start date is after end date")
	}

	return startDate, endDate, nil
}

func loadPakistanTimeZone() (*time.Location, error) {
	return time.LoadLocation("Asia/Karachi")
}

func backloadData(startDate, endDate time.Time, dbPath string) {
	for currentDate := startDate; !currentDate.After(endDate); currentDate = currentDate.AddDate(0, 0, 1) {
		slog.Info("Backloading", "date", currentDate.Format("2006-01-02"))

		if err := processMarketData(currentDate, dbPath); err != nil {
			slog.Error("Backload failed", "date", currentDate, "error", err)
		} else {
			slog.Info("Backload successful", "date", currentDate)
		}
	}
}

func processMarketData(date time.Time, dbPath string) error {
	slog.Info("Processing market data", "date", date.Format("2006-01-02"))

	data, fileName, err := downloadAndExtractMarketData(date)
	if err != nil {
		return fmt.Errorf("data extraction failed: %w", err)
	}

	db, err := openDatabase(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := createTable(db); err != nil {
		return err
	}

	return insertMarketData(db, data, fileName, date)
}

func downloadAndExtractMarketData(date time.Time) ([]byte, string, error) {
	url := fmt.Sprintf("https://dps.psx.com.pk/download/mkt_summary/%s.Z", date.Format("2006-01-02"))
	slog.Info("Downloading", "url", url)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, "", fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("unexpected status: %s", resp.Status)
	}

	zipData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("failed reading response: %w", err)
	}

	// First try to process as ZIP
	zipReader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err == nil {
		// Successfully opened as ZIP, process files
		for _, file := range zipReader.File {
			f, err := file.Open()
			if err != nil {
				return nil, "", fmt.Errorf("failed opening zip file: %w", err)
			}
			defer f.Close()

			fileData, err := io.ReadAll(f)
			if err != nil {
				return nil, "", fmt.Errorf("failed reading zip file: %w", err)
			}

			return fileData, file.Name, nil
		}
		return nil, "", fmt.Errorf("no files found in zip")
	}

	// If not ZIP, try as GZIP
	gzipReader, err := gzip.NewReader(bytes.NewReader(zipData))
	if err == nil {
		defer gzipReader.Close()

		fileData, err := io.ReadAll(gzipReader)
		if err != nil {
			return nil, "", fmt.Errorf("failed reading gzip file: %w", err)
		}

		// Use the original filename if available, otherwise generate one
		filename := gzipReader.Name
		if filename == "" {
			filename = fmt.Sprintf("%s_decompressed", date.Format("2006-01-02"))
		}

		return fileData, filename, nil
	}

	// If neither worked, return an error
	return nil, "", fmt.Errorf("file is neither valid ZIP nor GZIP format")
}

func openDatabase(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	return db, nil
}

func createTable(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS market_data (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		date TEXT,
		symbol TEXT,
		code TEXT,
		company_name TEXT,
		open REAL,
		high REAL,
		low REAL,
		close REAL,
		volume INTEGER,
		previous_close REAL,
		UNIQUE(date, symbol)
	);`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed creating table: %w", err)
	}
	return nil
}

func insertMarketData(db *sql.DB, fileData []byte, fileName string, date time.Time) error {
	reader := csv.NewReader(bytes.NewReader(fileData))
	reader.Comma = '|'
	reader.FieldsPerRecord = -1

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("transaction start failed: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO market_data
		(date, symbol, code, company_name, open, high, low, close, volume, previous_close)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("statement preparation failed: %w", err)
	}
	defer stmt.Close()

	var recordCount, errorCount int
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(record) < 10 {
			errorCount++
			continue
		}

		_, err = stmt.Exec(date.Format("2006-01-02"), strings.TrimSpace(record[1]), strings.TrimSpace(record[2]),
			strings.TrimSpace(record[3]), parseFloat(record[4]), parseFloat(record[5]),
			parseFloat(record[6]), parseFloat(record[7]), parseInt(record[8]), parseFloat(record[9]))

		if err != nil {
			errorCount++
			continue
		}
		recordCount++
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	slog.Info("Data inserted", "records", recordCount, "errors", errorCount, "file", fileName)
	return nil
}

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return v
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(strings.TrimSpace(s))
	return v
}
