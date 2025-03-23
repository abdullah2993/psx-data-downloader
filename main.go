package main

import (
	"archive/zip"
	"bytes"
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
	// Define command line flags
	dbPath := flag.String("db", "market_data.db", "SQLite database path")
	backloadFrom := flag.String("backloadFrom", "", "Backload data from this date (YYYY-MM-DD)")
	backloadTo := flag.String("backloadTo", time.Now().Format("2006-01-02"), "Backload data to this date (YYYY-MM-DD)")
	flag.Parse()

	// Check if in backload mode
	if *backloadFrom != "" {
		// Parse start date for backloading
		startDate, err := time.Parse("2006-01-02", *backloadFrom)
		if err != nil {
			slog.Error("Invalid backload start date format", "error", err, "date", *backloadFrom)
			os.Exit(1)
		}

		endDate := time.Now()
		if *backloadTo != "" {
			endDate, err = time.Parse("2006-01-02", *backloadTo)
			if err != nil {
				slog.Error("Invalid backload end date format", "error", err, "date", *backloadTo)
				os.Exit(1)
			}

		}
		if startDate.After(endDate) {
			slog.Error("Backload start date cannot be in the future", "startDate", *backloadFrom)
			os.Exit(1)
		}

		slog.Info("Starting backload operation",
			"fromDate", startDate.Format("2006-01-02"),
			"toDate", endDate.Format("2006-01-02"))

		backloadData(startDate, endDate, *dbPath)

		slog.Info("Backload operation completed successfully")
	}

	// Define Pakistan time zone (UTC+5)
	pakistanLocation, err := time.LoadLocation("Asia/Karachi")
	if err != nil {
		slog.Error("Failed to load timezone for pakistan", "error", err)
		os.Exit(1)
	}

	for {
		// Get the current time in Pakistan Time Zone
		now := time.Now().In(pakistanLocation)

		// Calculate the next 11 PM Pakistan Time
		nextRun := time.Date(now.Year(), now.Month(), now.Day(), 23, 0, 0, 0, pakistanLocation)
		if now.After(nextRun) {
			// If it's already past 11 PM today, schedule for tomorrow
			nextRun = nextRun.Add(24 * time.Hour)
		}

		// Calculate the duration to sleep until the next 11 PM
		sleepDuration := time.Until(nextRun)
		slog.Info("Scheduling next run", "duration", nextRun)

		time.Sleep(sleepDuration)

		current := time.Now().In(pakistanLocation)
		// Run the task at 11 PM
		err = processMarketData(current, *dbPath)
		if err != nil {
			slog.Error("Failed to process market data", "date", current.Format("2006-01-02"), "error", err)
		}
	}
}

// backloadData downloads and processes data for a range of dates
func backloadData(startDate, endDate time.Time, dbPath string) {
	currentDate := startDate
	for currentDate.Before(endDate) {
		slog.Info("Starting backload for", "date", currentDate.Format("2006-01-02"))

		err := processMarketData(currentDate, dbPath)
		if err != nil {
			slog.Error("Failed to backload data", "date", currentDate.Format("2006-01-02"))
		} else {
			slog.Info("Successfully backloaded date", "date", currentDate.Format("2006-01-02"))
		}

		currentDate = currentDate.AddDate(0, 0, 1)
	}
}

func processMarketData(date time.Time, dbPath string) error {
	slog.Info("Processing market data", "date", date.Format("2006-01-02"), "db", dbPath)
	// 1. Download the zip file
	url := fmt.Sprintf("https://dps.psx.com.pk/download/mkt_summary/%s.Z", date.Format("2006-01-02"))
	slog.Info("Downloading market data", "url", url)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed with status: %s", resp.Status)
	}
	// Read response body
	zipData, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	slog.Info("Downloaded zip file", "size", len(zipData), "date", date.Format("2006-01-02"))

	// 2. Extract the zip file
	zipReader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return fmt.Errorf("failed to parse zip file: %w", err)
	}

	// Find the file in the archive
	var fileData []byte
	var fileName string
	for _, file := range zipReader.File {
		fileName = file.Name
		slog.Info("Processing file from archive", "filename", fileName, "date", date.Format("2006-01-02"))

		f, err := file.Open()
		if err != nil {
			return fmt.Errorf("failed to open file within zip: %w", err)
		}

		fileData, err = io.ReadAll(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("failed to read file within zip: %w", err)
		}

		// We only process the first file
		break
	}

	if fileData == nil {
		return fmt.Errorf("no files found in the archive")
	}

	// 3. Parse the data using CSV parser
	reader := csv.NewReader(bytes.NewReader(fileData))
	reader.Comma = '|'          // Set delimiter to pipe
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	// 4. Create or open the SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// 5. Create table if it doesn't exist
	createTableSQL := `
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

	_, err = db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// 6. Insert data into the database
	slog.Info("Inserting data into database", "date", date.Format("2006-01-02"))
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt, err := tx.Prepare(`
	INSERT OR REPLACE INTO market_data
	(date, symbol, code, company_name, open, high, low, close, volume, previous_close)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	recordCount := 0
	errorCount := 0

	// Read and process all records
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Warn("Error reading CSV record", "error", err, "date", date.Format("2006-01-02"))
			errorCount++
			continue
		}

		// Skip empty lines
		if len(record) == 0 {
			continue
		}

		// Ensure we have enough fields
		if len(record) < 10 {
			slog.Debug("Skipping record with insufficient fields", "record", record, "fieldCount", len(record))
			errorCount++
			continue
		}

		// Extract fields
		recordDate := strings.TrimSpace(record[0])

		recordParsedDate, err := time.Parse("02Jan2006", recordDate)
		if err != nil {
			slog.Error("Failed to parse record date", "error", err, "record", record)
			errorCount++
			continue
		}

		recordDate = recordParsedDate.Format("2006-01-02")

		symbol := strings.TrimSpace(record[1])
		code := strings.TrimSpace(record[2])
		companyName := strings.TrimSpace(record[3])

		// Parse numeric values
		open, _ := parseNumeric(record[4])
		high, _ := parseNumeric(record[5])
		low, _ := parseNumeric(record[6])
		close, _ := parseNumeric(record[7])
		volume, _ := parseInt(record[8])
		previousClose, _ := parseNumeric(record[9])

		// Insert record
		_, err = stmt.Exec(recordDate, symbol, code, companyName, open, high, low, close, volume, previousClose)
		if err != nil {
			slog.Error("Failed to insert record", "error", err, "symbol", symbol, "date", date.Format("2006-01-02"))
			errorCount++
			continue
		}

		recordCount++
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	slog.Info("Database operation completed",
		"date", date.Format("2006-01-02"),
		"recordsInserted", recordCount,
		"errorCount", errorCount,
		"filename", fileName)

	slog.Info("Successfully processed market data", "date", date.Format("2006-01-02"))
	return nil
}

// Helper function to parse numeric values that handles both float and int
func parseNumeric(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" || s == "0.0" {
		return 0.0, nil
	}
	return strconv.ParseFloat(s, 64)
}

// Helper function specifically for parsing integers
func parseInt(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	return strconv.Atoi(s)
}
