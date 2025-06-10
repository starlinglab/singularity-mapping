package main

import (
	"database/sql"
	"encoding/base32"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	carv2 "github.com/ipld/go-car/v2"
)

var (
	carDir string
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Provide one arg: the path to the CAR storage directory")
		fmt.Println("Also set DATABASE_CONNECTION_STRING to point to the mysql database")
		return
	}
	carDir = os.Args[1]

	connStr := os.Getenv("DATABASE_CONNECTION_STRING")
	connStr = strings.TrimPrefix(connStr, "mysql://") // singularity connection strings have this

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		panic(err)
	}
	// https://github.com/go-sql-driver/mysql#important-settings
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	// TODO: foreign key constraints
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_range_car (
			file_range_id INTEGER,
			car_id INTEGER
		)
		`)
	if err != nil {
		panic(err)
	}

	if err := work(db); err != nil {
		panic(err)
	}
}

func work(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get all CIDs for the file ranges (1G blocks)
	// Map CID strings to the id column from file_ranges
	fileRangeIds := make(map[string]int)

	rows, err := tx.Query(`SELECT id, cid FROM file_ranges`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var cid []byte
		if err := rows.Scan(&id, &cid); err != nil {
			return err
		}
		fileRangeIds[cidToStr(cid)] = id
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Map CAR file names to IDs
	carFileIds := make(map[string]int)

	rows, err = tx.Query(`SELECT id, storage_path FROM cars`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var storagePath string
		if err := rows.Scan(&id, &storagePath); err != nil {
			return err
		}
		carFileIds[storagePath] = id
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// For each CAR check each CID
	for fileName, fileId := range carFileIds {
		fmt.Printf("Reading file: %s\n", fileName)

		err := func() error {
			f, err := os.Open(filepath.Join(carDir, fileName))
			if err != nil {
				return err
			}
			defer f.Close()
			rd, err := carv2.NewBlockReader(f)
			if err != nil {
				return err
			}
			for {
				blk, err := rd.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}

				fileRangeId, ok := fileRangeIds[blk.Cid().String()]
				if !ok {
					// CID is not associated with a file range
					continue
				}

				// Store the mapping between file range ID and car ID
				_, err = tx.Exec(`
					INSERT INTO file_range_car (file_range_id, car_id) VALUES (?, ?)
					`, fileRangeId, fileId)
				if err != nil {
					return err
				}
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

var multibaseBase32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// cidToStr converts a binary CID into its base32 representation.
func cidToStr(cid []byte) string {
	return "b" + multibaseBase32.EncodeToString(cid)
}
