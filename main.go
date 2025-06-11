package main

import (
	"database/sql"
	"encoding/base32"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
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

	if err := run(db); err != nil {
		panic(err)
	}
}

type job struct {
	fileName string
	fileId   int
}
type result struct {
	fileRangeId int
	carId       int
	err         error
}

var fileRangeIds map[string]int

func run(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get all CIDs for the file ranges (1G blocks)
	// Map CID strings to the id column from file_ranges
	fileRangeIds = make(map[string]int)

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
	// This is done in parallel by workers
	// There is a fixed number of workers who are fed jobs (CAR names)
	// This setup assumes the work is CPU bound
	// TODO: but maybe it's I/O bound?

	numJobs := len(carFileIds)
	// Buffering to allow all jobs to be queued before workers finish
	jobs := make(chan job, numJobs)
	// No buffering should be necessary because results are processed as they come out
	// I'll add some buffering just for efficiency
	// TODO: is this right?
	results := make(chan result, 5)

	// Launch workers
	numWorkers := runtime.NumCPU()
	if numWorkers > numJobs {
		numWorkers = numJobs
	}
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, &wg, jobs, results)
	}

	// Close results channel when all workers are done
	// This way we can process results later on without having to know how many
	// results there will be.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Send jobs
	for fileName, fileId := range carFileIds {
		jobs <- job{fileName: fileName, fileId: fileId}
	}
	close(jobs)

	// Process results
	for result := range results {
		if result.err != nil {
			// Stop everything
			return err
		}
		// Store the mapping between file range ID and car ID
		_, err = tx.Exec(`
			INSERT INTO file_range_car (file_range_id, car_id) VALUES (?, ?)
			`, result.fileRangeId, result.carId)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func worker(id int, wg *sync.WaitGroup, jobs <-chan job, results chan<- result) {
	defer wg.Done()
	for j := range jobs {
		fmt.Printf("worker %d: processing %s\n", id, j.fileName)
		func() {
			f, err := os.Open(filepath.Join(carDir, j.fileName))
			if err != nil {
				results <- result{err: err}
				return
			}
			defer f.Close()
			rd, err := carv2.NewBlockReader(f)
			if err != nil {
				results <- result{err: err}
				return
			}
			for {
				blk, err := rd.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					results <- result{err: err}
					return
				}

				fileRangeId, ok := fileRangeIds[blk.Cid().String()]
				if !ok {
					// CID is not associated with a file range
					continue
				}

				// This CAR file contains a CID that matches a file range
				// So send it off
				results <- result{
					fileRangeId: fileRangeId,
					carId:       j.fileId,
				}
			}
		}()
	}
}

var multibaseBase32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// cidToStr converts a binary CID into its base32 representation.
func cidToStr(cid []byte) string {
	return "b" + multibaseBase32.EncodeToString(cid)
}
