package main

import (
	"database/sql"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"log"
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

	log.Print("starting")

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		log.Fatal(err)
	}
	// https://github.com/go-sql-driver/mysql#important-settings
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	// TODO: foreign key constraints
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_range_car (
			file_range_id INTEGER NOT NULL,
			car_id INTEGER NOT NULL
		)
		`)
	if err != nil {
		log.Fatal(err)
	}

	// Assume any errors are due to the index already existing
	log.Print("creating indexes")
	db.Exec(`CREATE INDEX idx_file_range_car_file_range_id ON file_range_car(file_range_id)`)
	db.Exec(`CREATE INDEX idx_file_range_car_car_id ON file_range_car(car_id)`)

	if err := run(db); err != nil {
		log.Fatal(err)
	}
	log.Print("done")
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

	// Get CIDs for the file ranges (1G blocks)
	// Map CID strings to the id column from file_ranges
	fileRangeIds = make(map[string]int)

	// Only get the file ranges that haven't already been found by this script
	log.Print("selecting file ranges")
	rows, err := tx.Query(`
		SELECT fr.id, fr.cid FROM file_ranges fr
		WHERE NOT EXISTS (
			SELECT 1 FROM file_range_car frc
			WHERE frc.file_range_id = fr.id
		)`)
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

	// Only get CAR files that haven't already been parsed by this script
	log.Print("selecting cars")
	rows, err = tx.Query(`
		SELECT c.id, c.storage_path FROM cars c
		WHERE NOT EXISTS (
			SELECT 1 FROM file_range_car frc
			WHERE frc.car_id = c.id
		)`)
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
	// But actually it's I/O bound
	// TODO: try using 2xCPU workers or more

	numJobs := len(carFileIds)

	// Buffering to allow all jobs to be queued before workers finish
	jobs := make(chan job, numJobs)

	// No buffering should be necessary because results are processed as they come out
	// I'll add some buffering just for efficiency
	// TODO: is this right?
	results := make(chan result, 5)

	// Count how many jobs have been completed for progress output
	jobDone := make(chan struct{}, numJobs)
	jobsDone := 0

	// Launch workers
	numWorkers := runtime.NumCPU()
	if numWorkers > numJobs {
		numWorkers = numJobs
	}
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, &wg, jobs, results, jobDone)
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
			return result.err
		}
		// Store the mapping between file range ID and car ID
		_, err = tx.Exec(`
			INSERT INTO file_range_car (file_range_id, car_id) VALUES (?, ?)
			`, result.fileRangeId, result.carId)
		if err != nil {
			return err
		}

		// Loop over all the job done updates
		newProgress := false
	loop:
		for {
			select {
			case <-jobDone:
				jobsDone++
				newProgress = true
			default:
				// Don't block if no job has completed, just wait for the next result
				break loop
			}
		}

		if newProgress {
			log.Printf("file progress: %d/%d\n", jobsDone, numJobs)
		}
	}

	log.Print("committing transaction")
	return tx.Commit()
}

func worker(id int, wg *sync.WaitGroup, jobs <-chan job, results chan<- result, jobDone chan<- struct{}) {
	defer wg.Done()
	for j := range jobs {
		func() {
			f, err := os.Open(filepath.Join(carDir, j.fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					// A file that can't be found is just a warning
					log.Printf("WARN: worker %d: can't find %s\n", id, j.fileName)
					return
				}
				results <- result{err: err}
				return
			}
			log.Printf("worker %d: processing %s\n", id, j.fileName)
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
		log.Printf("worker %d: finished\n", id)
		jobDone <- struct{}{}
	}
}

var multibaseBase32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// cidToStr converts a binary CID into its base32 representation.
func cidToStr(cid []byte) string {
	return "b" + multibaseBase32.EncodeToString(cid)
}
