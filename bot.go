package cpbot

import (
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Setup struct {
	BotName          string
	DaysToRetrieve   int
	StartDelayAmout  int
	StartDelayFormat time.Duration
	DBConnection     string
}

func (s Setup) Do(fn func(config Control, db *sqlx.DB)) {
	// Connect to the database
	database := dbMustConnect(s.DBConnection)
	defer database.Close()

	// Retrieve the current scraper configuration
	conf, shouldrun := getConfig(database, s.BotName)
	if !shouldrun {
		return
	}

	// Create a pool of workers
	var wg sync.WaitGroup

	// Execute that worker
	for pos := 0; pos < s.DaysToRetrieve; pos++ {
		// Add a delta worker
		wg.Add(1)

		// Execute the task
		go func(inc int, current Control, db *sqlx.DB) {
			// Remove one task from the queue
			defer wg.Done()

			// Let's prevent running on weekends
			if !isWeekday(current.CurrentDate) {
				return
			}

			// Let's not overflow the date
			if current.CurrentDate.After(current.RangeEnd) {
				return
			}

			// Add a sleep function to delay n * time
			time.Sleep(time.Duration(inc*s.StartDelayAmout) * s.StartDelayFormat)

			// Execute the worker's task
			fn(current, db)
		}(pos, conf, database)

		// Increment day by one
		conf.CurrentDate = conf.CurrentDate.AddDate(0, 0, 1)
	}

	// Update the date to the new generated date
	updateConfigDate(conf, database, s.BotName)

	// Wait for all workers to finish their job
	wg.Wait()
}
