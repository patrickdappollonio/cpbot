package cpbot

import (
	"database/sql"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/jmoiron/sqlx"
)

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

func dbMustConnect(conn string) *sqlx.DB {
	// Connect to the database using the configuration provided
	database, err := sqlx.Connect("mysql", conn)

	// If there's an error, we should stop the
	if err != nil {
		log.Fatal(err)
	}

	return database
}

func getConfig(database *sqlx.DB, name string) (Control, bool) {
	var (
		configuration Control
		shouldrun     = true
	)

	// Query the database to get the proper configuration
	err := database.Get(&configuration, "select * from scraper_control where name = ?", name)

	// If there was an error trying to retrieve the information
	// we log it to the output log
	if err != nil && err != sql.ErrNoRows {
		log.Println(err.Error())
	}

	// If the error was that there were no config
	// just send a response back telling that there's no
	// need to run
	if err == sql.ErrNoRows {
		shouldrun = false
	}

	// Return the fetched values from the database
	return configuration, shouldrun
}

func updateConfigDate(conf Control, database *sqlx.DB, name string) {
	// Set the date to be updated as the last date processed by the bot
	finaldate := conf.CurrentDate

	// If the last date processed is beyond the maximum date range
	// then update the final date as the last one from the range
	if conf.CurrentDate.After(conf.RangeEnd) {
		finaldate = conf.RangeEnd
	}

	// Update database with the new date based on the scraper name
	database.MustExec("update scraper_control set range_position = ? where name = ? limit 1", finaldate, name)
}

func isWeekday(t time.Time) bool {
	// Is the date given a saturday or sunday?
	if t.Weekday() == time.Sunday || t.Weekday() == time.Saturday {
		return false
	}

	return true
}
