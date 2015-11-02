package cpbot

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Control struct {
	Name        string    `db:"name"`
	RangeStart  time.Time `db:"range_start"`
	RangeEnd    time.Time `db:"range_end"`
	CurrentDate time.Time `db:"range_position"`
}

func dbMustConnect(conn string) *sqlx.DB {
	// Connect to the database using the configuration provided
	database, err := sqlx.Connect("mysql", conn)

	// If there's an error, we should stop the system
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
