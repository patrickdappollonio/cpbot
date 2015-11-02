package cpbot

import "time"

type Control struct {
	Name        string    `db:"name"`
	RangeStart  time.Time `db:"range_start"`
	RangeEnd    time.Time `db:"range_end"`
	CurrentDate time.Time `db:"range_position"`
}

type Setup struct {
	BotName          string
	DaysToRetrieve   int
	StartDelayAmout  int
	StartDelayFormat time.Duration
	DBConnection     string
}
