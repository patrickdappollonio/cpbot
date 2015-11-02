package cpbot

import "time"

func isWeekday(t time.Time) bool {
	// Is the date given a saturday or sunday?
	if t.Weekday() == time.Sunday || t.Weekday() == time.Saturday {
		return false
	}

	return true
}
