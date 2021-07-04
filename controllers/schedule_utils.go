package controllers

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/robfig/cron"

	batch "github.com/jmarkp/controlled-job/api/v1"
)

// ScheduledEvent represents an instance of an event
type ScheduledEvent struct {
	Type          batch.EventType
	ScheduledTime time.Time
}

// Work out if we should currently be running, and when the current period
// of being started or stopped began
func (r *ControlledJobReconciler) calculateDesiredStatus(ctx context.Context, controlledJob *batch.ControlledJob, now time.Time) (shouldBeRunning bool, startOfCurrentPeriod time.Time, err error) {
	mostRecentEvent, err := findNearestEvent(ctx, controlledJob, now, directionPrevious)

	shouldBeRunning = mostRecentEvent.Type == batch.EventTypeStart

	return shouldBeRunning, mostRecentEvent.ScheduledTime, nil
}

// Calculate both the most recent event we were scheduled to perform but haven't yet (if any)
// and the next event in the schedule.
// The reconciler will use this to:
//  - immediately enqueue any missed event
//  - tell the controller to requeue us in time for the next scheduled event
func getNextEvent(ctx context.Context, controlledJob *batch.ControlledJob, now time.Time) (next *ScheduledEvent, err error) {
	return findNearestEvent(ctx, controlledJob, now, directionNext)
}

type eventDirection int

const (
	directionNext eventDirection = iota
	directionPrevious
)

// A controlledJob contains a list of events with different cron schedules
// We need to be able to find, amongst all those events, the most recent and the next
// scheduled event.
// This func searches either forward or backward for the nearest event to 'now' among the event specs
func findNearestEvent(ctx context.Context, controlledJob *batch.ControlledJob, now time.Time, direction eventDirection) (*ScheduledEvent, error) {

	var nearestEventTime time.Time = time.Time{}
	var nearestEventSpec batch.EventSpec

	// The EventSpec contains either a cron spec as string, or a human friendly format
	// In order to work out the next OR previous event in a cron schedule we need to
	//  1. Get a canonical cron spec string out of the event
	//  2. Get the cron library to parse it
	//  3. Cast it to a cron.SpecSchedule object (will always succeed in practice)
	mapEventToSpecSchedule := func(event batch.EventSpec) (*cron.SpecSchedule, error) {
		//  1. Get a canonical cron spec string out of the event
		cronSpec, err := event.AsCronSpec()
		if err != nil {
			return nil, err
		}
		//  2. Get the cron library to parse it
		schedule, err := cron.ParseStandard(cronSpec)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to parse cron schedule")
		}
		//  3. Cast it to a cron.SpecSchedule object (will always succeed in practice)
		specSchedule, ok := schedule.(*cron.SpecSchedule)
		if !ok {
			return nil, errors.Wrap(err, "Expected instance of SpecSchedule")
		}
		return specSchedule, nil
	}

	// Is the testTime time closer (in the desired direction)
	// to the reference time?
	eventIsNearer := func(testTime, referenceTime time.Time) bool {
		if referenceTime.IsZero() {
			return true
		}
		if direction == directionNext {
			return testTime.Before(referenceTime)
		}
		return testTime.After(referenceTime)
	}

	// We support multiple event specs on one ControlledJob
	// so we need to loop over each and for each event:
	//  1. Extract it's cron SpecSchedule
	//  2. Work out what the nearest adjacent event in that spec in the desired direction is
	//  3. Compare that to our current 'nearest' event
	for _, event := range controlledJob.Spec.Events {

		//  1. Extract it's cron SpecSchedule
		specSchedule, err := mapEventToSpecSchedule(event)
		if err != nil {
			return nil, err
		}

		//  2. Work out what the nearest adjacent event in that spec in the desired direction is
		var adjacentEventTime time.Time
		if direction == directionNext {
			adjacentEventTime = specSchedule.Next(now)
		} else {
			adjacentEventTime = cronPrev(specSchedule, now)
		}

		if adjacentEventTime.IsZero() {
			// Could not find a time to satisfy the schedule in that direction
			continue
		}

		//  3. Compare that to our current 'nearest' event
		if eventIsNearer(adjacentEventTime, nearestEventTime) {
			nearestEventTime = adjacentEventTime
			nearestEventSpec = event
		}
	}
	if nearestEventTime.IsZero() {
		return nil, errors.New("failed to find any nearest event")
	}
	return &ScheduledEvent{
		Type:          nearestEventSpec.Action,
		ScheduledTime: nearestEventTime,
	}, nil
}

// cronPrev returns the most recent previous time this schedule was activated, less than OR EQUAL TO the given
// time.  If no time can be found to satisfy the schedule, return the zero time.
// This is the inverse of SpecSchedule.Next
func cronPrev(s *cron.SpecSchedule, t time.Time) time.Time {
	// General approach:
	// For Month, Day, Hour, Minute, Second:
	// Check if the time value matches.  If yes, continue to the next field.
	// If the field doesn't match the schedule, then decrement the field until it matches.
	// While decrementing the field, a wrap-around brings it back to the end
	// of the field list (since it is necessary to re-verify previous field
	// values)

	// Start at the latest possible time (the current second).
	// t = t

	// This flag indicates whether a field has been decremented.
	subtracted := false

	// If no time is found within five years, return zero.
	yearLimit := t.Year() - 5

WRAP:
	if t.Year() < yearLimit {
		return time.Time{}
	}

	// Find the last applicable month.
	// If it's this month, then do nothing.
	for 1<<uint(t.Month())&s.Month == 0 {
		// If we have to subtract a month, reset the other parts to 0.
		if !subtracted {
			subtracted = true
			// Otherwise, set the date at the beginning (since the current time is irrelevant).
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
		}
		t = t.AddDate(0, -1, 0)

		// Wrapped around.
		if t.Month() == time.December {
			goto WRAP
		}
	}

	// Now get a day in that month.
	for !dayMatches(s, t) {
		if !subtracted {
			subtracted = true
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		}

		// If the current day is the 1st, then by subtracting 1
		// we might be going back a year (January->December) so
		// WRAP back to recheck the year limit
		if t.Day() == 1 {
			t = t.AddDate(0, 0, -1)
			goto WRAP
		}

		// Otherwise carry on
		t = t.AddDate(0, 0, -1)
	}

	for 1<<uint(t.Hour())&s.Hour == 0 {
		if !subtracted {
			subtracted = true
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
		}
		t = t.Add(-1 * time.Hour)

		if t.Hour() == 23 {
			goto WRAP
		}
	}

	for 1<<uint(t.Minute())&s.Minute == 0 {
		if !subtracted {
			subtracted = true
			t = t.Truncate(time.Minute)
		}
		t = t.Add(-1 * time.Minute)

		if t.Minute() == 59 {
			goto WRAP
		}
	}

	for 1<<uint(t.Second())&s.Second == 0 {
		if !subtracted {
			subtracted = true
			t = t.Truncate(time.Second)
		}
		t = t.Add(-1 * time.Second)

		if t.Second() == 59 {
			goto WRAP
		}
	}

	return t
}

const (
	// Set the top bit if a star was included in the expression.
	// Copied from cron package
	starBit = 1 << 63
)

// dayMatches returns true if the schedule's day-of-week and day-of-month
// restrictions are satisfied by the given time.
func dayMatches(s *cron.SpecSchedule, t time.Time) bool {
	var (
		domMatch bool = 1<<uint(t.Day())&s.Dom > 0
		dowMatch bool = 1<<uint(t.Weekday())&s.Dow > 0
	)
	if s.Dom&starBit > 0 || s.Dow&starBit > 0 {
		return domMatch && dowMatch
	}
	return domMatch || dowMatch
}
