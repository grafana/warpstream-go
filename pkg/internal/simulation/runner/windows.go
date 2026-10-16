package runner

import "time"

type observationWindow struct {
	name     string
	from, to time.Duration
}

type windowResult struct {
	window   observationWindow
	wgo, kgo observationsSummary
}

func (s observationsSnapshot) scheduledBetween(from, to time.Time) observationsSnapshot {
	out := observationsSnapshot{}
	for _, ob := range s.list {
		if !ob.scheduled.Before(from) && ob.scheduled.Before(to) {
			out.list = append(out.list, ob)
		}
	}
	return out
}

func summarizeWindows(windows []observationWindow, start time.Time, wgo, kgo observationsSnapshot) []windowResult {
	out := make([]windowResult, 0, len(windows))
	for _, window := range windows {
		from, to := start.Add(window.from), start.Add(window.to)
		out = append(out, windowResult{window: window, wgo: wgo.scheduledBetween(from, to).summary(), kgo: kgo.scheduledBetween(from, to).summary()})
	}
	return out
}
