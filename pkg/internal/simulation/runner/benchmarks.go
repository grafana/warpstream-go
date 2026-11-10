package runner

// benchmarkScenarios are the scenarios whose expectations are meant to gate a PR once
// CI starts enforcing them. The report marks only these with a pass or fail icon; the
// rest are informational (their expectations are aspirational, or they probe a limit on
// purpose). Every one of them held 100% on every build in repeated runs, so a miss is a
// regression and not noise.
var benchmarkScenarios = map[string]bool{
	"all healthy":                                          true,
	"1 dropped leader":                                     true,
	"1 fast-failing agent":                                 true,
	"2 fast-failing agents":                                true,
	"1 slow agent":                                         true,
	"2 slow agents":                                        true,
	"1% failure rate across all agents":                    true,
	"1% slow bursts across all agents":                     true,
	"rolling restart":                                      true,
	"network blackhole":                                    true,
	"pod killed during writes":                             true,
	"moderately slow agents just below MaxSlowFraction":    true,
	"moderately slow agents just above MaxSlowFraction":    true,
	"barely hedged slow agents just below MaxSlowFraction": true,
	"barely hedged slow agents just above MaxSlowFraction": true,
	"investigation: concurrent healthy":                    true,
	"metadata unavailable while agents are replaced":       true,
	"prometheus: healthy control":                          true,
}
