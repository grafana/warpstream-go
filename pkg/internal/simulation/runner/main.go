package runner

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sync"
	"time"
)

func Main(factory WorkloadFactory) {
	reportFilepath := flag.String("report-filepath", "", "if set, write the Markdown report to this file path")
	jsonReportFilepath := flag.String("json-report-filepath", "", "if set, write scenario names and attempt/pass counts as JSON to this file path")
	timeout := flag.Duration("timeout", 20*time.Minute, "overall timeout for the whole simulation")
	parallelism := 0
	if factory != nil {
		parallelism = 4
	}
	parallel := flag.Int("parallel", parallelism, "maximum simultaneous scenarios; 0 runs all scenarios concurrently")
	defaultProfile := "standard"
	if factory != nil {
		defaultProfile = "all"
	}
	profile := flag.String("profile", defaultProfile, "scenario profile: standard, all, or mimir-investigation (last two require external workloads)")
	pattern := flag.String("scenario", "", "run scenarios whose names match this regular expression")
	check := flag.Bool("check", true, "exit unsuccessfully when a scenario misses an expectation")
	client := flag.String("client", "both", "client to run: both, wgo, or kgo")
	flag.Parse()
	clients, err := parseClients(*client)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	available, err := profileScenarios(*profile, factory != nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	selected, err := filterScenarios(*pattern, available)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	res, err := run(ctx, factory, *parallel, selected, clients)
	if err != nil {
		fmt.Fprintf(os.Stderr, "simulation failed: %v\n", err)
		os.Exit(1)
	}

	if *reportFilepath != "" {
		if err := os.WriteFile(*reportFilepath, []byte(newResultsReport(res).generateMarkdown()), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write report %q: %v\n", *reportFilepath, err)
			os.Exit(1)
		}
		fmt.Printf("wrote report to %s\n", *reportFilepath)
	}
	if *jsonReportFilepath != "" {
		data, err := newResultsReport(res).generateJSON()
		if err == nil {
			err = os.WriteFile(*jsonReportFilepath, data, 0o644)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "write JSON report %q: %v\n", *jsonReportFilepath, err)
			os.Exit(1)
		}
		fmt.Printf("wrote JSON report to %s\n", *jsonReportFilepath)
	}

	printSummary(os.Stdout, res)

	if failures := res.check(); len(failures) > 0 {
		fmt.Fprintf(os.Stderr, "\nFAIL: %d scenario expectation(s) not met:\n", len(failures))
		for _, f := range failures {
			fmt.Fprintf(os.Stderr, "  - %s\n", f)
		}
		if *check {
			os.Exit(1)
		}
		return
	}
	fmt.Println("\nPASS: all scenarios met their expectations.")
}

func selectScenarios(pattern string) ([]scenario, error) {
	return filterScenarios(pattern, scenarios())
}

func filterScenarios(pattern string, available []scenario) ([]scenario, error) {
	match, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("scenario filter: %w", err)
	}
	var selected []scenario
	for _, sc := range available {
		if match.MatchString(sc.name) {
			selected = append(selected, sc)
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("no scenarios match %q", pattern)
	}
	return selected, nil
}

func run(ctx context.Context, factory WorkloadFactory, parallel int, scs []scenario, clients clientSelection) (scenarioResults, error) {
	if parallel <= 0 {
		parallel = len(scs)
	}
	slots := make(chan struct{}, parallel)
	out := make([]scenarioResult, len(scs))
	errs := make([]error, len(scs))

	var wg sync.WaitGroup
	for i, sc := range scs {
		wg.Go(func() {
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			out[i], errs[i] = runScenario(ctx, sc, factory, clients)
		})
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return scenarioResults{}, err
		}
	}
	return scenarioResults{entries: out}, nil
}

// printSummary writes a one-line-per-scenario PASS/FAIL overview. Status is
// driven by checkResult so a scenario that misses a slow-fraction or
// success-delta gate cannot print as if it only had a success-rate floor.
func printSummary(w io.Writer, r scenarioResults) {
	fmt.Fprintln(w, "scenario summary (app-level success):")
	for _, res := range r.entries {
		missed := checkResult(res)
		status := "PASS"
		if len(missed) > 0 {
			status = "FAIL"
		}
		if res.clients == clientKgoOnly {
			fmt.Fprintf(w, "  [%s] %-42s kgo %.1f%%\n", status, res.sc.name, 100*res.kgoSuccessRate())
		} else {
			fmt.Fprintf(w, "  [%s] %-42s %.1f%% (want >= %.1f%%)\n",
				status, res.sc.name, 100*res.successRate(), 100*res.sc.expect.minWgoSuccessRate)
		}
		for _, f := range missed {
			fmt.Fprintf(w, "         %s\n", f.detail)
		}
	}
}
