package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

func main() {
	reportFilepath := flag.String("report-filepath", "", "if set, write the Markdown report to this file path")
	timeout := flag.Duration("timeout", 20*time.Minute, "overall timeout for the whole simulation")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	res, err := run(ctx)
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

	printSummary(os.Stdout, res)

	if failures := res.check(); len(failures) > 0 {
		fmt.Fprintf(os.Stderr, "\nFAIL: %d scenario(s) below expected minimum success rate:\n", len(failures))
		for _, f := range failures {
			fmt.Fprintf(os.Stderr, "  - %s: got %.1f%%, want >= %.1f%%\n", f.name, 100*f.got, 100*f.want)
		}
		os.Exit(1)
	}
	fmt.Println("\nPASS: all scenarios met their expected minimum success rate.")
}

// run executes every scenario concurrently (each in its own environment) and
// returns the collected results in scenario order.
func run(ctx context.Context) (scenarioResults, error) {
	scs := scenarios()
	out := make([]scenarioResult, len(scs))
	errs := make([]error, len(scs))

	var wg sync.WaitGroup
	for i, sc := range scs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[i], errs[i] = runScenario(ctx, sc)
		}()
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return scenarioResults{}, err
		}
	}
	return scenarioResults{entries: out}, nil
}

// printSummary writes a one-line-per-scenario PASS/FAIL overview.
func printSummary(w io.Writer, r scenarioResults) {
	fmt.Fprintln(w, "scenario summary (wgo app-level success):")
	for _, res := range r.entries {
		status := "PASS"
		if res.successRate() < res.sc.expectedMinSuccessRate {
			status = "FAIL"
		}
		fmt.Fprintf(w, "  [%s] %-42s %.1f%% (want >= %.1f%%)\n",
			status, res.sc.name, 100*res.successRate(), 100*res.sc.expectedMinSuccessRate)
	}
}
