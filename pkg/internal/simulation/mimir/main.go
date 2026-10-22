package main

import (
	"flag"
	"os"
	"path/filepath"

	"github.com/grafana/warpstream-go/pkg/internal/simulation/runner"
)

func main() {
	binaryPath := os.Getenv("MIMIR_BINARY")
	if binaryPath == "" {
		binaryPath = os.Getenv("SIMULATION_BINARY")
	}
	prometheusBinary := flag.String("prometheus-binary", os.Getenv("PROMETHEUS_BINARY"), "path to the actual Prometheus binary for remote-write scenarios")
	binary := flag.String("mimir-binary", binaryPath, "path to cmd/mimir built by build-mimir.sh")
	overlay := flag.String("mimir-config-overlay", "", "optional YAML Go-template overlay for a compatible Mimir binary; never commit private configuration")
	logs := flag.String("mimir-log-directory", "", "if set, retain per-process logs and configuration under this directory")
	writers := flag.Int("mimir-writers", 0, "independent distributors per lane: 0 selects the profile default, 1 or 2 overrides it")
	runner.Main(func(cfg runner.WorkloadConfig) (runner.Producer, error) {
		options := distributorOptions{overlay: *overlay, logDirectory: *logs}
		if cfg.Prometheus {
			path := *prometheusBinary
			if path == "" {
				path = filepath.Join(filepath.Dir(*binary), "prometheus")
			}
			return newPrometheusWorkload(*binary, path, cfg, options)
		}
		return newDistributorWriters(*binary, cfg, options, *writers)
	})
}
