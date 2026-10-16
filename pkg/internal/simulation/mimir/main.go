package main

import (
	"flag"
	"os"

	"github.com/grafana/warpstream-go/pkg/internal/simulation/runner"
)

func main() {
	binaryPath := os.Getenv("MIMIR_BINARY")
	if binaryPath == "" {
		binaryPath = os.Getenv("SIMULATION_BINARY")
	}
	binary := flag.String("mimir-binary", binaryPath, "path to cmd/mimir built by build-mimir.sh")
	runner.Main(func(cfg runner.WorkloadConfig) (runner.Producer, error) { return newDistributor(*binary, cfg) })
}
