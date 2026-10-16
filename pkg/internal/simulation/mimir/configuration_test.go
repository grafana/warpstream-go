package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/warpstream-go/pkg/internal/simulation/runner"
)

func TestConfiguration_Overlay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "overlay.yaml")
	require.NoError(t, os.WriteFile(path, []byte("limits:\n  max_global_series_per_user: 200000\ncustom:\n  endpoint: '{{.Address}}'\n  topic: '{{.Topic}}'\n"), 0600))
	base := "limits:\n  ingestion_artificial_delay: 500ms\n  ingestion_rate: 1000000\n"
	cfg := runner.WorkloadConfig{Topic: "primary"}
	out, fingerprint, err := applyConfigurationOverlay(base, cfg, "localhost:1000", path)
	require.NoError(t, err)
	require.Contains(t, out, "max_global_series_per_user: 200000")
	require.Contains(t, out, "ingestion_rate: 1000000")
	require.Contains(t, out, "localhost:1000")
	require.Contains(t, out, "topic: primary")
	_, secondFingerprint, err := applyConfigurationOverlay(base, cfg, "localhost:2000", path)
	require.NoError(t, err)
	require.Equal(t, fingerprint, secondFingerprint)
	require.Contains(t, fingerprint, "sha256:")
}

func TestConfiguration_ProtectedFields(t *testing.T) {
	for _, overlay := range []string{"target: all", "server:\n  http_listen_port: 9009", "ingest_storage: null", "distributor:\n  remote_timeout: 10s", "limits:\n  ingestion_artificial_delay: 0s"} {
		t.Run(overlay, func(t *testing.T) {
			cfg := runner.WorkloadConfig{Backend: "warpstream", Topic: "primary", Partitions: 50, WriteTimeout: 4 * time.Second, RemoteTimeout: 5 * time.Second, ArtificialDelay: 500 * time.Millisecond}
			base := distributorConfiguration(cfg, "localhost:9092", "http://localhost:8500", t.TempDir())
			path := filepath.Join(t.TempDir(), "overlay.yaml")
			require.NoError(t, os.WriteFile(path, []byte(overlay), 0600))
			_, _, err := applyConfigurationOverlay(base, cfg, "localhost:9092", path)
			require.ErrorContains(t, err, "harness-owned")
		})
	}
}

func TestConfiguration_InvalidOverlay(t *testing.T) {
	for _, overlay := range []string{"custom: '{{.Missing}}'", "custom: [invalid", "{{"} {
		path := filepath.Join(t.TempDir(), "overlay.yaml")
		require.NoError(t, os.WriteFile(path, []byte(overlay), 0600))
		_, _, err := applyConfigurationOverlay("target: distributor", runner.WorkloadConfig{}, "", path)
		require.Error(t, err)
	}
}
