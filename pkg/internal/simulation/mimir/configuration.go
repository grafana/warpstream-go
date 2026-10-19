package main

import (
	"bytes"
	"crypto/sha256"
	"debug/buildinfo"
	"fmt"
	"os"
	"reflect"
	"strings"
	"text/template"

	"go.yaml.in/yaml/v3"

	"github.com/grafana/warpstream-go/pkg/internal/simulation/runner"
)

type distributorOptions struct {
	overlay, logDirectory string
}

type configurationTemplate struct {
	Backend, Address, Topic, SecondaryTopic string
}

func applyConfigurationOverlay(base string, cfg runner.WorkloadConfig, address string, path string) (string, string, error) {
	if path == "" {
		return base, "none", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", fmt.Errorf("read configuration overlay: %w", err)
	}
	tmpl, err := template.New("overlay").Option("missingkey=error").Parse(string(data))
	if err != nil {
		return "", "", fmt.Errorf("parse configuration overlay: %w", err)
	}
	var expanded bytes.Buffer
	if err = tmpl.Execute(&expanded, configurationTemplate{Backend: cfg.Backend, Address: address, Topic: cfg.Topic, SecondaryTopic: cfg.SecondaryTopic}); err != nil {
		return "", "", fmt.Errorf("expand configuration overlay: %w", err)
	}
	var original, overlay map[string]any
	if err = yaml.Unmarshal([]byte(base), &original); err != nil {
		return "", "", err
	}
	if err = yaml.Unmarshal(expanded.Bytes(), &overlay); err != nil {
		return "", "", fmt.Errorf("decode configuration overlay: %w", err)
	}
	var merged map[string]any
	if err = yaml.Unmarshal([]byte(base), &merged); err != nil {
		return "", "", err
	}
	mergeConfiguration(merged, overlay)
	// These fields keep both lanes on the measured topology and declared deadlines.
	for _, key := range []string{"target", "server.http_listen_address", "server.http_listen_port", "server.grpc_listen_address", "server.grpc_listen_port", "server.log_format", "ingest_storage", "distributor.remote_timeout", "distributor.ring", "ingester.partition_ring", "ingester.ring", "memberlist", "limits.ingestion_artificial_delay"} {
		if !reflect.DeepEqual(configurationValue(original, key), configurationValue(merged, key)) {
			return "", "", fmt.Errorf("configuration overlay changes harness-owned field %q", key)
		}
	}
	encoded, err := yaml.Marshal(merged)
	if err != nil {
		return "", "", err
	}
	return string(encoded), fmt.Sprintf("sha256:%x", sha256.Sum256(data)), nil
}

func mergeConfiguration(base, overlay map[string]any) {
	for key, value := range overlay {
		child, childOK := value.(map[string]any)
		existing, existingOK := base[key].(map[string]any)
		if childOK && existingOK {
			mergeConfiguration(existing, child)
		} else {
			base[key] = value
		}
	}
}

func configurationValue(config map[string]any, path string) any {
	var value any = config
	for _, key := range strings.Split(path, ".") {
		child, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = child[key]
	}
	return value
}

func binaryIdentity(path string) string {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return "Go build information unavailable"
	}
	client := "not recorded"
	for _, dep := range info.Deps {
		if dep.Path != "github.com/grafana/warpstream-go" {
			continue
		}
		client = dep.Version
		if dep.Replace != nil {
			client = dep.Replace.Version
			if client == "" {
				client = "local replacement"
			}
		}
	}
	return fmt.Sprintf("%s; warpstream-go %s", info.GoVersion, client)
}
