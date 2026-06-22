package wgo

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConfig_Validate(t *testing.T) {
	validBase := Config{
		Address:       []string{"localhost:9092"},
		Topic:         "ingest",
		DialTimeout:   2 * time.Second,
		WriteTimeout:  10 * time.Second,
		BatchMaxBytes: 16_000_000,
		HealthCheck: HealthCheckConfig{
			SlowMultiplier:    2.0,
			MaxSlowFraction:   0.3,
			FaultyThreshold:   0.05,
			MaxFaultyFraction: 0.3,
		},
		Hedger: HedgerConfig{
			MinHedgeDelay:  10 * time.Millisecond,
			MaxHedgeAgents: 3,
		},
		Demoter: DemoterConfig{
			ProbeInterval: time.Second,
		},
		ClusterStatsTTL:         time.Second,
		MetadataRefreshInterval: 10 * time.Second,
		DirectProducer: KafkaDirectProducerConfig{
			ProduceRequestTimeout:         2 * time.Second,
			ProduceRequestTimeoutOverhead: time.Second,
		},
	}

	tests := map[string]struct {
		mutate     func(*Config)
		wantErrMsg string
	}{
		"valid config": {
			mutate: func(_ *Config) {},
		},
		"empty address list": {
			mutate:     func(c *Config) { c.Address = nil },
			wantErrMsg: "at least one broker address must be configured",
		},
		"empty topic": {
			mutate:     func(c *Config) { c.Topic = "" },
			wantErrMsg: "topic must not be empty",
		},
		"negative dial timeout": {
			mutate:     func(c *Config) { c.DialTimeout = -1 },
			wantErrMsg: "dial timeout must be non-negative",
		},
		"zero write timeout": {
			mutate:     func(c *Config) { c.WriteTimeout = 0 },
			wantErrMsg: "write timeout must be positive",
		},
		"negative write timeout": {
			mutate:     func(c *Config) { c.WriteTimeout = -1 },
			wantErrMsg: "write timeout must be positive",
		},
		"zero batch max bytes": {
			mutate:     func(c *Config) { c.BatchMaxBytes = 0 },
			wantErrMsg: "batch max bytes must be positive",
		},
		"negative batch max bytes": {
			mutate:     func(c *Config) { c.BatchMaxBytes = -1 },
			wantErrMsg: "batch max bytes must be positive",
		},
		"batch max bytes over ceiling": {
			mutate:     func(c *Config) { c.BatchMaxBytes = batchMaxBytesCeiling + 1 },
			wantErrMsg: "batch max bytes must not exceed 1073741824",
		},
		"negative linger": {
			mutate:     func(c *Config) { c.Linger = -1 },
			wantErrMsg: "linger must be non-negative",
		},
		"TLS enabled without TLS config": {
			mutate:     func(c *Config) { c.TLSEnabled = true; c.TLSConfig = nil },
			wantErrMsg: "TLS config must be set when TLS is enabled",
		},
		"health check slow multiplier below 1": {
			mutate:     func(c *Config) { c.HealthCheck.SlowMultiplier = 0.5 },
			wantErrMsg: "health check: health check slow multiplier must be >= 1",
		},
		"zero health check slow multiplier": {
			mutate:     func(c *Config) { c.HealthCheck.SlowMultiplier = 0 },
			wantErrMsg: "health check: health check slow multiplier must be >= 1",
		},
		"health check max slow fraction below 0": {
			mutate:     func(c *Config) { c.HealthCheck.MaxSlowFraction = -0.1 },
			wantErrMsg: "health check: health check max slow fraction must be between 0 and 1",
		},
		"health check max slow fraction above 1": {
			mutate:     func(c *Config) { c.HealthCheck.MaxSlowFraction = 1.1 },
			wantErrMsg: "health check: health check max slow fraction must be between 0 and 1",
		},
		"zero dial timeout is valid": {
			mutate: func(c *Config) { c.DialTimeout = 0 },
		},
		"zero linger is valid": {
			mutate: func(c *Config) { c.Linger = 0 },
		},
		"TLS enabled with TLS config is valid": {
			mutate: func(c *Config) { c.TLSEnabled = true; c.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12} },
		},
		"TLS disabled without TLS config is valid": {
			mutate: func(c *Config) { c.TLSEnabled = false; c.TLSConfig = nil },
		},
		"health check max slow fraction of exactly 1 is valid": {
			mutate: func(c *Config) { c.HealthCheck.MaxSlowFraction = 1.0 },
		},
		"zero faulty threshold is rejected": {
			mutate:     func(c *Config) { c.HealthCheck.FaultyThreshold = 0 },
			wantErrMsg: "health check: health check faulty threshold must be greater than 0 and at most 1",
		},
		"negative faulty threshold is rejected": {
			mutate:     func(c *Config) { c.HealthCheck.FaultyThreshold = -0.1 },
			wantErrMsg: "health check: health check faulty threshold must be greater than 0 and at most 1",
		},
		"faulty threshold of exactly 1 is valid": {
			mutate: func(c *Config) { c.HealthCheck.FaultyThreshold = 1.0 },
		},
		"write timeout below produce request timeout plus overhead is rejected": {
			mutate:     func(c *Config) { c.WriteTimeout = 2 * time.Second },
			wantErrMsg: "write timeout (2s) must be at least the produce request timeout plus overhead (3s)",
		},
		"write timeout equal to produce request timeout plus overhead is valid": {
			mutate: func(c *Config) { c.WriteTimeout = 3 * time.Second },
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := validBase
			tc.mutate(&cfg)
			err := cfg.Validate()
			if tc.wantErrMsg != "" {
				require.EqualError(t, err, tc.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDefaultConfig_PassesValidation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Address = []string{"localhost:9092"}
	cfg.Topic = "ingest"

	require.NoError(t, cfg.Validate())

	// The defaults sit exactly on Validate's WriteTimeout >= per-attempt deadline
	// boundary, so any future change to these three must keep the relation intact.
	require.Equal(t, DefaultWriteTimeout, DefaultProduceRequestTimeout+DefaultProduceRequestTimeoutOverhead)
}

func TestOptions_ApplyToConfig(t *testing.T) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	cfg := DefaultConfig()
	for _, o := range []Opt{
		WithAddress("a:9092", "b:9092"),
		WithTopic("ingest"),
		WithClientID("cid"),
		WithDialTimeout(time.Second),
		WithWriteTimeout(9 * time.Second),
		WithTLSConfig(tlsCfg),
		WithSASL(kgo.ClientID("sasl")),
		WithLinger(2 * time.Millisecond),
		WithBatchMaxBytes(123),
		WithProduceRequestTimeout(3 * time.Second),
		WithProduceRequestTimeoutOverhead(4 * time.Second),
		WithHealthCheckSlowMultiplier(5),
		WithHealthCheckMaxSlowFraction(0.5),
		WithHealthCheckFaultyThreshold(0.6),
		WithHealthCheckMaxFaultyFraction(0.7),
		WithHedgerMinHedgeDelay(8 * time.Millisecond),
		WithHedgerMaxHedgeAgents(9),
		WithDemoterProbeInterval(11 * time.Second),
		WithClusterStatsTTL(12 * time.Second),
		WithMetadataRefreshInterval(13 * time.Second),
	} {
		o.apply(&cfg)
	}

	assert.Equal(t, []string{"a:9092", "b:9092"}, cfg.Address)
	assert.Equal(t, "ingest", cfg.Topic)
	assert.Equal(t, "cid", cfg.ClientID)
	assert.Equal(t, time.Second, cfg.DialTimeout)
	assert.Equal(t, 9*time.Second, cfg.WriteTimeout)
	assert.True(t, cfg.TLSEnabled)
	assert.Same(t, tlsCfg, cfg.TLSConfig)
	assert.Len(t, cfg.SASLOptions, 1)
	assert.Equal(t, 2*time.Millisecond, cfg.Linger)
	assert.Equal(t, int32(123), cfg.BatchMaxBytes)
	assert.Equal(t, 3*time.Second, cfg.DirectProducer.ProduceRequestTimeout)
	assert.Equal(t, 4*time.Second, cfg.DirectProducer.ProduceRequestTimeoutOverhead)
	assert.Equal(t, 5.0, cfg.HealthCheck.SlowMultiplier)
	assert.Equal(t, 0.5, cfg.HealthCheck.MaxSlowFraction)
	assert.Equal(t, 0.6, cfg.HealthCheck.FaultyThreshold)
	assert.Equal(t, 0.7, cfg.HealthCheck.MaxFaultyFraction)
	assert.Equal(t, 8*time.Millisecond, cfg.Hedger.MinHedgeDelay)
	assert.Equal(t, 9, cfg.Hedger.MaxHedgeAgents)
	assert.Equal(t, 11*time.Second, cfg.Demoter.ProbeInterval)
	assert.Equal(t, 12*time.Second, cfg.ClusterStatsTTL)
	assert.Equal(t, 13*time.Second, cfg.MetadataRefreshInterval)
}

func TestOptions_UnsetFieldKeepsDefault(t *testing.T) {
	cfg := DefaultConfig()
	WithDialTimeout(time.Minute).apply(&cfg)

	assert.Equal(t, time.Minute, cfg.DialTimeout)
	// An untouched field keeps the DefaultConfig value.
	assert.Equal(t, DefaultWriteTimeout, cfg.WriteTimeout)
}
