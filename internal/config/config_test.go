package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestConfig_HedgeDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if !cfg.Hedge.Enabled {
		t.Fatalf("expected Hedge.Enabled to default to true")
	}
	if cfg.Hedge.DelayMs != 650 {
		t.Fatalf("expected Hedge.DelayMs default 650, got %d", cfg.Hedge.DelayMs)
	}
	if cfg.Hedge.IntervalMs != 120 {
		t.Fatalf("expected Hedge.IntervalMs default 120, got %d", cfg.Hedge.IntervalMs)
	}
	if cfg.Hedge.MaxCandidates != 5 {
		t.Fatalf("expected Hedge.MaxCandidates default 5, got %d", cfg.Hedge.MaxCandidates)
	}
}

func TestConfig_HedgeJSONUnmarshal(t *testing.T) {
	raw := `{
		"hedge": {
			"enabled": false,
			"delay_ms": 500,
			"interval_ms": 100,
			"max_candidates": 3
		}
	}`
	cfg := DefaultConfig()
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if cfg.Hedge.Enabled {
		t.Fatalf("expected Hedge.Enabled to be false")
	}
	if cfg.Hedge.DelayMs != 500 {
		t.Fatalf("expected Hedge.DelayMs 500, got %d", cfg.Hedge.DelayMs)
	}
	if cfg.Hedge.IntervalMs != 100 {
		t.Fatalf("expected Hedge.IntervalMs 100, got %d", cfg.Hedge.IntervalMs)
	}
	if cfg.Hedge.MaxCandidates != 3 {
		t.Fatalf("expected Hedge.MaxCandidates 3, got %d", cfg.Hedge.MaxCandidates)
	}
}

func TestConfig_HedgeValidation(t *testing.T) {
	tests := []struct {
		name      string
		modify    func(c *Config)
		errSubstr string
	}{
		{
			name: "negative delay_ms",
			modify: func(c *Config) {
				c.Hedge.DelayMs = -1
			},
			errSubstr: "hedge.delay_ms must not be negative",
		},
		{
			name: "negative interval_ms",
			modify: func(c *Config) {
				c.Hedge.IntervalMs = -10
			},
			errSubstr: "hedge.interval_ms must not be negative",
		},
		{
			name: "negative max_candidates",
			modify: func(c *Config) {
				c.Hedge.MaxCandidates = -5
			},
			errSubstr: "hedge.max_candidates must not be negative",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ServerKeys = []string{"test-server-key"}
			cfg.Anonymous = true
			tc.modify(&cfg)

			_, err := Normalize("", cfg)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errSubstr)
			}
			if !strings.Contains(err.Error(), tc.errSubstr) {
				t.Fatalf("expected error containing %q, got %v", tc.errSubstr, err)
			}
		})
	}
}
