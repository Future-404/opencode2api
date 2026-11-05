package models

import (
	"sync"
	"testing"
	"time"
)

func TestIsModelUnavailableReason(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     string
		expected bool
	}{
		{
			name:     "Authoritative 404 model_not_found",
			status:   404,
			body:     `{"error": {"code": "model_not_found", "message": "The model was not found"}}`,
			expected: true,
		},
		{
			name:     "Authoritative 400 model_retired",
			status:   400,
			body:     `{"error": {"message": "model has been retired"}}`,
			expected: true,
		},
		{
			name:     "Authoritative 410 model_deprecated",
			status:   410,
			body:     `{"error": "model_deprecated"}`,
			expected: true,
		},
		{
			name:     "429 Rate limited (transient, NOT unavailable)",
			status:   429,
			body:     `{"error": {"code": "rate_limit_exceeded", "message": "Quota exhausted"}}`,
			expected: false,
		},
		{
			name:     "500 Internal error (transient, NOT unavailable)",
			status:   500,
			body:     `{"error": "internal server error"}`,
			expected: false,
		},
		{
			name:     "503 Service unavailable (transient, NOT model unavailable)",
			status:   503,
			body:     `{"error": "service temporarily unavailable"}`,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsModelUnavailableReason(tt.status, tt.body)
			if got != tt.expected {
				t.Errorf("IsModelUnavailableReason(%d, %s) = %v, want %v", tt.status, tt.body, got, tt.expected)
			}
		})
	}
}

func TestAvailabilityRegistry_PassiveObservationAndManualLock(t *testing.T) {
	store, err := AvailabilityStore("")
	if err != nil {
		t.Fatalf("AvailabilityStore failed: %v", err)
	}

	model := "test-model-free"
	now := time.Now()

	// 1. Success observation marks available
	err = store.RecordPassiveObservation(model, true, 200, "ok", "zen", now)
	if err != nil {
		t.Fatalf("RecordPassiveObservation failed: %v", err)
	}
	if store.Disabled(model) {
		t.Fatalf("expected model to be enabled")
	}

	// 2. Transient 429 or 500 error does NOT disable model
	err = store.RecordPassiveObservation(model, false, 429, `{"error":"quota"}`, "zen", now)
	if err != nil {
		t.Fatalf("RecordPassiveObservation 429 failed: %v", err)
	}
	if store.Disabled(model) {
		t.Fatalf("model should NOT be disabled on 429")
	}

	// 3. Authoritative model_not_found disables model
	err = store.RecordPassiveObservation(model, false, 404, `{"error":"model_not_found"}`, "zen", now)
	if err != nil {
		t.Fatalf("RecordPassiveObservation 404 failed: %v", err)
	}
	if !store.Disabled(model) {
		t.Fatalf("model should be disabled on authoritative 404")
	}

	// 4. Operator manual lock
	err = store.SetManual(model, true, now)
	if err != nil {
		t.Fatalf("SetManual failed: %v", err)
	}
	if store.Disabled(model) {
		t.Fatalf("model under manual control should be enabled")
	}

	// 5. Subsequent passive observation cannot override manual control
	err = store.RecordPassiveObservation(model, false, 404, `{"error":"model_not_found"}`, "zen", now)
	if err != nil {
		t.Fatalf("RecordPassiveObservation failed: %v", err)
	}
	if store.Disabled(model) {
		t.Fatalf("manual lock should protect model from being disabled")
	}
}

func TestAvailabilityRegistry_ConcurrentAccess(t *testing.T) {
	store, err := AvailabilityStore("")
	if err != nil {
		t.Fatalf("AvailabilityStore failed: %v", err)
	}

	var wg sync.WaitGroup
	workers := 50
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			now := time.Now()
			for j := 0; j < iterations; j++ {
				m := "model-concurrent"
				_ = store.Get(m)
				_ = store.Disabled(m)
				if j%2 == 0 {
					_ = store.RecordPassiveObservation(m, true, 200, "ok", "test", now)
				} else {
					_ = store.RecordPassiveObservation(m, false, 429, "rate limit", "test", now)
				}
			}
		}(i)
	}

	wg.Wait()
}
