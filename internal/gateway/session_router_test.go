package gateway

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"opencode2api/internal/config"
)

func TestSessionRouter_GetOrSetAndPromote(t *testing.T) {
	sr := NewSessionRouter(100 * time.Millisecond)

	// First turn uses default session
	s1 := sr.GetOrSet("conv_123", "ses_init_1")
	if s1 != "ses_init_1" {
		t.Fatalf("expected ses_init_1, got %s", s1)
	}

	// Next lookup before promotion returns same session
	s2 := sr.GetOrSet("conv_123", "ses_ignored")
	if s2 != "ses_init_1" {
		t.Fatalf("expected ses_init_1, got %s", s2)
	}

	// Promote winner session
	sr.Promote("conv_123", "ses_winner_2")
	s3 := sr.GetOrSet("conv_123", "ses_ignored")
	if s3 != "ses_winner_2" {
		t.Fatalf("expected ses_winner_2 after promotion, got %s", s3)
	}
}

func TestSessionRouter_Invalidate(t *testing.T) {
	sr := NewSessionRouter(100 * time.Millisecond)
	sr.GetOrSet("conv_abc", "ses_bad")
	sr.Invalidate("conv_abc", "ses_bad")

	sNext := sr.GetOrSet("conv_abc", "ses_fresh")
	if sNext != "ses_fresh" {
		t.Fatalf("expected ses_fresh after invalidation, got %s", sNext)
	}
}

func TestSessionRouter_Expiration(t *testing.T) {
	sr := NewSessionRouter(20 * time.Millisecond)
	sr.GetOrSet("conv_exp", "ses_old")
	time.Sleep(30 * time.Millisecond)

	sNew := sr.GetOrSet("conv_exp", "ses_new")
	if sNew != "ses_new" {
		t.Fatalf("expected ses_new after TTL expiration, got %s", sNew)
	}
}

func TestSessionRouter_ConcurrentAccess(t *testing.T) {
	sr := NewSessionRouter(50 * time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)
		go func(idx int) {
			defer wg.Done()
			sr.GetOrSet("conv_race", "ses_init")
		}(i)
		go func(idx int) {
			defer wg.Done()
			sr.Promote("conv_race", "ses_promoted")
		}(i)
		go func(idx int) {
			defer wg.Done()
			sr.Invalidate("conv_race", "ses_init")
		}(i)
	}
	wg.Wait()
}

func TestSessionRouter_AutomatedEviction(t *testing.T) {
	sr := NewSessionRouter(50 * time.Millisecond)

	// Pre-populate with 1050 expired entries
	oldTime := time.Now().Add(-100 * time.Millisecond)
	sr.mu.Lock()
	for i := 0; i < 1050; i++ {
		sr.entries[string(rune(i))] = sessionEntry{
			session:   "ses_expired",
			updatedAt: oldTime,
		}
	}
	sr.mu.Unlock()

	if sr.Len() != 1050 {
		t.Fatalf("expected 1050 entries, got %d", sr.Len())
	}

	// Calling GetOrSet triggers automated eviction because len >= 1000
	s := sr.GetOrSet("conv_fresh", "ses_fresh")
	if s != "ses_fresh" {
		t.Fatalf("expected ses_fresh, got %s", s)
	}

	// Expired entries should be purged, leaving only conv_fresh
	if sr.Len() != 1 {
		t.Fatalf("expected 1 entry after automated eviction, got %d", sr.Len())
	}

	// Test explicit PurgeExpired
	sr.mu.Lock()
	sr.entries["conv_exp1"] = sessionEntry{session: "ses_exp", updatedAt: time.Now().Add(-100 * time.Millisecond)}
	sr.entries["conv_exp2"] = sessionEntry{session: "ses_exp", updatedAt: time.Now().Add(-100 * time.Millisecond)}
	sr.mu.Unlock()

	if sr.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", sr.Len())
	}

	purged := sr.PurgeExpired()
	if purged != 2 {
		t.Fatalf("expected 2 purged, got %d", purged)
	}
	if sr.Len() != 1 {
		t.Fatalf("expected 1 entry left, got %d", sr.Len())
	}
}

func TestGateway_SessionRouterTTL(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ServerKeys = []string{"test-key"}
	cfg.Anonymous = true
	cfg.Hedge.SessionTTLMinutes = 45

	gw, err := New(cfg, slog.Default(), nil, nil)
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}
	if gw.sessionRouter == nil {
		t.Fatalf("expected sessionRouter to be non-nil")
	}
	if gw.sessionRouter.ttl != 45*time.Minute {
		t.Fatalf("expected sessionRouter ttl 45m, got %v", gw.sessionRouter.ttl)
	}

	// Test fallback when <= 0
	cfg.Hedge.SessionTTLMinutes = 0
	gw2, err := New(cfg, slog.Default(), nil, nil)
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}
	if gw2.sessionRouter.ttl != 30*time.Minute {
		t.Fatalf("expected fallback sessionRouter ttl 30m, got %v", gw2.sessionRouter.ttl)
	}
}

