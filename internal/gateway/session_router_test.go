package gateway

import (
	"sync"
	"testing"
	"time"
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
