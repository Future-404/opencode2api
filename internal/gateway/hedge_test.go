package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
)

// trackingBody wraps a bytes.Reader with an atomic close counter.
type trackingBody struct {
	io.Reader
	closed atomic.Bool
}

func (tb *trackingBody) Close() error {
	tb.closed.Store(true)
	return nil
}

func TestHedgedPipeline_FirstWinnerWins(t *testing.T) {
	var callCount int32
	var primaryCancelled atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := atomic.AddInt32(&callCount, 1)
		if idx == 1 {
			// Simulate slow primary
			select {
			case <-time.After(400 * time.Millisecond):
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("slow"))
			case <-r.Context().Done():
				primaryCancelled.Store(true)
			}
			return
		}

		// Candidate 2 finishes fast
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("fast"))
	}))
	defer server.Close()

	cfg := config.HedgeConfig{
		Enabled:       true,
		DelayMs:       40, // Trigger candidate 1 quickly
		IntervalMs:    50,
		MaxCandidates: 3,
	}

	attemptFn := func(ctx context.Context, attemptNum int, session string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-Session", session)
		return server.Client().Do(req)
	}

	resp, winningSession, err, attempts := executeHedgedPipeline(context.Background(), cfg, "ses_init_primary", attemptFn)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if resp == nil {
		t.Fatalf("expected non-nil response")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(bodyBytes) != "fast" {
		t.Fatalf("expected body 'fast', got %s", string(bodyBytes))
	}

	if winningSession == "ses_init_primary" {
		t.Fatalf("expected winning session to be a hedged candidate, got primary %s", winningSession)
	}
	if attempts < 2 {
		t.Fatalf("expected at least 2 attempts dispatched, got %d", attempts)
	}

	// Verify that the slow primary branch context was actively cancelled
	time.Sleep(50 * time.Millisecond)
	if !primaryCancelled.Load() {
		t.Fatalf("expected slow primary branch to have been cancelled via context")
	}
}

func TestHedgedPipeline_FastFail(t *testing.T) {
	var callCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := atomic.AddInt32(&callCount, 1)
		if idx == 1 {
			// Primary returns 429 immediately
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("rate limit"))
			return
		}
		// Candidate 1 succeeds immediately
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("candidate1_ok"))
	}))
	defer server.Close()

	// DelayMs is intentionally long (1000ms); fast-fail should launch Candidate 1 in <100ms
	cfg := config.HedgeConfig{
		Enabled:       true,
		DelayMs:       1000,
		IntervalMs:    200,
		MaxCandidates: 3,
	}

	attemptFn := func(ctx context.Context, attemptNum int, session string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, nil)
		if err != nil {
			return nil, err
		}
		return server.Client().Do(req)
	}

	start := time.Now()
	resp, winningSession, err, attempts := executeHedgedPipeline(context.Background(), cfg, "ses_primary", attemptFn)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if resp == nil {
		t.Fatalf("expected non-nil response")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "candidate1_ok" {
		t.Fatalf("expected 'candidate1_ok', got '%s'", string(body))
	}

	if elapsed > 500*time.Millisecond {
		t.Fatalf("fast-fail took %v; expected Candidate 1 to be dispatched immediately without waiting for 1000ms DelayMs", elapsed)
	}

	if winningSession == "ses_primary" {
		t.Fatalf("expected winning session to be hedged candidate, got %s", winningSession)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts dispatched, got %d", attempts)
	}
}

func TestHedgedPipeline_AllCandidatesFail(t *testing.T) {
	var callCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("rate limit"))
	}))
	defer server.Close()

	cfg := config.HedgeConfig{
		Enabled:       true,
		DelayMs:       20,
		IntervalMs:    20,
		MaxCandidates: 3,
	}

	attemptFn := func(ctx context.Context, attemptNum int, session string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, nil)
		if err != nil {
			return nil, err
		}
		return server.Client().Do(req)
	}

	resp, lastSession, err, attempts := executeHedgedPipeline(context.Background(), cfg, "ses_primary", attemptFn)
	if resp == nil {
		t.Fatalf("expected last non-nil 429 response when all fail, got nil (err: %v)", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected status 429, got %d", resp.StatusCode)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
	if lastSession == "" {
		t.Fatalf("expected non-empty lastSession")
	}
}

func TestHedgedPipeline_LosingBranchBodyDrained(t *testing.T) {
	body0 := &trackingBody{Reader: bytes.NewReader([]byte("body0"))}
	body1 := &trackingBody{Reader: bytes.NewReader([]byte("body1"))}

	cfg := config.HedgeConfig{
		Enabled:       true,
		DelayMs:       20,
		IntervalMs:    50,
		MaxCandidates: 2,
	}

	attemptFn := func(ctx context.Context, attemptNum int, session string) (*http.Response, error) {
		if attemptNum == 0 {
			// Slower attempt
			select {
			case <-time.After(80 * time.Millisecond):
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       body0,
				}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		// Faster attempt
		time.Sleep(10 * time.Millisecond)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       body1,
		}, nil
	}

	resp, _, err, _ := executeHedgedPipeline(context.Background(), cfg, "ses_primary", attemptFn)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if resp == nil {
		t.Fatalf("expected non-nil response")
	}

	// Winner body should NOT be closed yet
	if body1.closed.Load() {
		t.Fatalf("expected winning body1 to be open before caller closes it")
	}

	// Caller closes winning response body
	httpx.DrainAndClose(resp.Body)
	if !body1.closed.Load() {
		t.Fatalf("expected winning body1 to be closed after DrainAndClose")
	}
}

func TestHedgedPipeline_BoundsAndDefaults(t *testing.T) {
	// Zero or invalid values should fall back to safe defaults
	delay, interval, maxCandidates := normalizeHedgeConfig(config.HedgeConfig{
		DelayMs:       0,
		IntervalMs:    -5,
		MaxCandidates: 0,
	})

	if delay != 650*time.Millisecond {
		t.Fatalf("expected default delay 650ms, got %v", delay)
	}
	if interval != 120*time.Millisecond {
		t.Fatalf("expected default interval 120ms, got %v", interval)
	}
	if maxCandidates != 1 {
		t.Fatalf("expected maxCandidates 1 when <=0, got %d", maxCandidates)
	}

	// If MaxCandidates == 1, execute as a single attempt
	callCount := 0
	attemptFn := func(ctx context.Context, attemptNum int, session string) (*http.Response, error) {
		callCount++
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte("ok"))),
		}, nil
	}

	resp, _, err, attempts := executeHedgedPipeline(context.Background(), config.HedgeConfig{MaxCandidates: 1}, "ses_single", attemptFn)
	if err != nil || resp == nil {
		t.Fatalf("expected successful single attempt, err: %v", err)
	}
	_ = resp.Body.Close()

	if callCount != 1 || attempts != 1 {
		t.Fatalf("expected exactly 1 attempt executed, got calls=%d, attempts=%d", callCount, attempts)
	}
}

func TestHedgedPipeline_ParentContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := config.HedgeConfig{
		Enabled:       true,
		DelayMs:       100,
		IntervalMs:    50,
		MaxCandidates: 3,
	}

	attemptFn := func(bCtx context.Context, attemptNum int, session string) (*http.Response, error) {
		<-bCtx.Done()
		return nil, bCtx.Err()
	}

	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	resp, _, err, _ := executeHedgedPipeline(ctx, cfg, "ses_primary", attemptFn)
	if resp != nil {
		resp.Body.Close()
		t.Fatalf("expected nil response on parent context cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestHedgedPipeline_RaceDetector(t *testing.T) {
	const routines = 10
	var wg sync.WaitGroup
	wg.Add(routines)

	for i := 0; i < routines; i++ {
		go func(routineID int) {
			defer wg.Done()

			cfg := config.HedgeConfig{
				Enabled:       true,
				DelayMs:       15,
				IntervalMs:    15,
				MaxCandidates: 4,
			}

			attemptFn := func(bCtx context.Context, attemptNum int, session string) (*http.Response, error) {
				if attemptNum%2 == 0 {
					// Fast fail
					return &http.Response{
						StatusCode: http.StatusTooManyRequests,
						Body:       io.NopCloser(bytes.NewReader([]byte("429"))),
					}, nil
				}
				// Success
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte("200"))),
				}, nil
			}

			resp, _, err, attempts := executeHedgedPipeline(context.Background(), cfg, "ses_race", attemptFn)
			if err == nil && resp != nil {
				_ = resp.Body.Close()
			}
			if attempts < 1 {
				t.Errorf("expected at least 1 attempt, got %d", attempts)
			}
		}(i)
	}

	wg.Wait()
}

func TestHedgedPipeline_GatewayAnonymousPreconditions(t *testing.T) {
	gw := &Gateway{
		cfg: config.Config{
			Hedge: config.HedgeConfig{
				Enabled:       true,
				DelayMs:       100,
				IntervalMs:    50,
				MaxCandidates: 2,
			},
		},
	}

	// 1. Nil anonymous pool
	_, _, err, _ := gw.executeHedgedAnonymousPipeline(
		context.Background(),
		models.Route{ID: "opencode-free"},
		map[config.Tier][]byte{config.TierZen: []byte(`{"prompt":"hello"}`)},
		identity.RequestIDs{Session: "ses_test"},
		0,
	)
	if !errors.Is(err, errAnonymousLanePaused) {
		t.Fatalf("expected errAnonymousLanePaused when anonymous pool is nil, got %v", err)
	}

	// 2. Empty Zen body
	gw.anonymous = anonymousPool(true, nil, 0, anonymousBackoff{})
	_, _, err, _ = gw.executeHedgedAnonymousPipeline(
		context.Background(),
		models.Route{ID: "opencode-free"},
		map[config.Tier][]byte{},
		identity.RequestIDs{Session: "ses_test"},
		0,
	)
	if err == nil || err.Error() != "no prepared Zen request body" {
		t.Fatalf("expected 'no prepared Zen request body' error, got %v", err)
	}
}

