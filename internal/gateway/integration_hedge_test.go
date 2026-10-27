package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/telemetry"
)

func TestGateway_ConversationSessionPromotion(t *testing.T) {
	var mu sync.Mutex
	receivedSessions := make([]string, 0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session := r.Header.Get("x-opencode-session")
		mu.Lock()
		receivedSessions = append(receivedSessions, session)
		mu.Unlock()

		// Primary session gets rate limited (429)
		if session == "ses_conv_initial" {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error": "rate limited"}`))
			return
		}

		// Any other (hedged rotated) session succeeds immediately (200)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`data: {"type":"chunk","delta":{"text":"hello"}}\n\n`))
	}))
	defer server.Close()

	cfg := config.Config{
		Anonymous: true,
		Upstream: config.UpstreamConfig{
			Anonymous: server.URL,
		},
		Hedge: config.HedgeConfig{
			Enabled:       true,
			DelayMs:       40,
			IntervalMs:    20,
			MaxCandidates: 3,
		},
		Retry: config.RetryConfig{
			TimeoutSeconds: 5,
			MaxAttempts:    3,
		},
	}

	gw, err := New(cfg, slog.Default(), nil, nil)
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	route := models.Route{
		ID:        "opencode-free",
		Anonymous: true,
		Protocol:  wire.Responses,
	}
	bodies := map[config.Tier][]byte{
		config.TierZen: []byte(`{"model":"opencode-free","stream":true,"messages":[{"role":"user","content":"turn 1"}]}`),
	}

	convKey := "ses_conv_initial"

	// -------------------------------------------------------------
	// TURN 1: Primary hits 429, hedged candidate wins with Session B
	// -------------------------------------------------------------
	metaTurn1 := &telemetry.RequestMeta{Channel: "anonymous"}
	ctxTurn1 := telemetry.WithRequestMeta(context.Background(), metaTurn1)
	ctxTurn1 = context.WithValue(ctxTurn1, requestShapeContextKey{}, &requestShape{})

	idsTurn1 := identity.RequestIDs{Session: convKey}

	resp1, err, attempts1 := gw.doAnonymousUpstream(ctxTurn1, route, bodies, idsTurn1, 0)
	if err != nil {
		t.Fatalf("Turn 1 expected success, got error: %v", err)
	}
	if resp1 == nil || resp1.StatusCode != http.StatusOK {
		t.Fatalf("Turn 1 expected status 200, got %v", resp1)
	}
	_ = resp1.Body.Close()

	if attempts1 < 2 {
		t.Fatalf("Turn 1 expected at least 2 attempts (primary + hedged), got %d", attempts1)
	}

	mu.Lock()
	if len(receivedSessions) < 2 {
		t.Fatalf("expected at least 2 requests to mock server, got %d", len(receivedSessions))
	}
	firstReqSession := receivedSessions[0]
	winningSession := receivedSessions[1]
	mu.Unlock()

	if firstReqSession != "ses_conv_initial" {
		t.Fatalf("expected first request to be ses_conv_initial, got %s", firstReqSession)
	}
	if winningSession == "ses_conv_initial" {
		t.Fatalf("expected winning session to be different from ses_conv_initial, got %s", winningSession)
	}

	// Verify session router promoted winningSession for convKey
	if gw.sessionRouter == nil {
		t.Fatalf("gw.sessionRouter is nil")
	}
	cachedSession := gw.sessionRouter.GetOrSet(convKey, "fallback")
	if cachedSession != winningSession {
		t.Fatalf("expected sessionRouter to hold winning session %s, got %s", winningSession, cachedSession)
	}

	// Verify telemetry legs format ("anon:429:XXms anon:hedged#1:200:YYms")
	if len(metaTurn1.Legs) < 2 {
		t.Fatalf("expected at least 2 legs in telemetry for Turn 1, got %d: %v", len(metaTurn1.Legs), metaTurn1.Legs)
	}
	if !strings.HasPrefix(metaTurn1.Legs[0], "anon:429:") {
		t.Fatalf("expected first leg to start with 'anon:429:', got %s", metaTurn1.Legs[0])
	}
	if !strings.HasPrefix(metaTurn1.Legs[1], "anon:hedged#1:200:") {
		t.Fatalf("expected second leg to start with 'anon:hedged#1:200:', got %s", metaTurn1.Legs[1])
	}

	// -------------------------------------------------------------
	// TURN 2: Next turn in same conversation should dial directly
	// using winningSession without any 429
	// -------------------------------------------------------------
	mu.Lock()
	receivedSessions = nil // reset tracking
	mu.Unlock()

	metaTurn2 := &telemetry.RequestMeta{Channel: "anonymous"}
	ctxTurn2 := telemetry.WithRequestMeta(context.Background(), metaTurn2)
	ctxTurn2 = context.WithValue(ctxTurn2, requestShapeContextKey{}, &requestShape{})

	idsTurn2 := identity.RequestIDs{Session: convKey}

	resp2, err, attempts2 := gw.doAnonymousUpstream(ctxTurn2, route, bodies, idsTurn2, 0)
	if err != nil {
		t.Fatalf("Turn 2 expected success, got error: %v", err)
	}
	if resp2 == nil || resp2.StatusCode != http.StatusOK {
		t.Fatalf("Turn 2 expected status 200, got %v", resp2)
	}
	_ = resp2.Body.Close()

	if attempts2 != 1 {
		t.Fatalf("Turn 2 expected exactly 1 attempt on sticky winning session, got %d", attempts2)
	}

	mu.Lock()
	if len(receivedSessions) != 1 {
		t.Fatalf("Turn 2 expected exactly 1 request to mock server, got %d", len(receivedSessions))
	}
	turn2DialedSession := receivedSessions[0]
	mu.Unlock()

	if turn2DialedSession != winningSession {
		t.Fatalf("Turn 2 expected to dial winning session %s directly, but dialed %s", winningSession, turn2DialedSession)
	}

	// Verify Turn 2 telemetry has 1 successful leg
	if len(metaTurn2.Legs) != 1 {
		t.Fatalf("Turn 2 expected 1 leg in telemetry, got %d: %v", len(metaTurn2.Legs), metaTurn2.Legs)
	}
	if !strings.HasPrefix(metaTurn2.Legs[0], "anon:200:") {
		t.Fatalf("Turn 2 expected leg to start with 'anon:200:', got %s", metaTurn2.Legs[0])
	}
}

func TestGateway_ConversationSequentialFallback(t *testing.T) {
	var mu sync.Mutex
	receivedSessions := make([]string, 0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session := r.Header.Get("x-opencode-session")
		mu.Lock()
		receivedSessions = append(receivedSessions, session)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`data: {"type":"chunk","delta":{"text":"hello"}}\n\n`))
	}))
	defer server.Close()

	// Hedge.Enabled = false => sequential behavior
	cfg := config.Config{
		Anonymous: true,
		Upstream: config.UpstreamConfig{
			Anonymous: server.URL,
		},
		Hedge: config.HedgeConfig{
			Enabled: false,
		},
		Retry: config.RetryConfig{
			TimeoutSeconds: 5,
			MaxAttempts:    3,
		},
	}

	gw, err := New(cfg, slog.Default(), nil, nil)
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	route := models.Route{
		ID:        "opencode-free",
		Anonymous: true,
		Protocol:  wire.Responses,
	}
	bodies := map[config.Tier][]byte{
		config.TierZen: []byte(`{"model":"opencode-free","stream":true,"messages":[{"role":"user","content":"sequential turn 1"}]}`),
	}

	convKey := "ses_conv_seq"
	ids := identity.RequestIDs{Session: convKey}

	resp, err, attempts := gw.doAnonymousUpstream(context.Background(), route, bodies, ids, 0)
	if err != nil {
		t.Fatalf("sequential mode expected success, got: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("sequential mode expected status 200, got: %v", resp)
	}
	_ = resp.Body.Close()

	if attempts != 1 {
		t.Fatalf("expected 1 attempt in sequential mode, got %d", attempts)
	}

	mu.Lock()
	if len(receivedSessions) != 1 || receivedSessions[0] != convKey {
		t.Fatalf("expected dialed session %s, got %v", convKey, receivedSessions)
	}
	mu.Unlock()

	// Verify session router promoted convKey
	if gw.sessionRouter == nil {
		t.Fatalf("gw.sessionRouter is nil")
	}
	cachedSession := gw.sessionRouter.GetOrSet(convKey, "fallback")
	if cachedSession != convKey {
		t.Fatalf("expected sessionRouter to hold %s, got %s", convKey, cachedSession)
	}
}

func TestGateway_ConversationInvalidationOnPermanentFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// All requests 429
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error": "all rate limited"}`))
	}))
	defer server.Close()

	cfg := config.Config{
		Anonymous: true,
		Upstream: config.UpstreamConfig{
			Anonymous: server.URL,
		},
		Hedge: config.HedgeConfig{
			Enabled:       true,
			DelayMs:       20,
			IntervalMs:    10,
			MaxCandidates: 2,
		},
		Retry: config.RetryConfig{
			TimeoutSeconds: 5,
			MaxAttempts:    2,
		},
	}

	gw, err := New(cfg, slog.Default(), nil, nil)
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	convKey := "conv_perm_fail"
	badSession := "ses_bad_initial"

	// Pre-populate router with bad session
	if gw.sessionRouter != nil {
		gw.sessionRouter.Promote(convKey, badSession)
	}

	route := models.Route{
		ID:        "opencode-free",
		Anonymous: true,
		Protocol:  wire.Responses,
	}
	bodies := map[config.Tier][]byte{
		config.TierZen: []byte(`{"model":"opencode-free","stream":true,"messages":[{"role":"user","content":"fail"}]}`),
	}
	ids := identity.RequestIDs{Session: convKey}

	resp, _, _ := gw.doAnonymousUpstream(context.Background(), route, bodies, ids, 0)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	// Verify badSession was invalidated
	if gw.sessionRouter != nil {
		fresh := gw.sessionRouter.GetOrSet(convKey, "fresh_default")
		if fresh != "fresh_default" {
			t.Fatalf("expected bad session to be invalidated, but got %s", fresh)
		}
	}
}

func TestGateway_DoUpstreamTiers_HedgedPromotion(t *testing.T) {
	var mu sync.Mutex
	receivedSessions := make([]string, 0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session := r.Header.Get("x-opencode-session")
		mu.Lock()
		receivedSessions = append(receivedSessions, session)
		mu.Unlock()

		if session == "ses_tiers_init" {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error": "rate limited"}`))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`data: {"type":"chunk","delta":{"text":"tiers-ok"}}\n\n`))
	}))
	defer server.Close()

	cfg := config.Config{
		Anonymous: true,
		Upstream: config.UpstreamConfig{
			Anonymous: server.URL,
		},
		Hedge: config.HedgeConfig{
			Enabled:       true,
			DelayMs:       40,
			IntervalMs:    20,
			MaxCandidates: 3,
		},
		Retry: config.RetryConfig{
			TimeoutSeconds: 5,
			MaxAttempts:    3,
		},
	}

	gw, err := New(cfg, slog.Default(), nil, nil)
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	route := models.Route{
		ID:        "opencode-free",
		Anonymous: true,
		Protocol:  wire.Responses,
	}
	bodies := map[config.Tier][]byte{
		config.TierZen: []byte(`{"model":"opencode-free","stream":true,"messages":[{"role":"user","content":"tiers turn 1"}]}`),
	}

	convKey := "ses_tiers_init"

	// Turn 1 via doUpstreamTiers
	metaTurn1 := &telemetry.RequestMeta{Channel: "anonymous"}
	ctxTurn1 := telemetry.WithRequestMeta(context.Background(), metaTurn1)
	ctxTurn1 = context.WithValue(ctxTurn1, requestShapeContextKey{}, &requestShape{})

	idsTurn1 := identity.RequestIDs{Session: convKey}

	resp1, effRoute1, attempts1, err := gw.doUpstreamTiers(ctxTurn1, route, bodies, idsTurn1, 0)
	if err != nil {
		t.Fatalf("Turn 1 doUpstreamTiers failed: %v", err)
	}
	if resp1 == nil || resp1.StatusCode != http.StatusOK {
		t.Fatalf("Turn 1 doUpstreamTiers expected 200, got %v", resp1)
	}
	_ = resp1.Body.Close()
	_ = effRoute1

	if attempts1 < 2 {
		t.Fatalf("expected at least 2 attempts for Turn 1, got %d", attempts1)
	}

	mu.Lock()
	winningSession := receivedSessions[len(receivedSessions)-1]
	receivedSessions = nil
	mu.Unlock()

	// Turn 2 via doUpstreamTiers for same conversation
	metaTurn2 := &telemetry.RequestMeta{Channel: "anonymous"}
	ctxTurn2 := telemetry.WithRequestMeta(context.Background(), metaTurn2)
	ctxTurn2 = context.WithValue(ctxTurn2, requestShapeContextKey{}, &requestShape{})

	idsTurn2 := identity.RequestIDs{Session: convKey}

	resp2, _, attempts2, err := gw.doUpstreamTiers(ctxTurn2, route, bodies, idsTurn2, 0)
	if err != nil {
		t.Fatalf("Turn 2 doUpstreamTiers failed: %v", err)
	}
	if resp2 == nil || resp2.StatusCode != http.StatusOK {
		t.Fatalf("Turn 2 doUpstreamTiers expected 200, got %v", resp2)
	}
	_ = resp2.Body.Close()

	if attempts2 != 1 {
		t.Fatalf("Turn 2 doUpstreamTiers expected 1 attempt, got %d", attempts2)
	}

	mu.Lock()
	if len(receivedSessions) != 1 || receivedSessions[0] != winningSession {
		t.Fatalf("Turn 2 expected to dial winning session %s, got %v", winningSession, receivedSessions)
	}
	mu.Unlock()
}
