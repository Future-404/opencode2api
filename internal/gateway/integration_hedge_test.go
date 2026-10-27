package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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
	metaTurn1 := &telemetry.RequestMeta{}
	ctxTurn1 := telemetry.WithRequestMeta(context.Background(), metaTurn1)
	ctxTurn1 = context.WithValue(ctxTurn1, requestShapeContextKey{}, &requestShape{})

	idsTurn1 := identity.RequestIDs{Session: convKey, Conversation: convKey}

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

	// Verify gateway populated parent telemetry from the winning candidate branch
	if metaTurn1.Channel != "hedged#1" {
		t.Fatalf("expected metaTurn1.Channel to be 'hedged#1', got %q", metaTurn1.Channel)
	}
	if metaTurn1.Session != winningSession {
		t.Fatalf("expected metaTurn1.Session to be %s, got %s", winningSession, metaTurn1.Session)
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

	metaTurn2 := &telemetry.RequestMeta{}
	ctxTurn2 := telemetry.WithRequestMeta(context.Background(), metaTurn2)
	ctxTurn2 = context.WithValue(ctxTurn2, requestShapeContextKey{}, &requestShape{})

	idsTurn2 := identity.RequestIDs{Session: "ses_initial_2", Conversation: convKey}

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

	// Verify gateway populated parent telemetry from attempt 0 (channel "anonymous")
	if metaTurn2.Channel != "anonymous" {
		t.Fatalf("expected metaTurn2.Channel to be 'anonymous', got %q", metaTurn2.Channel)
	}
	if metaTurn2.Session != winningSession {
		t.Fatalf("expected metaTurn2.Session to be %s, got %s", winningSession, metaTurn2.Session)
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
	metaTurn1 := &telemetry.RequestMeta{}
	ctxTurn1 := telemetry.WithRequestMeta(context.Background(), metaTurn1)
	ctxTurn1 = context.WithValue(ctxTurn1, requestShapeContextKey{}, &requestShape{})

	idsTurn1 := identity.RequestIDs{Session: convKey, Conversation: convKey}

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

	if metaTurn1.Channel != "hedged#1" {
		t.Fatalf("expected metaTurn1.Channel to be 'hedged#1', got %q", metaTurn1.Channel)
	}

	mu.Lock()
	winningSession := receivedSessions[len(receivedSessions)-1]
	receivedSessions = nil
	mu.Unlock()

	// Turn 2 via doUpstreamTiers for same conversation
	metaTurn2 := &telemetry.RequestMeta{}
	ctxTurn2 := telemetry.WithRequestMeta(context.Background(), metaTurn2)
	ctxTurn2 = context.WithValue(ctxTurn2, requestShapeContextKey{}, &requestShape{})

	idsTurn2 := identity.RequestIDs{Session: "ses_initial_2", Conversation: convKey}

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

	if metaTurn2.Channel != "anonymous" {
		t.Fatalf("expected metaTurn2.Channel to be 'anonymous', got %q", metaTurn2.Channel)
	}

	mu.Lock()
	if len(receivedSessions) != 1 || receivedSessions[0] != winningSession {
		t.Fatalf("Turn 2 expected to dial winning session %s, got %v", winningSession, receivedSessions)
	}
	mu.Unlock()
}

func TestGateway_HedgedToolRenamingPreservedOnWinner(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session := r.Header.Get("x-opencode-session")
		if session == "ses_tools_primary" {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error": "rate limited"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`data: {"type":"chunk","delta":{"text":"done"}}\n\n`))
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
	bodyWithTools := []byte(`{
		"model":"opencode-free",
		"stream":true,
		"messages":[{"role":"user","content":"call tool"}],
		"tools":[{"type":"function","name":"custom_tool","description":"desc"}]
	}`)
	bodies := map[config.Tier][]byte{
		config.TierZen: bodyWithTools,
	}

	meta := &telemetry.RequestMeta{}
	ctx := telemetry.WithRequestMeta(context.Background(), meta)
	ctx = context.WithValue(ctx, requestShapeContextKey{}, &requestShape{})

	ids := identity.RequestIDs{Session: "ses_tools_primary"}

	resp, err, attempts := gw.doAnonymousUpstream(ctx, route, bodies, ids, 0)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %v", resp)
	}
	_ = resp.Body.Close()

	if attempts < 2 {
		t.Fatalf("expected at least 2 attempts, got %d", attempts)
	}

	shape := requestShapeFrom(ctx)
	if shape.renames == nil || shape.renames.Count() == 0 {
		t.Fatalf("expected non-empty tool renames preserved on parent context, got %v", shape.renames)
	}
	wireName, ok := shape.renames.Wire("custom_tool")
	if !ok || wireName != "clint_custom_tool" {
		t.Fatalf("expected custom_tool to map to clint_custom_tool, got ok=%v, wireName=%s", ok, wireName)
	}
	if meta.ToolRenames == 0 {
		t.Fatalf("expected meta.ToolRenames > 0, got %d", meta.ToolRenames)
	}
	if meta.Channel != "hedged#1" {
		t.Fatalf("expected meta.Channel 'hedged#1', got %s", meta.Channel)
	}
}

func TestGateway_DeterministicErrorShortCircuit(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": {"code": "invalid_request_error", "message": "context_length_exceeded: maximum context length is 8192"}}`))
	}))
	defer server.Close()

	cfg := config.Config{
		Anonymous: true,
		Upstream: config.UpstreamConfig{
			Anonymous: server.URL,
		},
		Hedge: config.HedgeConfig{
			Enabled:       true,
			DelayMs:       100,
			IntervalMs:    50,
			MaxCandidates: 4,
		},
		Retry: config.RetryConfig{
			TimeoutSeconds: 5,
			MaxAttempts:    4,
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
		config.TierZen: []byte(`{"model":"opencode-free","stream":true,"messages":[{"role":"user","content":"too long"}]}`),
	}

	ids := identity.RequestIDs{Session: "ses_deterministic"}
	meta := &telemetry.RequestMeta{}
	ctx := telemetry.WithRequestMeta(context.Background(), meta)
	ctx = context.WithValue(ctx, requestShapeContextKey{}, &requestShape{})

	resp, err, attempts := gw.doAnonymousUpstream(ctx, route, bodies, ids, 0)
	if resp == nil {
		t.Fatalf("expected non-nil response for deterministic rejection")
	}
	defer resp.Body.Close()

	if !errors.Is(err, errDeterministicClientRejection) {
		t.Fatalf("expected errDeterministicClientRejection, got %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", resp.StatusCode)
	}
	if attempts != 1 {
		t.Fatalf("expected exactly 1 attempt dispatched (short-circuit), got %d", attempts)
	}
	if count := atomic.LoadInt32(&requestCount); count != 1 {
		t.Fatalf("expected exactly 1 upstream request dispatched, got %d", count)
	}
}

func TestGateway_StickyIPAbuseBlock(t *testing.T) {
	var callIndex int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := atomic.AddInt32(&callIndex, 1)
		if idx == 1 {
			// Candidate 0 gets IP abuse 403
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`Forbidden, OpenCode Free Model Gateway Detect Abuse Traffic`))
			return
		}
		// Candidate 1 gets 429
		if idx == 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error": "rate limited"}`))
			return
		}
		// Candidate 2 gets 502
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error": "bad gateway"}`))
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
		config.TierZen: []byte(`{"model":"opencode-free","stream":true,"messages":[{"role":"user","content":"test"}]}`),
	}

	ids := identity.RequestIDs{Session: "ses_abuse"}
	meta := &telemetry.RequestMeta{}
	ctx := telemetry.WithRequestMeta(context.Background(), meta)
	ctx = context.WithValue(ctx, requestShapeContextKey{}, &requestShape{})

	resp, err, attempts := gw.doAnonymousUpstream(ctx, route, bodies, ids, 0)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	if !errors.Is(err, errFreeTierIPBlocked) {
		t.Fatalf("expected sticky errFreeTierIPBlocked, got %v", err)
	}
	if attempts < 2 {
		t.Fatalf("expected at least 2 attempts, got %d", attempts)
	}
}
