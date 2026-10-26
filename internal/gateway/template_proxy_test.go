package gateway

import (
	"testing"
	"time"

	"opencode2api/internal/config"
)

func TestTemplateProxyExpansionAndCaching(t *testing.T) {
	raw := "http://10m:{{session-id}}:K7mQ2vL9xR4pT8Za@127.0.0.1:2260#pool"
	clean, pool := config.ParseProxyEntry(raw)
	if !pool {
		t.Fatalf("expected pool to be true for %s", raw)
	}
	if clean != "http://10m:{{session-id}}:K7mQ2vL9xR4pT8Za@127.0.0.1:2260" {
		t.Fatalf("unexpected clean URL: %s", clean)
	}

	redacted := config.RedactURL(raw)
	if redacted != "http://%2A%2A%2A@127.0.0.1:2260#pool" {
		t.Fatalf("unexpected redacted URL: %s", redacted)
	}

	perf := config.PerformanceConfig{
		ConnectTimeoutSeconds:  10,
		IdleConnTimeoutSeconds: 60,
		MaxIdleConns:           100,
		MaxIdleConnsPerHost:    10,
		MaxConnsPerHost:        100,
	}

	poolTransports, err := transportPool([]string{raw}, perf, 30*time.Second)
	if err != nil {
		t.Fatalf("transportPool failed: %v", err)
	}
	if len(poolTransports.items) != 1 {
		t.Fatalf("expected 1 transport item, got %d", len(poolTransports.items))
	}

	proxy := poolTransports.items[0]
	if !proxy.isTemplate {
		t.Fatalf("expected proxy to be marked as template")
	}

	session1 := "ses_0192e2b3c4d5"
	session2 := "ses_998877665544"

	scope1 := sessionScope(session1)
	scope2 := sessionScope(session2)

	if len(scope1) != 16 || len(scope2) != 16 {
		t.Fatalf("expected 16-char scopes, got %s, %s", scope1, scope2)
	}
	if scope1 == scope2 {
		t.Fatalf("different sessions must have different scopes")
	}

	// First call builds client
	c1 := proxy.ClientForSession(session1, false)
	if c1 == nil {
		t.Fatalf("expected non-nil client for session1")
	}

	// Repeated call must return the exact same cached client
	c1Again := proxy.ClientForSession(session1, false)
	if c1 != c1Again {
		t.Fatalf("expected cached client to be reused for session1")
	}

	// Different session gets distinct client
	c2 := proxy.ClientForSession(session2, false)
	if c2 == nil {
		t.Fatalf("expected non-nil client for session2")
	}
	if c1 == c2 {
		t.Fatalf("different sessions should have distinct clients")
	}

	// Test SOCKS5 scheme
	socksRaw := "socks5://10m:{{session-id}}:K7mQ2vL9xR4pT8Za@127.0.0.1:2260"
	socksPool, err := transportPool([]string{socksRaw}, perf, 30*time.Second)
	if err != nil {
		t.Fatalf("socks5 transportPool failed: %v", err)
	}
	sProxy := socksPool.items[0]
	if !sProxy.isTemplate {
		t.Fatalf("expected socks5 proxy to be template")
	}
	sc1 := sProxy.ClientForSession(session1, false)
	if sc1 == nil {
		t.Fatalf("expected non-nil socks5 client")
	}
	sc1Again := sProxy.ClientForSession(session1, false)
	if sc1 != sc1Again {
		t.Fatalf("expected cached socks5 client to be reused")
	}
}
