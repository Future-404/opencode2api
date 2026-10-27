package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"opencode2api/internal/config"
	"opencode2api/internal/gateway"
	"opencode2api/internal/telemetry"
)

func setupTestAdminServer(t *testing.T) (*Server, *gateway.RuntimeController, string, *http.Cookie, string) {
	t.Helper()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	cfg := config.DefaultConfig()
	cfg.ServerKeys = []string{"test-server-key"}
	cfg.Anonymous = true
	cfg.WebUI.Enabled = true
	cfg.WebUI.Username = "admin"
	cfg.WebUI.Password = "adminpassword123"
	cfg.Upstream.Anonymous = "https://opencode.ai/inference"
	cfg.Hedge = config.HedgeConfig{
		Enabled:           true,
		DelayMs:           650,
		IntervalMs:        120,
		MaxCandidates:     5,
		SessionTTLMinutes: 30,
	}
	if err := config.SaveAtomic(configPath, cfg); err != nil {
		t.Fatalf("failed to save test config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	level := new(slog.LevelVar)
	hub := telemetry.LogHub(100)
	redactor := config.SecretRedactor()
	redactor.Replace(cfg)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	monitor := telemetry.Monitor()

	manager, err := gateway.RuntimeManager(ctx, configPath, cfg, logger, monitor, hub, redactor, level)
	if err != nil {
		t.Fatalf("failed to start RuntimeManager: %v", err)
	}
	t.Cleanup(manager.Shutdown)

	srv := New(manager, monitor, hub, logger)

	// Perform login to acquire session cookie and CSRF token
	loginBody, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "adminpassword123",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(loginBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("login failed: code %d, body %s", rec.Code, rec.Body.String())
	}

	var sessionCookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == adminCookieName {
			sessionCookie = c
			break
		}
	}
	if sessionCookie == nil {
		t.Fatalf("login response did not set session cookie")
	}

	var loginResp struct {
		CSRFToken string `json:"csrf_token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &loginResp); err != nil {
		t.Fatalf("failed to parse login response: %v", err)
	}
	if loginResp.CSRFToken == "" {
		t.Fatalf("login response did not contain CSRF token")
	}

	return srv, manager, configPath, sessionCookie, loginResp.CSRFToken
}

func testConfigUpdate(cfg config.Config) ConfigUpdate {
	var serverKeys []SecretInput
	for _, k := range cfg.ServerKeys {
		serverKeys = append(serverKeys, SecretInput{Value: k})
	}
	var zenKeys []SecretInput
	for _, k := range cfg.ZenKeys {
		zenKeys = append(zenKeys, SecretInput{Value: k})
	}
	var goKeys []SecretInput
	for _, k := range cfg.GoKeys {
		goKeys = append(goKeys, SecretInput{Value: k})
	}
	var proxies []SecretInput
	for _, p := range cfg.Proxies {
		proxies = append(proxies, SecretInput{Value: p})
	}

	return ConfigUpdate{
		Listen:      cfg.Listen,
		ServerKeys:  serverKeys,
		ZenKeys:     zenKeys,
		GoKeys:      goKeys,
		Anonymous:   cfg.Anonymous,
		Warp:        cfg.Warp,
		Proxies:     proxies,
		ProxyFile:   cfg.ProxyFile,
		Upstream:    cfg.Upstream,
		Retry:       cfg.Retry,
		Models:      cfg.Models,
		Performance: cfg.Performance,
		Logging:     cfg.Logging,
		Prefer:      cfg.Prefer,
		WebUI: WebUIView{
			Enabled:           cfg.WebUI.Enabled,
			Listen:            cfg.WebUI.Listen,
			Username:          cfg.WebUI.Username,
			SessionTTLMinutes: cfg.WebUI.SessionTTLMinutes,
		},
	}
}

func TestConfigView_IncludesHedge(t *testing.T) {
	srv, _, _, cookie, _ := setupTestAdminServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/config failed: code %d, body %s", rec.Code, rec.Body.String())
	}

	var view ConfigView
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("failed to decode ConfigView: %v", err)
	}

	if !view.Hedge.Enabled {
		t.Fatalf("expected view.Hedge.Enabled to be true")
	}
	if view.Hedge.DelayMs != 650 {
		t.Fatalf("expected view.Hedge.DelayMs 650, got %d", view.Hedge.DelayMs)
	}
	if view.Hedge.IntervalMs != 120 {
		t.Fatalf("expected view.Hedge.IntervalMs 120, got %d", view.Hedge.IntervalMs)
	}
	if view.Hedge.MaxCandidates != 5 {
		t.Fatalf("expected view.Hedge.MaxCandidates 5, got %d", view.Hedge.MaxCandidates)
	}
	if view.Hedge.SessionTTLMinutes != 30 {
		t.Fatalf("expected view.Hedge.SessionTTLMinutes 30, got %d", view.Hedge.SessionTTLMinutes)
	}
}

func TestHandlePutConfig_PreservesHedgeAndAnonymousUpstreamWhenOmitted(t *testing.T) {
	srv, manager, configPath, cookie, csrf := setupTestAdminServer(t)

	currentCfg := manager.Config()
	update := testConfigUpdate(currentCfg)
	// Explicitly leave Hedge nil and Upstream.Anonymous empty to simulate partial/omitted client update
	update.Hedge = nil
	update.Upstream.Anonymous = ""
	update.Retry.MaxAttempts = 5 // Change an arbitrary field to confirm update applies

	body, err := json.Marshal(update)
	if err != nil {
		t.Fatalf("marshal update: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewReader(body))
	req.AddCookie(cookie)
	req.Header.Set("X-CSRF-Token", csrf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("PUT /api/config failed: code %d, body %s", rec.Code, rec.Body.String())
	}

	updatedCfg := manager.Config()
	if updatedCfg.Retry.MaxAttempts != 5 {
		t.Fatalf("expected Retry.MaxAttempts 5, got %d", updatedCfg.Retry.MaxAttempts)
	}

	// Verify Hedge was preserved in runtime memory
	if !updatedCfg.Hedge.Enabled {
		t.Fatalf("expected Hedge.Enabled to remain true, got false")
	}
	if updatedCfg.Hedge.DelayMs != 650 {
		t.Fatalf("expected Hedge.DelayMs to remain 650, got %d", updatedCfg.Hedge.DelayMs)
	}
	if updatedCfg.Hedge.IntervalMs != 120 {
		t.Fatalf("expected Hedge.IntervalMs to remain 120, got %d", updatedCfg.Hedge.IntervalMs)
	}
	if updatedCfg.Hedge.MaxCandidates != 5 {
		t.Fatalf("expected Hedge.MaxCandidates to remain 5, got %d", updatedCfg.Hedge.MaxCandidates)
	}
	if updatedCfg.Hedge.SessionTTLMinutes != 30 {
		t.Fatalf("expected Hedge.SessionTTLMinutes to remain 30, got %d", updatedCfg.Hedge.SessionTTLMinutes)
	}

	// Verify Upstream.Anonymous was preserved in runtime memory
	if updatedCfg.Upstream.Anonymous != "https://opencode.ai/inference" {
		t.Fatalf("expected Upstream.Anonymous to remain https://opencode.ai/inference, got %q", updatedCfg.Upstream.Anonymous)
	}

	// Verify persistence to disk did not wipe Hedge or Upstream.Anonymous
	loadedCfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("failed to reload config from disk: %v", err)
	}
	if !loadedCfg.Hedge.Enabled || loadedCfg.Hedge.DelayMs != 650 || loadedCfg.Hedge.IntervalMs != 120 || loadedCfg.Hedge.MaxCandidates != 5 || loadedCfg.Hedge.SessionTTLMinutes != 30 {
		t.Fatalf("persisted config has wiped or altered Hedge: %+v", loadedCfg.Hedge)
	}
	if loadedCfg.Upstream.Anonymous != "https://opencode.ai/inference" {
		t.Fatalf("persisted config has altered Upstream.Anonymous: %q", loadedCfg.Upstream.Anonymous)
	}
}

func TestHandlePutConfig_UpdatesHedgeWhenProvided(t *testing.T) {
	srv, manager, configPath, cookie, csrf := setupTestAdminServer(t)

	currentCfg := manager.Config()
	update := testConfigUpdate(currentCfg)
	update.Hedge = &config.HedgeConfig{
		Enabled:           false,
		DelayMs:           350,
		IntervalMs:        90,
		MaxCandidates:     4,
		SessionTTLMinutes: 20,
	}

	body, err := json.Marshal(update)
	if err != nil {
		t.Fatalf("marshal update: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewReader(body))
	req.AddCookie(cookie)
	req.Header.Set("X-CSRF-Token", csrf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("PUT /api/config failed: code %d, body %s", rec.Code, rec.Body.String())
	}

	// Check runtime config
	updatedCfg := manager.Config()
	if updatedCfg.Hedge.Enabled {
		t.Fatalf("expected Hedge.Enabled to be updated to false")
	}
	if updatedCfg.Hedge.DelayMs != 350 {
		t.Fatalf("expected Hedge.DelayMs updated to 350, got %d", updatedCfg.Hedge.DelayMs)
	}
	if updatedCfg.Hedge.IntervalMs != 90 {
		t.Fatalf("expected Hedge.IntervalMs updated to 90, got %d", updatedCfg.Hedge.IntervalMs)
	}
	if updatedCfg.Hedge.MaxCandidates != 4 {
		t.Fatalf("expected Hedge.MaxCandidates updated to 4, got %d", updatedCfg.Hedge.MaxCandidates)
	}
	if updatedCfg.Hedge.SessionTTLMinutes != 20 {
		t.Fatalf("expected Hedge.SessionTTLMinutes updated to 20, got %d", updatedCfg.Hedge.SessionTTLMinutes)
	}

	// Check GET /api/config reflects new Hedge
	getReq := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	getReq.AddCookie(cookie)
	getRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET /api/config failed: code %d", getRec.Code)
	}
	var view ConfigView
	if err := json.Unmarshal(getRec.Body.Bytes(), &view); err != nil {
		t.Fatalf("failed to decode ConfigView: %v", err)
	}
	if view.Hedge.Enabled || view.Hedge.DelayMs != 350 || view.Hedge.IntervalMs != 90 || view.Hedge.MaxCandidates != 4 || view.Hedge.SessionTTLMinutes != 20 {
		t.Fatalf("GET /api/config did not return updated Hedge: %+v", view.Hedge)
	}

	// Check persisted config on disk
	loadedCfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("failed to reload config from disk: %v", err)
	}
	if loadedCfg.Hedge.Enabled || loadedCfg.Hedge.DelayMs != 350 || loadedCfg.Hedge.IntervalMs != 90 || loadedCfg.Hedge.MaxCandidates != 4 || loadedCfg.Hedge.SessionTTLMinutes != 20 {
		t.Fatalf("persisted config did not save updated Hedge: %+v", loadedCfg.Hedge)
	}
}
