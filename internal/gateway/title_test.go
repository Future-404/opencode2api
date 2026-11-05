package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func TestIsTitleGenerationRequest_Comprehensive(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		expected bool
	}{
		{
			name: "Legacy v1 OpenCode title prompt in user turn",
			body: `{
				"model": "flint-40b-free",
				"messages": [
					{"role": "user", "content": "Generate a title for this conversation:\nUser: Fix login bug"}
				]
			}`,
			expected: true,
		},
		{
			name: "New v2 OpenCode title prompt in system turn",
			body: `{
				"model": "flint-40b-free",
				"messages": [
					{"role": "system", "content": "You name conversations so the user can find them later in a list of past sessions. Reply with a title and nothing else: one line, no quotes, no trailing period."},
					{"role": "user", "content": "Implement auth middleware"}
				]
			}`,
			expected: true,
		},
		{
			name: "New v2 OpenCode title prompt variant with 'for it'",
			body: `{
				"model": "flint-40b-free",
				"messages": [
					{"role": "system", "content": "You name conversations so the user can find them later in a list of past sessions. Reply with a title for it and nothing else: one line, no quotes, no trailing period."},
					{"role": "user", "content": "Fix typo"}
				]
			}`,
			expected: true,
		},
		{
			name: "New v2 auto-rename prompt",
			body: `{
				"model": "flint-40b-free",
				"messages": [
					{"role": "system", "content": "You decide whether the session title still matches the conversation. Reply with KEEP or new title."},
					{"role": "user", "content": "Database migration script"}
				]
			}`,
			expected: true,
		},
		{
			name: "New v2 generate a descriptive title prompt",
			body: `{
				"model": "flint-40b-free",
				"messages": [
					{"role": "system", "content": "You generate a descriptive title for this conversation"},
					{"role": "user", "content": "Setup Docker swarm"}
				]
			}`,
			expected: true,
		},
		{
			name: "Normal user agent request with declared tools (not title)",
			body: `{
				"model": "flint-40b-free",
				"messages": [
					{"role": "system", "content": "You are a coding assistant."},
					{"role": "user", "content": "List files in current directory"}
				],
				"tools": [
					{"type": "function", "function": {"name": "shell", "description": "run command"}}
				]
			}`,
			expected: false,
		},
		{
			name: "Tool-free regular Q&A request (must NOT be treated as title)",
			body: `{
				"model": "flint-40b-free",
				"messages": [
					{"role": "user", "content": "What is the capital of France?"}
				]
			}`,
			expected: false,
		},
		{
			name: "User message quotes title prompt in normal conversation (false positive prevention)",
			body: `{
				"model": "flint-40b-free",
				"messages": [
					{"role": "system", "content": "You are a helpful coding assistant."},
					{"role": "user", "content": "Can you review this prompt for me: 'You name conversations so the user can find them later in a list of past sessions.'"}
				]
			}`,
			expected: false,
		},
		{
			name: "Multi-turn conversation with assistant turn (must NOT be treated as title)",
			body: `{
				"model": "flint-40b-free",
				"messages": [
					{"role": "system", "content": "You name conversations so the user can find them later in a list of past sessions."},
					{"role": "user", "content": "Hi"},
					{"role": "assistant", "content": "Hello!"},
					{"role": "user", "content": "Now generate code"}
				]
			}`,
			expected: false,
		},
		{
			name: "Conversation with tool turn (must NOT be treated as title)",
			body: `{
				"model": "flint-40b-free",
				"messages": [
					{"role": "system", "content": "You name conversations so the user can find them later in a list of past sessions."},
					{"role": "user", "content": "Run tool"},
					{"role": "tool", "content": "output"}
				]
			}`,
			expected: false,
		},
		{
			name: "Exceeds titleMaxMessages limit",
			body: `{
				"model": "flint-40b-free",
				"messages": [
					{"role": "system", "content": "You name conversations so the user can find them later in a list of past sessions."},
					{"role": "user", "content": "m1"},
					{"role": "user", "content": "m2"},
					{"role": "user", "content": "m3"}
				]
			}`,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTitleGenerationBody([]byte(tt.body))
			if got != tt.expected {
				t.Errorf("isTitleGenerationBody() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestSynthesizeTitle_UnicodeAndFallbacks(t *testing.T) {
	tests := []struct {
		name     string
		payload  map[string]any
		expected string
	}{
		{
			name: "Standard english prompt",
			payload: map[string]any{
				"messages": []any{
					map[string]any{"role": "user", "content": "Fix authentication bug in middleware"},
				},
			},
			expected: "Fix authentication bug in middleware",
		},
		{
			name: "Unicode characters and truncation",
			payload: map[string]any{
				"messages": []any{
					map[string]any{"role": "user", "content": "Türkçe başlık oluşturma ve kota sınırlaması çözümü"},
				},
			},
			expected: "Türkçe başlık oluşturma ve kota sınırlam",
		},
		{
			name: "Legacy prefix stripped cleanly",
			payload: map[string]any{
				"messages": []any{
					map[string]any{"role": "user", "content": "Generate a title for this conversation:\nUser: Upgrade database schema to v3"},
				},
			},
			expected: "User: Upgrade database schema to v3",
		},
		{
			name: "Empty content safe fallback",
			payload: map[string]any{
				"messages": []any{
					map[string]any{"role": "user", "content": "   "},
				},
			},
			expected: defaultTitle,
		},
		{
			name: "No user message safe fallback",
			payload: map[string]any{
				"messages": []any{
					map[string]any{"role": "system", "content": "System prompt only"},
				},
			},
			expected: defaultTitle,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := synthesizeTitle(tt.payload)
			if got != tt.expected {
				t.Errorf("synthesizeTitle() = %q, expected %q", got, tt.expected)
			}
		})
	}
}

func TestSynthesizeTitleResponse(t *testing.T) {
	// 1. Non-streaming JSON completion
	respJSON := synthesizeTitleResponse("test-model", "My Generated Title", false)
	if respJSON.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", respJSON.StatusCode)
	}
	if respJSON.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("expected application/json, got %s", respJSON.Header.Get("Content-Type"))
	}
	bodyBytes, err := io.ReadAll(respJSON.Body)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	var data map[string]any
	if err := json.Unmarshal(bodyBytes, &data); err != nil {
		t.Fatalf("json unmarshal failed: %v", err)
	}
	if data["model"] != "test-model" {
		t.Fatalf("expected model 'test-model', got %v", data["model"])
	}
	choices := data["choices"].([]any)
	choice0 := choices[0].(map[string]any)
	msg := choice0["message"].(map[string]any)
	if msg["content"] != "My Generated Title" {
		t.Fatalf("expected title 'My Generated Title', got %v", msg["content"])
	}

	// 2. Streaming SSE completion
	respSSE := synthesizeTitleResponse("test-model", "Streamed Title", true)
	if respSSE.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", respSSE.StatusCode)
	}
	if respSSE.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %s", respSSE.Header.Get("Content-Type"))
	}
	sseBytes, err := io.ReadAll(respSSE.Body)
	if err != nil {
		t.Fatalf("ReadAll SSE error: %v", err)
	}
	sseStr := string(sseBytes)
	if !strings.Contains(sseStr, "Streamed Title") {
		t.Fatalf("expected sse to contain title: %s", sseStr)
	}
	if !strings.Contains(sseStr, "data: [DONE]\n\n") {
		t.Fatalf("expected sse to end with data: [DONE]\n\n: %s", sseStr)
	}
}

func TestTitleFallback(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rawBody := []byte(`{
		"model": "test-model",
		"messages": [{"role": "user", "content": "Fallback Title Test"}],
		"stream": false
	}`)

	// 1. Upstream 429 response -> should produce fallback response
	resp429 := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Body:       io.NopCloser(strings.NewReader(`{"error": "rate limited"}`)),
	}
	fb429 := titleFallback(logger, "test-model", rawBody, resp429, nil)
	if fb429 == nil {
		t.Fatalf("expected titleFallback to return response for 429")
	}
	defer fb429.Body.Close()
	if fb429.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 from fallback, got %d", fb429.StatusCode)
	}

	// 2. Upstream 500 response -> should return nil (no fallback)
	resp500 := &http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       io.NopCloser(strings.NewReader(`{"error": "internal error"}`)),
	}
	fb500 := titleFallback(logger, "test-model", rawBody, resp500, nil)
	if fb500 != nil {
		t.Fatalf("expected titleFallback to return nil for 500")
	}

	// 3. Network error (nil response, non-nil error) -> should produce fallback response
	fbErr := titleFallback(logger, "test-model", rawBody, nil, errors.New("dial timeout"))
	if fbErr == nil {
		t.Fatalf("expected titleFallback to return response on connection error")
	}
	defer fbErr.Body.Close()
	if fbErr.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 from connection error fallback, got %d", fbErr.StatusCode)
	}
}

func TestTitle_ConcurrentDetectionAndSynthesis(t *testing.T) {
	rawBody := []byte(`{
		"model": "test-model",
		"messages": [
			{"role": "system", "content": "You name conversations so the user can find them later in a list of past sessions. Reply with a title and nothing else."},
			{"role": "user", "content": "Concurrent Test Operation"}
		],
		"stream": false
	}`)

	var wg sync.WaitGroup
	workers := 50
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if !isTitleGenerationBody(rawBody) {
					t.Errorf("expected isTitleGenerationBody true")
				}
				var payload map[string]any
				_ = json.Unmarshal(rawBody, &payload)
				title := synthesizeTitle(payload)
				if title != "Concurrent Test Operation" {
					t.Errorf("expected title 'Concurrent Test Operation', got %q", title)
				}
				resp := synthesizeTitleResponse("test-model", title, false)
				_ = resp.Body.Close()
			}
		}()
	}

	wg.Wait()
}
