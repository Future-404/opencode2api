package identity

import (
	"bytes"
	"net/http"
	"testing"
)

func TestDeriveRequestIDs_StableConversationKey(t *testing.T) {
	bodyTurn1 := map[string]any{
		"messages": []any{
			map[string]any{"role": "system", "content": "You are a helpful assistant."},
			map[string]any{"role": "user", "content": "Hello, write a story."},
		},
	}
	bodyTurn2 := map[string]any{
		"messages": []any{
			map[string]any{"role": "system", "content": "You are a helpful assistant."},
			map[string]any{"role": "user", "content": "Hello, write a story."},
			map[string]any{"role": "assistant", "content": "Once upon a time..."},
			map[string]any{"role": "user", "content": "Continue the story."},
		},
	}

	req1, _ := http.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(nil))
	req2, _ := http.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(nil))

	ids1 := DeriveRequestIDs(req1, bodyTurn1, "scope_test")
	ids2 := DeriveRequestIDs(req2, bodyTurn2, "scope_test")

	if ids1.Conversation == "" {
		t.Fatalf("expected non-empty conversation key for turn 1")
	}
	if ids1.Conversation != ids2.Conversation {
		t.Fatalf("expected identical conversation keys across turns, got %q vs %q", ids1.Conversation, ids2.Conversation)
	}
}
