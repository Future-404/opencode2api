package gateway

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	wire "opencode2api/internal/protocol"
)

func TestToolShape_RenameAndComplete_Chat(t *testing.T) {
	body := []byte(`{
		"model": "flint-40b-free",
		"messages": [
			{"role": "user", "content": "run task"}
		],
		"tools": [
			{
				"type": "function",
				"function": {
					"name": "Bash",
					"description": "Execute bash commands",
					"parameters": {"type": "object", "properties": {"cmd": {"type": "string"}}}
				}
			},
			{
				"type": "function",
				"function": {
					"name": "Read",
					"description": "Read file contents",
					"parameters": {"type": "object", "properties": {"path": {"type": "string"}}}
				}
			}
		]
	}`)

	shaped, renames := prepareAnonymousBody(body, wire.Chat, false)
	if renames == nil || renames.Count() != 2 {
		t.Fatalf("expected 2 client tools renamed, got %v", renames)
	}

	// Verify wire name for declared tools
	wireBash, ok := renames.Wire("Bash")
	if !ok || !strings.HasPrefix(wireBash, wire.ClientToolPrefix) {
		t.Fatalf("expected Bash wire name with prefix %s, got ok=%v, %s", wire.ClientToolPrefix, ok, wireBash)
	}
	wireRead, ok := renames.Wire("Read")
	if !ok || !strings.HasPrefix(wireRead, wire.ClientToolPrefix) {
		t.Fatalf("expected Read wire name with prefix %s, got ok=%v, %s", wire.ClientToolPrefix, ok, wireRead)
	}

	// Verify official tool alias bindings (v2 lane has official tools "shell" and "read")
	boundShell, ok := renames.Official("shell")
	if !ok || boundShell != "Bash" {
		t.Fatalf("expected official 'shell' bound to declared 'Bash', got ok=%v, %s", ok, boundShell)
	}
	boundRead, ok := renames.Official("read")
	if !ok || boundRead != "Read" {
		t.Fatalf("expected official 'read' bound to declared 'Read', got ok=%v, %s", ok, boundRead)
	}

	// Verify shaped payload structure
	var parsed map[string]any
	if err := json.Unmarshal(shaped, &parsed); err != nil {
		t.Fatalf("unmarshal shaped body failed: %v", err)
	}

	tools, ok := parsed["tools"].([]any)
	if !ok {
		t.Fatalf("expected tools array in shaped payload")
	}

	// Declared tools (2) + v2OfficialToolOrder (12) = 14 total tools
	expectedTotal := 2 + len(v2OfficialToolOrder)
	if len(tools) != expectedTotal {
		t.Fatalf("expected %d tools, got %d", expectedTotal, len(tools))
	}

	// Verify first tool has the obfuscated wire name
	firstTool := tools[0].(map[string]any)
	firstFunc := firstTool["function"].(map[string]any)
	if firstFunc["name"] != wireBash {
		t.Fatalf("expected first tool wire name %s, got %s", wireBash, firstFunc["name"])
	}
}

func TestToolShape_RenameAndComplete_Anthropic(t *testing.T) {
	body := []byte(`{
		"model": "flint-40b-free",
		"messages": [
			{"role": "user", "content": "run task"}
		],
		"tools": [
			{
				"name": "Edit",
				"description": "Edit file",
				"input_schema": {"type": "object", "properties": {"file": {"type": "string"}}}
			}
		]
	}`)

	shaped, renames := prepareAnonymousBody(body, wire.Anthropic, false)
	if renames == nil || renames.Count() != 1 {
		t.Fatalf("expected 1 client tool renamed, got %v", renames)
	}

	wireEdit, ok := renames.Wire("Edit")
	if !ok || !strings.HasPrefix(wireEdit, wire.ClientToolPrefix) {
		t.Fatalf("expected Edit wire name with prefix %s, got %s", wire.ClientToolPrefix, wireEdit)
	}

	// Official tool "edit" bound to "Edit"
	boundEdit, ok := renames.Official("edit")
	if !ok || boundEdit != "Edit" {
		t.Fatalf("expected official 'edit' bound to 'Edit', got ok=%v, %s", ok, boundEdit)
	}

	var parsed map[string]any
	if err := json.Unmarshal(shaped, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	tools, ok := parsed["tools"].([]any)
	if !ok || len(tools) != 1+len(v2OfficialToolOrder) {
		t.Fatalf("unexpected tools length: %v", len(tools))
	}
}

func TestToolShape_TitleRequestStaysToolFree(t *testing.T) {
	titleBody := []byte(`{
		"model": "flint-40b-free",
		"messages": [
			{"role": "system", "content": "You name conversations so the user can find them later in a list of past sessions. Reply with a title and nothing else."},
			{"role": "user", "content": "Refactor router"}
		]
	}`)

	shaped, renames := prepareAnonymousBody(titleBody, wire.Chat, false)
	if renames != nil && !renames.Empty() {
		t.Fatalf("expected nil or empty renames for title request, got %v", renames)
	}

	var parsed map[string]any
	if err := json.Unmarshal(shaped, &parsed); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if _, ok := parsed["tools"]; ok {
		t.Fatalf("title request must stay tool-free, but tools was present")
	}
}

func TestToolShape_HistorySanitizationAndChoice(t *testing.T) {
	body := []byte(`{
		"model": "flint-40b-free",
		"messages": [
			{
				"role": "assistant",
				"tool_calls": [
					{
						"id": "call_1",
						"type": "function",
						"function": {"name": "Bash", "arguments": "ls"}
					}
				]
			},
			{
				"role": "tool",
				"tool_call_id": "call_1",
				"content": "file1.go\nfile2.go"
			}
		],
		"tools": [
			{
				"type": "function",
				"function": {"name": "Bash", "description": "run command"}
			}
		],
		"tool_choice": {
			"type": "function",
			"function": {"name": "Bash"}
		}
	}`)

	shaped, renames := prepareAnonymousBody(body, wire.Chat, false)
	if renames == nil || renames.Count() != 1 {
		t.Fatalf("expected 1 rename, got %v", renames)
	}
	wireBash, _ := renames.Wire("Bash")

	var parsed map[string]any
	if err := json.Unmarshal(shaped, &parsed); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	// Verify history tool_calls name rewritten to wire name
	messages := parsed["messages"].([]any)
	var asstMsg map[string]any
	for _, m := range messages {
		msgMap := m.(map[string]any)
		if msgMap["role"] == "assistant" {
			asstMsg = msgMap
			break
		}
	}
	if asstMsg == nil {
		t.Fatalf("expected assistant message in parsed messages: %v", messages)
	}
	toolCalls, ok := asstMsg["tool_calls"].([]any)
	if !ok || len(toolCalls) == 0 {
		t.Fatalf("expected tool_calls in assistant message: %v", asstMsg)
	}
	call0 := toolCalls[0].(map[string]any)
	func0 := call0["function"].(map[string]any)
	if func0["name"] != wireBash {
		t.Fatalf("expected history tool call name %s, got %s", wireBash, func0["name"])
	}

	// Verify tool_choice name rewritten to wire name
	toolChoice := parsed["tool_choice"].(map[string]any)
	choiceFunc := toolChoice["function"].(map[string]any)
	if choiceFunc["name"] != wireBash {
		t.Fatalf("expected tool_choice name %s, got %s", wireBash, choiceFunc["name"])
	}
}

func TestToolShape_ConcurrentPrepare(t *testing.T) {
	agentBody := []byte(`{
		"model": "flint-40b-free",
		"messages": [{"role": "user", "content": "analyze code"}],
		"tools": [
			{"type": "function", "function": {"name": "Bash", "description": "shell"}},
			{"type": "function", "function": {"name": "Read", "description": "read"}}
		]
	}`)

	var wg sync.WaitGroup
	workers := 50
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				shaped, renames := prepareAnonymousBody(agentBody, wire.Chat, false)
				if renames == nil || renames.Count() != 2 {
					t.Errorf("expected 2 renames")
				}
				if len(shaped) == 0 {
					t.Errorf("expected non-empty shaped body")
				}
				wireBash, _ := renames.Wire("Bash")
				if restored := renames.Restore(wireBash); restored != "Bash" {
					t.Errorf("restore failed: got %s, want Bash", restored)
				}
			}
		}()
	}

	wg.Wait()
}
