package protocol

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestClientToolWireName(t *testing.T) {
	used := make(map[string]bool)

	// 1. Basic formatting: clint_ + 12 hex chars
	wireName1 := ClientToolWireName("custom_bash", used)
	if !strings.HasPrefix(wireName1, ClientToolPrefix) {
		t.Fatalf("expected wire name to start with %q, got %q", ClientToolPrefix, wireName1)
	}
	expectedLen := len(ClientToolPrefix) + clientToolDigestLength
	if len(wireName1) != expectedLen {
		t.Fatalf("expected length %d, got %d (%q)", expectedLen, len(wireName1), wireName1)
	}
	if !used[wireName1] {
		t.Fatalf("expected wireName1 to be registered in used map")
	}

	// 2. Collision resolution: forcing a collision using same base
	wireName2 := ClientToolWireName("custom_bash", used)
	if wireName2 != wireName1+"_2" {
		t.Fatalf("expected collision suffix '_2', got %q", wireName2)
	}
	wireName3 := ClientToolWireName("custom_bash", used)
	if wireName3 != wireName1+"_3" {
		t.Fatalf("expected collision suffix '_3', got %q", wireName3)
	}

	// 3. Length ceiling limit: never exceed toolWireNameLimit = 64
	longName := strings.Repeat("very_long_tool_name_", 20)
	longWire := ClientToolWireName(longName, used)
	if len(longWire) > toolWireNameLimit {
		t.Fatalf("wire name exceeded limit %d: got length %d (%q)", toolWireNameLimit, len(longWire), longWire)
	}
}

func TestToolRenames_BasicMappingAndOfficial(t *testing.T) {
	renames := NewToolRenames()
	if !renames.Empty() {
		t.Fatalf("expected empty renames")
	}
	if renames.Count() != 0 {
		t.Fatalf("expected 0 count")
	}

	renames.Add("run_shell", "clint_abc123456789")
	if renames.Empty() || renames.Count() != 1 {
		t.Fatalf("expected 1 tool registered")
	}

	// Check Wire & Client lookups
	wire, ok := renames.Wire("run_shell")
	if !ok || wire != "clint_abc123456789" {
		t.Fatalf("Wire('run_shell') = (%q, %v), want ('clint_abc123456789', true)", wire, ok)
	}
	client, ok := renames.Client("clint_abc123456789")
	if !ok || client != "run_shell" {
		t.Fatalf("Client('clint_abc123456789') = (%q, %v), want ('run_shell', true)", client, ok)
	}

	// BindOfficial
	renames.BindOfficial("shell", "run_shell")
	officialClient, ok := renames.Official("shell")
	if !ok || officialClient != "run_shell" {
		t.Fatalf("Official('shell') = (%q, %v), want ('run_shell', true)", officialClient, ok)
	}
}

func TestToolRenames_ResolveAndRestore(t *testing.T) {
	renames := NewToolRenames()
	renames.Add("TodoWrite", "clint_111122223333")
	renames.Add("RunBash", "clint_444455556666")
	renames.BindOfficial("bash", "RunBash")

	// 1. Exact wire name
	if res := renames.Restore("clint_111122223333"); res != "TodoWrite" {
		t.Fatalf("Restore exact wire failed: got %q, want 'TodoWrite'", res)
	}

	// 2. Official alias
	if res := renames.Restore("bash"); res != "RunBash" {
		t.Fatalf("Restore official alias failed: got %q, want 'RunBash'", res)
	}

	// 3. Fallback prefix stripping
	if res := renames.Restore("clint_TodoWrite"); res != "TodoWrite" {
		t.Fatalf("Restore prefix strip failed: got %q, want 'TodoWrite'", res)
	}

	// 4. Case folding
	if res := renames.Restore("todowrite"); res != "TodoWrite" {
		t.Fatalf("Restore case folding failed: got %q, want 'TodoWrite'", res)
	}

	// 5. Separator & shape folding: todo_write, todo-write, todo.write
	for _, shape := range []string{"todo_write", "todo-write", "todo.write", "TODO_WRITE"} {
		if res := renames.Restore(shape); res != "TodoWrite" {
			t.Fatalf("Restore shape folding %q failed: got %q, want 'TodoWrite'", shape, res)
		}
	}

	// 6. Unknown tool name passes through unchanged
	if res := renames.Restore("completely_unknown_tool"); res != "completely_unknown_tool" {
		t.Fatalf("Restore unknown tool should pass through: got %q", res)
	}
}

func TestToolRenames_AmbiguousShapeRefusal(t *testing.T) {
	renames := NewToolRenames()
	// Two declared tools that fold to the same shape "todowrite" in byShape
	renames.Add("todo_write", "clint_111111111111")
	renames.Add("TodoWrite", "clint_222222222222")

	// "todo-write" is not in byCase, but folds to "todowrite" in byShape.
	// Because both "todo_write" and "TodoWrite" share that shape, byShape["todowrite"] is marked ambiguous ("").
	// Neither should be guessed for ambiguous input "todo-write".
	res, changed := renames.Resolve("todo-write")
	if res != "todo-write" || changed {
		t.Fatalf("expected ambiguous resolution to refuse guessing, got %q (changed=%v)", res, changed)
	}

	// Also test byCase ambiguous collision when two tools differ only by case:
	renamesCase := NewToolRenames()
	renamesCase.Add("myTool", "clint_333333333333")
	renamesCase.Add("MYTOOL", "clint_444444444444")
	resCase, changedCase := renamesCase.Resolve("mytool")
	if resCase != "mytool" || changedCase {
		t.Fatalf("expected ambiguous byCase resolution to refuse guessing, got %q (changed=%v)", resCase, changedCase)
	}
}

func TestToolRenames_PrefixQueries(t *testing.T) {
	renames := NewToolRenames()
	renames.Add("toolA", "clint_abc123")

	if !renames.HasPrefix("clint_") {
		t.Fatalf("expected HasPrefix('clint_') to be true")
	}
	if !renames.HasLongerPrefix("clint_") {
		t.Fatalf("expected HasLongerPrefix('clint_') to be true")
	}
	if renames.HasLongerPrefix("clint_abc123") {
		t.Fatalf("expected HasLongerPrefix('clint_abc123') to be false (exact length)")
	}
	if renames.HasPrefix("unknown_") {
		t.Fatalf("expected HasPrefix('unknown_') to be false")
	}
}

func TestToolRenames_ConcurrentAccess(t *testing.T) {
	renames := NewToolRenames()
	renames.Add("BashTool", "clint_111111111111")
	renames.Add("ReadTool", "clint_222222222222")
	renames.BindOfficial("bash", "BashTool")
	renames.BindOfficial("read", "ReadTool")

	var wg sync.WaitGroup
	workers := 50
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				// Concurrent reads
				_, _ = renames.Wire("BashTool")
				_, _ = renames.Client("clint_111111111111")
				_, _ = renames.Official("bash")
				_ = renames.Restore("clint_111111111111")
				_ = renames.Restore("bash")
				_ = renames.Restore("bashtool")
				_ = renames.Restore("read_tool")
				_ = renames.HasPrefix("clint_")

				// Concurrent independent generator
				localUsed := make(map[string]bool)
				name := fmt.Sprintf("tool_%d_%d", workerID, j)
				wire := ClientToolWireName(name, localUsed)
				if !strings.HasPrefix(wire, ClientToolPrefix) {
					t.Errorf("worker %d: bad wire %s", workerID, wire)
				}
			}
		}(i)
	}

	wg.Wait()
}
