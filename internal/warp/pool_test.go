package warp

import (
	"net/netip"
	"sync"
	"testing"
)

func TestExit_Penalized(t *testing.T) {
	e := &exit{
		egress: netip.MustParseAddr("2001:db8::1"),
	}
	if e.isPenalized() {
		t.Fatalf("expected new exit to not be penalized")
	}
	if !e.markPenalized() {
		t.Fatalf("expected markPenalized to return true first time")
	}
	if !e.isPenalized() {
		t.Fatalf("expected exit to be penalized")
	}
	if e.markPenalized() {
		t.Fatalf("expected markPenalized to return false when already penalized")
	}
	e.clearPenalty()
	if e.isPenalized() {
		t.Fatalf("expected clearPenalty to unmark penalized")
	}
}

func TestPool_HasUntriedActive(t *testing.T) {
	e1 := &exit{egress: netip.MustParseAddr("2606:4700:110:8100::1")}
	e2 := &exit{egress: netip.MustParseAddr("2606:4700:110:8200::1")}
	e3 := &exit{egress: netip.MustParseAddr("2606:4700:110:8300::1")}

	p := &Pool{
		active: []*exit{e1, e2, e3},
	}

	key1 := e1.egressKey()
	key2 := e2.egressKey()
	key3 := e3.egressKey()

	// 1. None tried -> untried exists
	if !p.HasUntriedActive(map[string]bool{}) {
		t.Fatalf("expected HasUntriedActive true when tried is empty")
	}

	// 2. Only key1 tried -> key2 and key3 still untried
	if !p.HasUntriedActive(map[string]bool{key1: true}) {
		t.Fatalf("expected HasUntriedActive true when only key1 tried")
	}

	// 3. key1 and key2 tried, key3 penalized -> no untried active left
	e3.markPenalized()
	if p.HasUntriedActive(map[string]bool{key1: true, key2: true}) {
		t.Fatalf("expected HasUntriedActive false when key3 is penalized and key1/key2 tried")
	}

	// 4. All tried -> false
	e3.clearPenalty()
	if p.HasUntriedActive(map[string]bool{key1: true, key2: true, key3: true}) {
		t.Fatalf("expected HasUntriedActive false when all are tried")
	}
}

func TestPool_HasUntriedActive_Concurrent(t *testing.T) {
	e1 := &exit{egress: netip.MustParseAddr("2606:4700:110:8100::1")}
	e2 := &exit{egress: netip.MustParseAddr("2606:4700:110:8200::1")}
	p := &Pool{
		active: []*exit{e1, e2},
	}

	var wg sync.WaitGroup
	workers := 50
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			tried := map[string]bool{
				e1.egressKey(): workerID%2 == 0,
			}
			for j := 0; j < iterations; j++ {
				_ = p.HasUntriedActive(tried)
				if j%10 == 0 {
					e2.markPenalized()
				} else if j%10 == 5 {
					e2.clearPenalty()
				}
			}
		}(i)
	}

	wg.Wait()
}
