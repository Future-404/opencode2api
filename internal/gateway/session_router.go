package gateway

import (
	"sync"
	"time"
)

const defaultSessionRouterMaxEntries = 1000

type sessionEntry struct {
	session   string
	updatedAt time.Time
}

// SessionRouter tracks the active healthy upstream session for each client
// conversation so consecutive turns reuse the proven proxy exit and keep
// prompt cache affinity without repeatedly hitting rate-limited exits.
type SessionRouter struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[string]sessionEntry
}

func NewSessionRouter(ttl time.Duration) *SessionRouter {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	return &SessionRouter{
		ttl:     ttl,
		entries: make(map[string]sessionEntry),
	}
}

func (sr *SessionRouter) cleanupExpiredLocked(now time.Time) int {
	var purged int
	for k, entry := range sr.entries {
		if now.Sub(entry.updatedAt) >= sr.ttl {
			delete(sr.entries, k)
			purged++
		}
	}
	return purged
}

// PurgeExpired evicts entries whose TTL has elapsed and returns the number of purged entries.
func (sr *SessionRouter) PurgeExpired() int {
	if sr == nil {
		return 0
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.cleanupExpiredLocked(time.Now())
}

// Len returns the current count of tracked sessions.
func (sr *SessionRouter) Len() int {
	if sr == nil {
		return 0
	}
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return len(sr.entries)
}

func (sr *SessionRouter) GetOrSet(convKey, defaultSession string) string {
	if sr == nil || convKey == "" {
		return defaultSession
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()

	now := time.Now()
	if len(sr.entries) >= defaultSessionRouterMaxEntries {
		sr.cleanupExpiredLocked(now)
	}

	if entry, exists := sr.entries[convKey]; exists {
		if now.Sub(entry.updatedAt) < sr.ttl {
			entry.updatedAt = now
			sr.entries[convKey] = entry
			return entry.session
		}
	}
	sr.entries[convKey] = sessionEntry{
		session:   defaultSession,
		updatedAt: now,
	}
	return defaultSession
}

func (sr *SessionRouter) Promote(convKey, winningSession string) {
	if sr == nil || convKey == "" || winningSession == "" {
		return
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()

	now := time.Now()
	if len(sr.entries) >= defaultSessionRouterMaxEntries {
		sr.cleanupExpiredLocked(now)
	}

	sr.entries[convKey] = sessionEntry{
		session:   winningSession,
		updatedAt: now,
	}
}

func (sr *SessionRouter) Invalidate(convKey, badSession string) {
	if sr == nil || convKey == "" {
		return
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if entry, exists := sr.entries[convKey]; exists && entry.session == badSession {
		delete(sr.entries, convKey)
	}
}

