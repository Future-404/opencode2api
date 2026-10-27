package gateway

import (
	"sync"
	"time"
)

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

func (sr *SessionRouter) GetOrSet(convKey, defaultSession string) string {
	if convKey == "" {
		return defaultSession
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()

	now := time.Now()
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
	if convKey == "" || winningSession == "" {
		return
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.entries[convKey] = sessionEntry{
		session:   winningSession,
		updatedAt: time.Now(),
	}
}

func (sr *SessionRouter) Invalidate(convKey, badSession string) {
	if convKey == "" {
		return
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if entry, exists := sr.entries[convKey]; exists && entry.session == badSession {
		delete(sr.entries, convKey)
	}
}
