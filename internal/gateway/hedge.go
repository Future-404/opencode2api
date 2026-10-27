package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/telemetry"
)

const (
	defaultHedgeDelayMs    = 650
	defaultHedgeIntervalMs = 120
	defaultHedgeCandidates = 5
)

// hedgedAttemptResult holds the outcome of an individual speculative branch.
type hedgedAttemptResult struct {
	resp       *http.Response
	session    string
	attemptNum int
	err        error
	legs       []string
}

// hedgedAttemptFunc is the attempt-dispatch abstraction executed by each hedged branch.
type hedgedAttemptFunc func(ctx context.Context, attemptNum int, session string) (*http.Response, error)

// normalizeHedgeConfig validates and applies default boundaries defensively.
func normalizeHedgeConfig(cfg config.HedgeConfig) (delay time.Duration, interval time.Duration, maxCandidates int) {
	if !cfg.Enabled {
		return 0, 0, 1
	}

	maxCandidates = cfg.MaxCandidates
	if maxCandidates <= 0 {
		maxCandidates = defaultHedgeCandidates
	}

	delayMs := cfg.DelayMs
	if delayMs <= 0 {
		delayMs = defaultHedgeDelayMs
	}
	delay = time.Duration(delayMs) * time.Millisecond

	intervalMs := cfg.IntervalMs
	if intervalMs <= 0 {
		intervalMs = defaultHedgeIntervalMs
	}
	interval = time.Duration(intervalMs) * time.Millisecond

	return delay, interval, maxCandidates
}

// isHedgeWinner reports whether an upstream attempt yielded a successful 2xx response.
func isHedgeWinner(resp *http.Response, err error) bool {
	return err == nil && resp != nil && (resp.StatusCode == http.StatusOK || resp.StatusCode/100 == 2)
}

// cancelOnCloseBody wraps a winning response body so closing it releases the winning branch context.
type cancelOnCloseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnCloseBody) Close() error {
	defer b.cancel()
	return b.ReadCloser.Close()
}

// executeHedgedPipeline coordinates dual-triggered hedged concurrency:
// 1. Fast-fail (0ms): If any attempt receives an immediate 429, 502, or transport failure,
//    the next candidate is dispatched immediately without waiting for the timer/interval.
// 2. Stalled timeout (DelayMs, default 650ms): If primary hasn't returned within DelayMs,
//    Candidate 1 is launched.
// 3. Micro-pipelining: Stagger subsequent candidates by IntervalMs (default 120ms) up to MaxCandidates.
// 4. First-Byte Winner Takes All: As soon as ANY branch yields HTTP 200 OK, that candidate wins.
//    Losing branch contexts are cancelled immediately, and losing bodies are drained and closed.
func executeHedgedPipeline(
	ctx context.Context,
	cfg config.HedgeConfig,
	initialSession string,
	attempt hedgedAttemptFunc,
) (*http.Response, string, error, int) {
	if ctx.Err() != nil {
		return nil, initialSession, ctx.Err(), 0
	}

	delay, interval, maxCandidates := normalizeHedgeConfig(cfg)

	if maxCandidates <= 1 {
		resp, err := attempt(ctx, 0, initialSession)
		return resp, initialSession, err, 1
	}

	resultCh := make(chan hedgedAttemptResult, maxCandidates)
	branchCancels := make([]context.CancelFunc, 0, maxCandidates)

	launch := func(idx int, session string) {
		branchCtx, branchCancel := context.WithCancel(ctx)
		// Isolate per-branch request shape and telemetry metadata to prevent concurrent data races
		branchCtx = context.WithValue(branchCtx, requestShapeContextKey{}, &requestShape{})
		branchMeta := &telemetry.RequestMeta{}
		if parentMeta := telemetry.MetaFromContext(ctx); parentMeta != nil {
			*branchMeta = *parentMeta
			branchMeta.Legs = nil
		}
		branchCtx = telemetry.WithRequestMeta(branchCtx, branchMeta)

		branchCancels = append(branchCancels, branchCancel)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					resultCh <- hedgedAttemptResult{
						session:    session,
						attemptNum: idx,
						err:        fmt.Errorf("hedged candidate panicked: %v", r),
					}
				}
			}()
			resp, err := attempt(branchCtx, idx, session)
			var legs []string
			if bMeta := telemetry.MetaFromContext(branchCtx); bMeta != nil && len(bMeta.Legs) > 0 {
				legs = append([]string(nil), bMeta.Legs...)
			}
			resultCh <- hedgedAttemptResult{
				resp:       resp,
				session:    session,
				attemptNum: idx,
				err:        err,
				legs:       legs,
			}
		}()
	}

	// Launch primary branch
	launch(0, initialSession)
	dispatched := 1
	active := 1
	resultsRead := 0

	hedgeTimer := time.NewTimer(delay)
	defer hedgeTimer.Stop()

	var lastFailure hedgedAttemptResult

	for {
		select {
		case <-ctx.Done():
			// Parent context canceled or timed out
			for _, cancel := range branchCancels {
				cancel()
			}
			if lastFailure.resp != nil && lastFailure.resp.Body != nil {
				httpx.DrainAndClose(lastFailure.resp.Body)
				lastFailure.resp = nil
			}
			remaining := dispatched - resultsRead
			if remaining > 0 {
				go func(count int) {
					for i := 0; i < count; i++ {
						res := <-resultCh
						if res.resp != nil && res.resp.Body != nil {
							httpx.DrainAndClose(res.resp.Body)
						}
					}
				}(remaining)
			}
			return nil, initialSession, ctx.Err(), dispatched

		case <-hedgeTimer.C:
			// Stalled or interval timer fired: launch next candidate if quota remains
			if dispatched < maxCandidates {
				nextSession := identity.RotateSessionID()
				launch(dispatched, nextSession)
				dispatched++
				active++
				if dispatched < maxCandidates {
					hedgeTimer.Reset(interval)
				}
			}

		case res := <-resultCh:
			resultsRead++
			active--

			// Safely merge attempt legs into parent RequestMeta in serial fashion
			if parentMeta := telemetry.MetaFromContext(ctx); parentMeta != nil && len(res.legs) > 0 {
				parentMeta.Legs = append(parentMeta.Legs, res.legs...)
			}

			// First-Byte Winner Takes All
			if isHedgeWinner(res.resp, res.err) {
				if !hedgeTimer.Stop() {
					select {
					case <-hedgeTimer.C:
					default:
					}
				}
				// Cancel contexts of all losing branches immediately
				for i, cancel := range branchCancels {
					if i != res.attemptNum {
						cancel()
					}
				}
				// Drain previous failure body if any
				if lastFailure.resp != nil && lastFailure.resp.Body != nil {
					httpx.DrainAndClose(lastFailure.resp.Body)
					lastFailure.resp = nil
				}
				// Wrap winner response body with branch cancel
				if res.resp != nil && res.resp.Body != nil {
					res.resp.Body = &cancelOnCloseBody{
						ReadCloser: res.resp.Body,
						cancel:     branchCancels[res.attemptNum],
					}
				}
				// Drain any remaining branches that arrive in background
				remaining := dispatched - resultsRead
				if remaining > 0 {
					go func(count int) {
						for i := 0; i < count; i++ {
							r := <-resultCh
							if r.resp != nil && r.resp.Body != nil {
								httpx.DrainAndClose(r.resp.Body)
							}
						}
					}(remaining)
				}
				return res.resp, res.session, nil, dispatched
			}

			// Branch failed: cancel this branch's context
			branchCancels[res.attemptNum]()

			// Keep this failure, draining previous failure response body if replacing
			if lastFailure.resp != nil && lastFailure.resp.Body != nil {
				httpx.DrainAndClose(lastFailure.resp.Body)
			}
			lastFailure = res

			// Fast-fail trigger: if any attempt fails, immediately launch next candidate
			if dispatched < maxCandidates {
				if !hedgeTimer.Stop() {
					select {
					case <-hedgeTimer.C:
					default:
					}
				}
				nextSession := identity.RotateSessionID()
				launch(dispatched, nextSession)
				dispatched++
				active++
				if dispatched < maxCandidates {
					hedgeTimer.Reset(interval)
				}
			} else if active == 0 {
				// All candidates dispatched and all have finished (failed)
				return lastFailure.resp, lastFailure.session, lastFailure.err, dispatched
			}
		}
	}
}

// executeHedgedAnonymousPipeline coordinates speculative hedged requests across anonymous proxy exits.
func (g *Gateway) executeHedgedAnonymousPipeline(
	ctx context.Context,
	route models.Route,
	bodies map[config.Tier][]byte,
	ids identity.RequestIDs,
	attemptOffset int,
) (*http.Response, string, error, int) {
	if g.anonymous == nil || !g.anonymous.AllowAttempt() {
		return nil, ids.Session, errAnonymousLanePaused, 0
	}
	defer g.anonymous.ReleaseProbe()

	rawBody := bodies[config.TierZen]
	if len(rawBody) == 0 {
		return nil, ids.Session, errors.New("no prepared Zen request body"), 0
	}

	protocol := route.ProtocolFor(config.TierZen)
	upstreams := g.anonymousUpstreams()
	preparedBodies := make([][]byte, len(upstreams))
	toolRenames := make([]*wire.ToolRenames, len(upstreams))
	for i, baseURL := range upstreams {
		preparedBodies[i], toolRenames[i] = prepareAnonymousBody(rawBody, protocol, models.IsV1Upstream(baseURL))
	}

	attemptFn := func(branchCtx context.Context, attemptNum int, session string) (*http.Response, error) {
		attemptIDs := ids
		attemptIDs.Session = session
		attemptIDs.ChildSession = session
		attemptIDs.ParentSession = ""
		return g.doSingleAnonymousAttempt(branchCtx, route, preparedBodies, toolRenames, upstreams, attemptIDs, attemptOffset, attemptNum, session)
	}

	return executeHedgedPipeline(ctx, g.cfg.Hedge, ids.Session, attemptFn)
}

// doSingleAnonymousAttempt executes one anonymous request attempt through the session's proxy cursor.
func (g *Gateway) doSingleAnonymousAttempt(
	ctx context.Context,
	route models.Route,
	preparedBodies [][]byte,
	toolRenames []*wire.ToolRenames,
	upstreams []string,
	ids identity.RequestIDs,
	attemptOffset int,
	attemptNum int,
	session string,
) (*http.Response, error) {
	if g.anonymous == nil {
		return nil, errors.New("no anonymous pool configured")
	}

	cursor := g.anonymous.CursorFor(session)
	node := cursor.Next()
	if node == nil {
		return nil, errors.New("no healthy anonymous proxies available")
	}

	protocol := route.ProtocolFor(config.TierZen)
	var lastResp *http.Response
	var lastErr error

	for index, baseURL := range upstreams {
		if ctx.Err() != nil {
			if lastResp != nil {
				httpx.DrainAndClose(lastResp.Body)
			}
			return nil, ctx.Err()
		}

		setRequestShape(ctx, false, toolRenames[index])
		if meta := telemetry.MetaFromContext(ctx); meta != nil {
			meta.Attempts = attemptOffset + attemptNum + 1
			meta.Tier = string(config.TierZen)
		}

		if lastResp != nil {
			httpx.DrainAndClose(lastResp.Body)
			lastResp = nil
		}

		req, err := upstreamRequest(ctx, baseURL, protocol, preparedBodies[index], ids, anonymousZenKey)
		if err != nil {
			return nil, err
		}
		if node.proxy.pool {
			req.Close = true
		}

		channel := "anonymous"
		if attemptNum > 0 {
			channel = fmt.Sprintf("hedged#%d", attemptNum)
		}
		setRequestCredential(ctx, config.TierZen, "anonymous", channel, true, node.proxy)
		started := time.Now()
		resp, err := doInferenceAttempt(g.inferenceClient(node.proxy, baseURL, route.ID, session), req, time.Duration(g.cfg.Performance.FirstEventTimeoutSeconds)*time.Second, g.attemptTimeout)
		duration := time.Since(started)

		if ctx.Err() != nil {
			if resp != nil && resp.Body != nil {
				httpx.DrainAndClose(resp.Body)
			}
			return nil, ctx.Err()
		}

		g.recordUpstreamAttempt(ctx, route, ids, attemptOffset+attemptNum+1, "anonymous", channel, true, node.proxy, resp, err, duration)

		if err == nil && resp.StatusCode/100 == 2 {
			g.observeAnonymousResult(ctx, node, resp, nil)
			g.logger.Debug("anonymous upstream accepted request", "component", "upstream", "event", "anonymous_attempt_succeeded", "request_id", ids.Request, "attempt", attemptOffset+attemptNum+1, "tier", config.TierZen, "upstream", config.RedactURL(baseURL), "key_id", "anonymous", "channel", channel, "anonymous", true, "proxy", config.RedactURL(node.proxy.name), "status", resp.StatusCode, "duration_ms", duration.Milliseconds())
			return resp, nil
		}

		lastResp = resp
		lastErr = asProxyUnavailable(node.proxy, err)

		if err != nil {
			g.logger.Debug("anonymous transport attempt failed", "component", "upstream", "event", "anonymous_attempt_transport_failed", "request_id", ids.Request, "attempt", attemptOffset+attemptNum+1, "tier", config.TierZen, "upstream", config.RedactURL(baseURL), "key_id", "anonymous", "channel", channel, "anonymous", true, "proxy", config.RedactURL(node.proxy.name), "duration_ms", duration.Milliseconds(), "error", err)
			if isProxyFailure(err) {
				break
			}
		} else if peekNetworkAbuseBlock(resp) {
			node.proxy.markBlocked(time.Now().Add(g.freeTierBlockTTL()))
			break
		} else if isDeterministicClientResponse(resp.StatusCode, peekResponseBody(resp)) {
			break
		} else if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
			// IP rate-limit is exit-specific. Skip remaining upstreams on this exit.
			break
		}

		if index+1 < len(upstreams) && !node.proxy.healthy.Load() {
			break
		}
	}

	g.observeAnonymousResult(ctx, node, lastResp, lastErr)
	return lastResp, lastErr
}
