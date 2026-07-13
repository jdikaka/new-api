package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/setting"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withRateLimitSettings saves/restores all rate-limit globals for the test
// and forces the memory path (Redis disabled).
func withRateLimitSettings(t *testing.T, durationMinutes, totalCount, successCount int, groups map[string][2]int) {
	t.Helper()

	prevEnabled := setting.ModelRequestRateLimitEnabled
	prevDuration := setting.ModelRequestRateLimitDurationMinutes
	prevTotal := setting.ModelRequestRateLimitCount
	prevSuccess := setting.ModelRequestRateLimitSuccessCount
	prevRedis := common.RedisEnabled

	setting.ModelRequestRateLimitMutex.Lock()
	prevGroups := setting.ModelRequestRateLimitGroup
	setting.ModelRequestRateLimitMutex.Unlock()

	setting.ModelRequestRateLimitEnabled = true
	setting.ModelRequestRateLimitDurationMinutes = durationMinutes
	setting.ModelRequestRateLimitCount = totalCount
	setting.ModelRequestRateLimitSuccessCount = successCount
	common.RedisEnabled = false

	setting.ModelRequestRateLimitMutex.Lock()
	setting.ModelRequestRateLimitGroup = groups
	setting.ModelRequestRateLimitMutex.Unlock()

	t.Cleanup(func() {
		setting.ModelRequestRateLimitEnabled = prevEnabled
		setting.ModelRequestRateLimitDurationMinutes = prevDuration
		setting.ModelRequestRateLimitCount = prevTotal
		setting.ModelRequestRateLimitSuccessCount = prevSuccess
		common.RedisEnabled = prevRedis
		setting.ModelRequestRateLimitMutex.Lock()
		setting.ModelRequestRateLimitGroup = prevGroups
		setting.ModelRequestRateLimitMutex.Unlock()
	})
}

// buildRateLimitRouter creates a gin engine with ModelRequestRateLimit and a
// configurable downstream handler. If tokenID is 0 the token_id key is not set.
func buildRateLimitRouter(t *testing.T, userID, tokenID int, group string, downstream gin.HandlerFunc) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("id", userID)
		if tokenID != 0 {
			c.Set("token_id", tokenID)
		}
		if group != "" {
			c.Set(string(constant.ContextKeyTokenGroup), group)
		}
		c.Next()
	})
	router.GET("/test", ModelRequestRateLimit(), downstream)
	return router
}

func fireRateLimitRequest(router *gin.Engine) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	router.ServeHTTP(w, req)
	return w
}

func okJSONHandler() gin.HandlerFunc {
	return func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) }
}

// Test 1: per-token counters are independent — one token hitting its limit
// does not block another token under the same user.
func TestModelRateLimitTokenIndependence(t *testing.T) {
	withRateLimitSettings(t, 1, 100, 0, map[string][2]int{"test": {2, 0}})

	router := buildRateLimitRouter(t, 1001, 101, "test", okJSONHandler())

	// Token 101: 2 requests pass (tokenTotal=2), 3rd blocked by token layer.
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)
	require.Equal(t, http.StatusTooManyRequests, fireRateLimitRequest(router).Code,
		"3rd request for token 101 should be blocked by per-token total limit")

	// Token 102: independent counter — request passes despite token 101 being blocked.
	router2 := buildRateLimitRouter(t, 1001, 102, "test", okJSONHandler())
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router2).Code,
		"token 102 has its own independent counter and should pass")
}

// Test 2: the user-aggregate total ceiling bounds the sum across all tokens.
func TestModelRateLimitUserAggregateCeiling(t *testing.T) {
	// userTotal=4 (ceiling), group gives tokenTotal=100 (so token layer never blocks).
	withRateLimitSettings(t, 1, 4, 0, map[string][2]int{"test": {100, 0}})

	r201 := buildRateLimitRouter(t, 1002, 201, "test", okJSONHandler())
	r202 := buildRateLimitRouter(t, 1002, 202, "test", okJSONHandler())

	// 2 requests per token = 4 total, all pass (userTotal=4).
	require.Equal(t, http.StatusOK, fireRateLimitRequest(r201).Code)
	require.Equal(t, http.StatusOK, fireRateLimitRequest(r201).Code)
	require.Equal(t, http.StatusOK, fireRateLimitRequest(r202).Code)
	require.Equal(t, http.StatusOK, fireRateLimitRequest(r202).Code)

	// 5th request blocked by user-aggregate total (4/4 consumed).
	// Token layer has 98 remaining slots (100-2), so only the user layer can block.
	require.Equal(t, http.StatusTooManyRequests, fireRateLimitRequest(r201).Code,
		"5th request should be blocked by user-aggregate total ceiling")
}

// Test 3: group config overrides per-token threshold; without group config
// the per-token threshold falls back to the global default.
func TestModelRateLimitGroupThresholdOverride(t *testing.T) {
	t.Run("group override sets per-token total", func(t *testing.T) {
		// userTotal=2 (global ceiling), group "vip" gives tokenTotal=5.
		withRateLimitSettings(t, 1, 2, 0, map[string][2]int{"vip": {5, 0}})

		router := buildRateLimitRouter(t, 1003, 301, "vip", okJSONHandler())

		// 2 requests pass (userTotal=2 at capacity). Token layer has 3 remaining
		// slots (5-2), so it is NOT the blocker — only the user layer can block.
		require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)
		require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)
		require.Equal(t, http.StatusTooManyRequests, fireRateLimitRequest(router).Code,
			"3rd request blocked by user-aggregate total (2/2), not by token layer (2/5)")
	})

	t.Run("no group falls back to global", func(t *testing.T) {
		// userTotal=100 (high), no group config → tokenTotal falls back to 100.
		withRateLimitSettings(t, 1, 100, 0, nil)

		router := buildRateLimitRouter(t, 1004, 302, "", okJSONHandler())

		// 3 requests pass for a single token — if tokenTotal had fallen back to
		// a stale group value (e.g. 5 from the sub-test above) the 3rd might
		// still pass, but if it fell back to 2 (a hypothetical misconfigured
		// default) the 3rd would be blocked. With tokenTotal=100 all pass.
		for i := 0; i < 3; i++ {
			require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code,
				"request %d should pass with tokenTotal=100 (global fallback)", i+1)
		}
	})
}

// Test 4: when the user-total limit is exceeded, c.Next() is NOT called —
// the downstream handler must not be invoked. This verifies the missing-return
// bug fix (the old Redis path fell through to c.Next() after abort).
func TestModelRateLimitMissingReturnBugFixed(t *testing.T) {
	withRateLimitSettings(t, 1, 1, 0, nil)

	downstreamCalled := false
	router := buildRateLimitRouter(t, 1005, 501, "", func(c *gin.Context) {
		downstreamCalled = true
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	// 1st request passes, downstream invoked.
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)
	require.True(t, downstreamCalled, "downstream should be called for 1st request")

	// 2nd request blocked by user total (1/1). Downstream must NOT be called.
	downstreamCalled = false
	rec := fireRateLimitRequest(router)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.False(t, downstreamCalled, "downstream must NOT be called when rate limit blocks")
}

// Test 5: a failed request does not consume the success counter. The success
// counter is reserved atomically up front and refunded when the response is a
// failure (HTTP >= 400), so the 500 below leaves the counter where it was.
// Both user and token success counters follow the same behavior.
func TestModelRateLimitFailedRequestDoesNotConsumeSuccess(t *testing.T) {
	withRateLimitSettings(t, 1, 100, 2, nil)

	callCount := 0
	router := buildRateLimitRouter(t, 1006, 601, "", func(c *gin.Context) {
		callCount++
		if callCount == 2 {
			c.JSON(http.StatusInternalServerError, gin.H{"err": "simulated"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	// Request 1 (200): success counters increment to 1.
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)

	// Request 2 (500): refunded — success counters stay at 1.
	require.Equal(t, http.StatusInternalServerError, fireRateLimitRequest(router).Code)

	// Request 3 (200): passes because the failed request was refunded, so the
	// success counter still had room (1/2 → 2/2).
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code,
		"3rd request must pass — the 500 was refunded and did not consume the success counter")

	// Request 4 (200): blocked — success counter now at 2 (from requests 1 and 3).
	require.Equal(t, http.StatusTooManyRequests, fireRateLimitRequest(router).Code,
		"4th request blocked by success counter (2/2)")

	// Verify token success counter also reached 2 (same behavior as user success).
	// Check() here is used only as a read-only probe of the limiter's state.
	duration := int64(setting.ModelRequestRateLimitDurationMinutes * 60)
	tokenSuccessKey := ModelRequestRateLimitTokenSuccessCountMark + "1006:601"
	assert.False(t, inMemoryRateLimiter.Check(tokenSuccessKey, 2, duration),
		"token success counter should be at 2 (2 successful requests recorded)")
}

// Test 6: when token_id is 0, the per-token layer is entirely skipped —
// only the user-aggregate layer applies.
func TestModelRateLimitTokenIdZeroSkipsPerTokenLayer(t *testing.T) {
	// userTotal=100 (high), group gives tokenTotal=5. If the per-token layer
	// were active for tokenId=0, the 6th request would be blocked (tokenTotal=5).
	// Since tokenId=0 skips the per-token layer, all 6 pass.
	withRateLimitSettings(t, 1, 100, 0, map[string][2]int{"test": {5, 0}})

	router := buildRateLimitRouter(t, 1007, 0, "test", okJSONHandler())

	for i := 0; i < 6; i++ {
		require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code,
			"request %d should pass — per-token layer skipped when tokenId=0", i+1)
	}

	// Verify no per-token keys were created for this user by probing with
	// Check(). A per-token key that was never written returns true (key absent).
	duration := int64(setting.ModelRequestRateLimitDurationMinutes * 60)
	tokenTotalKey := ModelRequestRateLimitTokenCountMark + "1007:0"
	tokenSuccessKey := ModelRequestRateLimitTokenSuccessCountMark + "1007:0"
	assert.True(t, inMemoryRateLimiter.Check(tokenTotalKey, 1, duration),
		"no per-token total key should exist for tokenId=0")
	assert.True(t, inMemoryRateLimiter.Check(tokenSuccessKey, 1, duration),
		"no per-token success key should exist for tokenId=0")
}

// TestSlidingWindowConsume exercises the atomic Lua sliding-window limiter
// against an in-process miniredis. Verifies the three branches of the script:
//   - under-capacity → allow (LPUSH + EXPIRE)
//   - saturated, oldest within window → deny (EXPIRE refresh)
//   - after window elapses → allow again (oldest evicted)
//
// Also checks the Go-side invariants: maxCount==0 bypasses Redis entirely,
// and a saturated key still has a TTL set so it cannot leak past window+1.
func TestSlidingWindowConsume(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	ctx := context.Background()
	const maxCount = 3
	const duration = int64(60)
	// Fixed caller-supplied timestamp. The allow-after-window case below is
	// exercised by the key expiring under FastForward, not by advancing now, so
	// a constant keeps the test deterministic and free of wall-clock drift.
	const now = int64(1700000000)
	key := "test:sliding"

	// maxCount==0 must bypass Redis entirely (no keys created, no scripts run).
	t.Run("maxCount zero bypasses Redis", func(t *testing.T) {
		bypassKey := "test:bypass"
		require.True(t, slidingWindowConsume(ctx, rdb, bypassKey, 0, duration, now))
		assert.False(t, mr.Exists(bypassKey), "no key should be written when maxCount==0")
	})

	// 3 allows, 4th denied, then after FastForward(duration+1) the saturated
	// key expires and the 5th call is allowed again.
	t.Run("allow 3 then deny then allow after window", func(t *testing.T) {
		for i := 0; i < maxCount; i++ {
			require.True(t, slidingWindowConsume(ctx, rdb, key, maxCount, duration, now),
				"request %d under cap should be allowed", i+1)
		}

		// Saturated: oldest (tail) is within window → deny. EXPIRE must still be set.
		require.False(t, slidingWindowConsume(ctx, rdb, key, maxCount, duration, now),
			"4th request over cap should be denied")

		ttl := mr.TTL(key)
		assert.True(t, ttl > 0 && ttl <= time.Duration(duration+1)*time.Second,
			"deny branch must set EXPIRE in (0, duration+1], got %v", ttl)

		// Advance miniredis clock past window. The saturated key expires, so the
		// next call sees LLEN=0 and re-enters the under-capacity allow branch.
		mr.FastForward(time.Duration(duration+1) * time.Second)
		require.True(t, slidingWindowConsume(ctx, rdb, key, maxCount, duration, now),
			"request after window elapsed should be allowed")
	})
}

// newMiniRedisLimiter starts an in-process miniredis server, wires common.RDB
// to it, sets common.RedisEnabled=true, and registers cleanup. Call AFTER
// withRateLimitSettings — the RedisEnabled=true override needs to win, and
// cleanup runs LIFO so the original value is still restored correctly.
// Returns the miniredis handle so callers can FastForward / inspect keys.
func newMiniRedisLimiter(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)

	prevRDB := common.RDB
	prevRedis := common.RedisEnabled

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	common.RDB = rdb
	common.RedisEnabled = true

	t.Cleanup(func() {
		_ = rdb.Close()
		mr.Close()
		common.RDB = prevRDB
		common.RedisEnabled = prevRedis
	})

	return mr
}

// Redis mirror of TestModelRateLimitTokenIndependence. Per-token counters
// stored in Redis must be independent across tokens under the same user.
func TestRedisRateLimitTokenIndependence(t *testing.T) {
	withRateLimitSettings(t, 1, 100, 0, map[string][2]int{"test": {2, 0}})
	mr := newMiniRedisLimiter(t)
	_ = mr

	router := buildRateLimitRouter(t, 1001, 101, "test", okJSONHandler())

	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)
	require.Equal(t, http.StatusTooManyRequests, fireRateLimitRequest(router).Code,
		"3rd request for token 101 should be blocked by per-token total limit")

	router2 := buildRateLimitRouter(t, 1001, 102, "test", okJSONHandler())
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router2).Code,
		"token 102 has its own independent counter and should pass")
}

// Redis mirror of TestModelRateLimitUserAggregateCeiling. The user-aggregate
// MRRL2 counter bounds the sum across all of the user's tokens.
func TestRedisRateLimitUserAggregateCeiling(t *testing.T) {
	withRateLimitSettings(t, 1, 4, 0, map[string][2]int{"test": {100, 0}})
	mr := newMiniRedisLimiter(t)
	_ = mr

	r201 := buildRateLimitRouter(t, 1002, 201, "test", okJSONHandler())
	r202 := buildRateLimitRouter(t, 1002, 202, "test", okJSONHandler())

	require.Equal(t, http.StatusOK, fireRateLimitRequest(r201).Code)
	require.Equal(t, http.StatusOK, fireRateLimitRequest(r201).Code)
	require.Equal(t, http.StatusOK, fireRateLimitRequest(r202).Code)
	require.Equal(t, http.StatusOK, fireRateLimitRequest(r202).Code)

	require.Equal(t, http.StatusTooManyRequests, fireRateLimitRequest(r201).Code,
		"5th request should be blocked by user-aggregate total ceiling")
}

// Redis mirror of TestModelRateLimitGroupThresholdOverride. Group config
// overrides per-token thresholds; without group config the global default
// is used.
func TestRedisRateLimitGroupThresholdOverride(t *testing.T) {
	t.Run("group override sets per-token total", func(t *testing.T) {
		withRateLimitSettings(t, 1, 2, 0, map[string][2]int{"vip": {5, 0}})
		mr := newMiniRedisLimiter(t)
		_ = mr

		router := buildRateLimitRouter(t, 1003, 301, "vip", okJSONHandler())

		require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)
		require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)
		require.Equal(t, http.StatusTooManyRequests, fireRateLimitRequest(router).Code,
			"3rd request blocked by user-aggregate total (2/2), not by token layer (2/5)")
	})

	t.Run("no group falls back to global", func(t *testing.T) {
		withRateLimitSettings(t, 1, 100, 0, nil)
		mr := newMiniRedisLimiter(t)
		_ = mr

		router := buildRateLimitRouter(t, 1004, 302, "", okJSONHandler())

		for i := 0; i < 3; i++ {
			require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code,
				"request %d should pass with tokenTotal=100 (global fallback)", i+1)
		}
	})
}

// Redis mirror of TestModelRateLimitMissingReturnBugFixed. When the limit is
// hit, c.Next() must NOT be invoked — the Redis handler must `return` after
// abort, not fall through.
func TestRedisRateLimitMissingReturnBugFixed(t *testing.T) {
	withRateLimitSettings(t, 1, 1, 0, nil)
	mr := newMiniRedisLimiter(t)
	_ = mr

	downstreamCalled := false
	router := buildRateLimitRouter(t, 1005, 501, "", func(c *gin.Context) {
		downstreamCalled = true
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)
	require.True(t, downstreamCalled, "downstream should be called for 1st request")

	downstreamCalled = false
	rec := fireRateLimitRequest(router)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.False(t, downstreamCalled, "downstream must NOT be called when rate limit blocks")
}

// Redis mirror of TestModelRateLimitFailedRequestDoesNotConsumeSuccess. A
// failed request (status >= 400) must be refunded: the success slot reserved
// up front is released, so only successful responses leave an entry in the
// success list.
func TestRedisRateLimitFailedRequestDoesNotConsumeSuccess(t *testing.T) {
	withRateLimitSettings(t, 1, 100, 2, nil)
	mr := newMiniRedisLimiter(t)
	_ = mr

	callCount := 0
	router := buildRateLimitRouter(t, 1006, 601, "", func(c *gin.Context) {
		callCount++
		if callCount == 2 {
			c.JSON(http.StatusInternalServerError, gin.H{"err": "simulated"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)
	require.Equal(t, http.StatusInternalServerError, fireRateLimitRequest(router).Code)
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code,
		"3rd request must pass — the 500 was refunded and did not consume the success counter")
	require.Equal(t, http.StatusTooManyRequests, fireRateLimitRequest(router).Code,
		"4th request blocked by success counter (2/2)")

	// Verify token success counter reached 2 in Redis (LLen of MRRLTS2 list).
	ctx := context.Background()
	tokenSuccessKey := "rateLimit:" + ModelRequestRateLimitTokenSuccessCount2Mark + ":1006:601"
	length, err := common.RDB.LLen(ctx, tokenSuccessKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), length,
		"token success list should hold 2 entries after 2 successful requests (the 500 was refunded)")
}

// Redis mirror of TestModelRateLimitTokenIdZeroSkipsPerTokenLayer. When
// token_id=0, the per-token layer is skipped entirely — no MRRLT2/MRRLTS
// keys should be written.
func TestRedisRateLimitTokenIdZeroSkipsPerTokenLayer(t *testing.T) {
	withRateLimitSettings(t, 1, 100, 0, map[string][2]int{"test": {5, 0}})
	mr := newMiniRedisLimiter(t)
	_ = mr

	router := buildRateLimitRouter(t, 1007, 0, "test", okJSONHandler())

	for i := 0; i < 6; i++ {
		require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code,
			"request %d should pass — per-token layer skipped when tokenId=0", i+1)
	}

	assert.False(t, mr.Exists("rateLimit:"+ModelRequestRateLimitTokenCount2Mark+":1007:0"),
		"no per-token total key should exist for tokenId=0")
	assert.False(t, mr.Exists("rateLimit:"+ModelRequestRateLimitTokenSuccessCount2Mark+":1007:0"),
		"no per-token success key should exist for tokenId=0")
}

// TestRedisRateLimitConcurrencyAtomicity verifies the atomic Lua script
// serializes concurrent consumers correctly: with maxCount=5, exactly 5 of
// 100 concurrent requests must succeed. The Lua script runs atomically under
// Redis's single-threaded evaluation, so there is no TOCTOU window between
// LLEN and LPUSH.
func TestRedisRateLimitConcurrencyAtomicity(t *testing.T) {
	withRateLimitSettings(t, 1, 5, 0, nil)
	mr := newMiniRedisLimiter(t)
	_ = mr

	router := buildRateLimitRouter(t, 2001, 701, "", okJSONHandler())

	var successCount int64
	var wg sync.WaitGroup
	const goroutines = 100
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			rec := fireRateLimitRequest(router)
			if rec.Code == http.StatusOK {
				atomic.AddInt64(&successCount, 1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(5), successCount,
		"exactly maxCount=5 requests should succeed under concurrent load (atomic Lua)")
}

// TestRedisRateLimitSuccessCounterAtomicity is the regression test for the
// success-counter TOCTOU. With a success cap of 5 and a total cap that never
// binds, exactly 5 of 100 concurrent successful requests must pass. The old
// read-only-check-then-record design let a concurrent burst all pass the check
// before any of them recorded, blowing past the cap; reserving the slot
// atomically up front (refunded on failure) removes that window.
func TestRedisRateLimitSuccessCounterAtomicity(t *testing.T) {
	// total=1000 never binds across 100 requests; success=5 is the only gate.
	withRateLimitSettings(t, 1, 1000, 5, nil)
	mr := newMiniRedisLimiter(t)
	_ = mr

	router := buildRateLimitRouter(t, 2005, 705, "", okJSONHandler())

	var successCount int64
	var wg sync.WaitGroup
	const goroutines = 100
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			rec := fireRateLimitRequest(router)
			if rec.Code == http.StatusOK {
				atomic.AddInt64(&successCount, 1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(5), successCount,
		"exactly successCount=5 requests should pass the success cap under concurrent load (atomic reserve + refund)")
}

// TestRedisRateLimitNoscriptResilience verifies that a SCRIPT FLUSH between
// requests does not surface as HTTP 500. go-redis's NewScript.Run retries
// EVAL transparently on NOSCRIPT; even in the degenerate case,
// slidingWindowConsume fail-closes to "deny" (429), never 500.
func TestRedisRateLimitNoscriptResilience(t *testing.T) {
	withRateLimitSettings(t, 1, 100, 0, nil)
	mr := newMiniRedisLimiter(t)
	_ = mr

	router := buildRateLimitRouter(t, 2002, 702, "", okJSONHandler())

	// Prime the script cache with one request.
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)

	// Flush all loaded scripts — next EVALSHA will return NOSCRIPT.
	ctx := context.Background()
	require.NoError(t, common.RDB.ScriptFlush(ctx).Err(),
		"miniredis must accept SCRIPT FLUSH")

	// The next request must NOT surface as HTTP 500. go-redis retries EVAL
	// transparently after NOSCRIPT, so this should be 200 (still under cap).
	rec := fireRateLimitRequest(router)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code,
		"NOSCRIPT must be handled transparently — no HTTP 500")
}

// TestRedisRateLimitExpireOnDeny verifies that a saturated key still has
// EXPIRE set by the deny branch (Lua line 35), so it cannot outlive the
// window. After FastForward(duration+2) the key must be gone from Redis.
func TestRedisRateLimitExpireOnDeny(t *testing.T) {
	const durationMinutes = 1
	withRateLimitSettings(t, durationMinutes, 2, 0, nil)
	mr := newMiniRedisLimiter(t)

	router := buildRateLimitRouter(t, 2003, 703, "", okJSONHandler())

	// Fill to max.
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code)

	// Deny a few — deny branch must still set EXPIRE.
	require.Equal(t, http.StatusTooManyRequests, fireRateLimitRequest(router).Code)
	require.Equal(t, http.StatusTooManyRequests, fireRateLimitRequest(router).Code)

	userKey := "rateLimit:" + ModelRequestRateLimitCount2Mark + ":2003"
	require.True(t, mr.Exists(userKey),
		"saturated key should exist immediately after denies")

	// TTL must be positive and bounded by duration+1 (the Lua deny branch
	// sets EXPIRE to duration+1).
	ttl := mr.TTL(userKey)
	require.True(t, ttl > 0 && ttl <= time.Duration(durationMinutes*60+1)*time.Second,
		"deny branch should set EXPIRE in (0, duration+1], got %v", ttl)

	// Advance miniredis clock past the window — key must expire.
	duration := int64(durationMinutes * 60)
	mr.FastForward(time.Duration(duration+2) * time.Second)
	assert.False(t, mr.Exists(userKey),
		"saturated key must be gone after FastForward(duration+2) — EXPIRE-on-deny prevents leaks")
}

// TestRedisRateLimitMaxCountZeroUnlimited verifies that maxCount=0 means
// unlimited: 100 requests all pass and no MRRL2 keys are written. The Go
// wrapper short-circuits before touching Redis when maxCount==0.
func TestRedisRateLimitMaxCountZeroUnlimited(t *testing.T) {
	withRateLimitSettings(t, 1, 0, 0, nil)
	mr := newMiniRedisLimiter(t)
	_ = mr

	router := buildRateLimitRouter(t, 2004, 704, "", okJSONHandler())

	for i := 0; i < 100; i++ {
		require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code,
			"request %d should pass with maxCount=0 (unlimited)", i+1)
	}

	matches, err := common.RDB.Keys(context.Background(), "rateLimit:"+ModelRequestRateLimitCount2Mark+":*").Result()
	require.NoError(t, err)
	assert.Empty(t, matches, "no MRRL2 keys should exist when maxCount=0")
}
