package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/setting"

	"github.com/gin-gonic/gin"
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

// Test 5: Check() is read-only — a failed request does not consume the
// success counter. This proves the _check hack (which used Request() and
// consumed on every check) is gone. Both user and token success counters
// follow the same behavior.
func TestModelRateLimitCheckIsReadOnly(t *testing.T) {
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

	// Request 2 (500): success counters do NOT increment (still 1).
	require.Equal(t, http.StatusInternalServerError, fireRateLimitRequest(router).Code)

	// Request 3 (200): passes because Check() is read-only — the failure did
	// not consume the success counter. With the old _check hack, the check
	// counter would be at 2 and this request would be blocked.
	require.Equal(t, http.StatusOK, fireRateLimitRequest(router).Code,
		"3rd request must pass — Check() is read-only, failure didn't consume success counter")

	// Request 4 (200): blocked — success counter now at 2 (from requests 1 and 3).
	require.Equal(t, http.StatusTooManyRequests, fireRateLimitRequest(router).Code,
		"4th request blocked by success counter (2/2)")

	// Verify token success counter also reached 2 (same behavior as user success).
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
