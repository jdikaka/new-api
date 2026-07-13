package middleware

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/setting"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
)

//go:embed lua/sliding_window_consume.lua
var slidingWindowConsumeLua string

const (
	ModelRequestRateLimitCountMark             = "MRRL"
	ModelRequestRateLimitSuccessCountMark      = "MRRLS"
	ModelRequestRateLimitTokenCountMark        = "MRRLT"
	ModelRequestRateLimitTokenSuccessCountMark = "MRRLTS"
	// The *2 marks back the atomic Lua sliding-window counters. They stay
	// disjoint from the in-memory (non-2) marks used by the memory path, so a
	// deploy can never mix the Lua int-list format with stale string-list keys
	// in the same Redis key.
	ModelRequestRateLimitCount2Mark             = "MRRL2"
	ModelRequestRateLimitTokenCount2Mark        = "MRRLT2"
	ModelRequestRateLimitSuccessCount2Mark      = "MRRLS2"
	ModelRequestRateLimitTokenSuccessCount2Mark = "MRRLTS2"
)

// slidingWindowConsume atomically records and checks a request against the
// sliding-window limit in Redis. maxCount==0 means unlimited and bypasses
// Redis entirely. duration and now are in seconds; now is caller-supplied so
// the caller can refund the exact entry it recorded (see slidingWindowRefund).
// Returns false when the limit is reached or Redis reports an error; the error
// is logged via common.SysError before returning false (fail-closed).
func slidingWindowConsume(ctx context.Context, rdb *redis.Client, key string, maxCount int, duration, now int64) bool {
	if maxCount == 0 {
		return true
	}
	result, err := redis.NewScript(slidingWindowConsumeLua).Run(
		ctx, rdb,
		[]string{key},
		now, maxCount, duration,
	).Int()
	if err != nil {
		common.SysError(fmt.Sprintf("slidingWindowConsume failed: %v", err))
		return false
	}
	return result == 1
}

// slidingWindowRefund removes one occurrence of the timestamp that
// slidingWindowConsume recorded for key, releasing a reserved slot. It rolls
// back a success-counter reservation when the owning request fails (HTTP >=
// 400) or is denied by a later guard. now must match the value passed to
// slidingWindowConsume. The op is a no-op when the entry has already aged out
// (the limiter then stays slightly stricter, never too lax).
func slidingWindowRefund(ctx context.Context, rdb *redis.Client, key string, now int64) {
	if err := rdb.LRem(ctx, key, 1, now).Err(); err != nil {
		common.SysError(fmt.Sprintf("slidingWindowRefund failed: %v", err))
	}
}

// rateLimitMsg translates a rate-limit message for the current request, filling
// in the window length (minutes) and the threshold that was hit.
func rateLimitMsg(c *gin.Context, key string, max int) string {
	return common.TranslateMessage(c, key, map[string]any{
		"Minutes": setting.ModelRequestRateLimitDurationMinutes,
		"Max":     max,
	})
}

// redisRateLimitHandler enforces four thresholds. The success counters
// (userSuccess, tokenSuccess) are reserved atomically up front and refunded if
// the request ends up denied or fails (any HTTP >= 400, including the 429s
// raised by later guards). The total counters (userTotal, tokenTotal) are
// consumed up front and never refunded — failed requests count toward the
// total. Reserving success atomically (rather than a read-only check before
// and a record after) closes the TOCTOU window that let a concurrent burst
// slip past the success cap. duration is in seconds.
func redisRateLimitHandler(duration int64, userTotal, userSuccess, tokenTotal, tokenSuccess int) gin.HandlerFunc {
	return func(c *gin.Context) {
		userId := strconv.Itoa(c.GetInt("id"))
		tokenId := strconv.Itoa(c.GetInt("token_id"))
		ctx := context.Background()
		rdb := common.RDB
		now := time.Now().Unix()

		// Success slots reserved below; refunded once if the final response is
		// a failure. Reading status in the defer covers every deny path (a
		// later guard's 429) and downstream failures uniformly.
		var reserved []string
		defer func() {
			if c.Writer.Status() < http.StatusBadRequest {
				return
			}
			for _, key := range reserved {
				slidingWindowRefund(ctx, rdb, key, now)
			}
		}()

		// 1. Reserve user success slot (atomic).
		if userSuccess > 0 {
			key := fmt.Sprintf("rateLimit:%s:%s", ModelRequestRateLimitSuccessCount2Mark, userId)
			if !slidingWindowConsume(ctx, rdb, key, userSuccess, duration, now) {
				abortWithOpenAiMessage(c, http.StatusTooManyRequests, rateLimitMsg(c, i18n.MsgRateLimitReached, userSuccess))
				return
			}
			reserved = append(reserved, key)
		}

		// 2. Reserve token success slot (atomic).
		if tokenSuccess > 0 && c.GetInt("token_id") != 0 {
			key := fmt.Sprintf("rateLimit:%s:%s:%s", ModelRequestRateLimitTokenSuccessCount2Mark, userId, tokenId)
			if !slidingWindowConsume(ctx, rdb, key, tokenSuccess, duration, now) {
				abortWithOpenAiMessage(c, http.StatusTooManyRequests, rateLimitMsg(c, i18n.MsgRateLimitTokenReached, tokenSuccess))
				return
			}
			reserved = append(reserved, key)
		}

		// 3. Consume user total (atomic, non-refundable).
		if userTotal > 0 {
			key := fmt.Sprintf("rateLimit:%s:%s", ModelRequestRateLimitCount2Mark, userId)
			if !slidingWindowConsume(ctx, rdb, key, userTotal, duration, now) {
				abortWithOpenAiMessage(c, http.StatusTooManyRequests, rateLimitMsg(c, i18n.MsgRateLimitTotalReached, userTotal))
				return
			}
		}

		// 4. Consume token total (atomic, non-refundable).
		if tokenTotal > 0 && c.GetInt("token_id") != 0 {
			key := fmt.Sprintf("rateLimit:%s:%s:%s", ModelRequestRateLimitTokenCount2Mark, userId, tokenId)
			if !slidingWindowConsume(ctx, rdb, key, tokenTotal, duration, now) {
				abortWithOpenAiMessage(c, http.StatusTooManyRequests, rateLimitMsg(c, i18n.MsgRateLimitTokenTotalReached, tokenTotal))
				return
			}
		}

		// 5. Process request. A failed response (>= 400) triggers the deferred
		// refund above, releasing the reserved success slots.
		c.Next()
	}
}

// memoryRateLimitHandler mirrors the Redis handler's dual-layer logic using
// the in-memory sliding-window limiter. duration is in seconds.
func memoryRateLimitHandler(duration int64, userTotal, userSuccess, tokenTotal, tokenSuccess int) gin.HandlerFunc {
	inMemoryRateLimiter.Init(time.Duration(setting.ModelRequestRateLimitDurationMinutes) * time.Minute)

	return func(c *gin.Context) {
		userId := strconv.Itoa(c.GetInt("id"))
		tokenId := strconv.Itoa(c.GetInt("token_id"))
		now := time.Now().Unix()

		userTotalKey := ModelRequestRateLimitCountMark + userId
		userSuccessKey := ModelRequestRateLimitSuccessCountMark + userId
		tokenTotalKey := ModelRequestRateLimitTokenCountMark + userId + ":" + tokenId
		tokenSuccessKey := ModelRequestRateLimitTokenSuccessCountMark + userId + ":" + tokenId

		var reserved []string
		defer func() {
			if c.Writer.Status() < http.StatusBadRequest {
				return
			}
			for _, key := range reserved {
				inMemoryRateLimiter.Cancel(key, now)
			}
		}()

		// 1. Reserve user success slot (atomic).
		if userSuccess > 0 {
			if !inMemoryRateLimiter.RequestAt(userSuccessKey, userSuccess, duration, now) {
				abortWithOpenAiMessage(c, http.StatusTooManyRequests, rateLimitMsg(c, i18n.MsgRateLimitReached, userSuccess))
				return
			}
			reserved = append(reserved, userSuccessKey)
		}

		// 2. Reserve token success slot (atomic).
		if tokenSuccess > 0 && tokenId != "0" {
			if !inMemoryRateLimiter.RequestAt(tokenSuccessKey, tokenSuccess, duration, now) {
				abortWithOpenAiMessage(c, http.StatusTooManyRequests, rateLimitMsg(c, i18n.MsgRateLimitTokenReached, tokenSuccess))
				return
			}
			reserved = append(reserved, tokenSuccessKey)
		}

		// 3. Consume user total (atomic, non-refundable).
		if userTotal > 0 && !inMemoryRateLimiter.RequestAt(userTotalKey, userTotal, duration, now) {
			abortWithOpenAiMessage(c, http.StatusTooManyRequests, rateLimitMsg(c, i18n.MsgRateLimitTotalReached, userTotal))
			return
		}

		// 4. Consume token total (atomic, non-refundable).
		if tokenTotal > 0 && tokenId != "0" && !inMemoryRateLimiter.RequestAt(tokenTotalKey, tokenTotal, duration, now) {
			abortWithOpenAiMessage(c, http.StatusTooManyRequests, rateLimitMsg(c, i18n.MsgRateLimitTokenTotalReached, tokenTotal))
			return
		}

		// 5. Process request. A failed response (>= 400) triggers the deferred
		// refund above, releasing the reserved success slots.
		c.Next()
	}
}

// ModelRequestRateLimit is the model request rate limiting middleware.
// It enforces two layers: a user-aggregate ceiling (always global thresholds)
// and a per-token layer (group config → global default fallback).
func ModelRequestRateLimit() func(c *gin.Context) {
	return func(c *gin.Context) {
		if !setting.ModelRequestRateLimitEnabled {
			c.Next()
			return
		}

		duration := int64(setting.ModelRequestRateLimitDurationMinutes * 60)

		// Defensive guard: skip rate limiting entirely if duration is
		// non-positive (prevents degenerate behavior when admin sets
		// DurationMinutes=0).
		if duration <= 0 {
			c.Next()
			return
		}

		// User-aggregate thresholds: always global.
		userTotal := setting.ModelRequestRateLimitCount
		userSuccess := setting.ModelRequestRateLimitSuccessCount

		// Per-token thresholds: group config → global default.
		tokenTotal := userTotal
		tokenSuccess := userSuccess
		group := common.GetContextKeyString(c, constant.ContextKeyTokenGroup)
		if group == "" {
			group = common.GetContextKeyString(c, constant.ContextKeyUserGroup)
		}
		groupTotalCount, groupSuccessCount, found := setting.GetGroupRateLimit(group)
		if found {
			tokenTotal = groupTotalCount
			tokenSuccess = groupSuccessCount
		}

		if common.RedisEnabled {
			redisRateLimitHandler(duration, userTotal, userSuccess, tokenTotal, tokenSuccess)(c)
		} else {
			memoryRateLimitHandler(duration, userTotal, userSuccess, tokenTotal, tokenSuccess)(c)
		}
	}
}
