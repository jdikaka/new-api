package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/common/limiter"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/setting"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
)

const (
	ModelRequestRateLimitCountMark             = "MRRL"
	ModelRequestRateLimitSuccessCountMark      = "MRRLS"
	ModelRequestRateLimitTokenCountMark        = "MRRLT"
	ModelRequestRateLimitTokenSuccessCountMark = "MRRLTS"
)

// checkRedisRateLimit checks whether a request would be allowed under the
// sliding-window count limit, without recording it. maxCount==0 means
// unlimited. duration is in seconds.
func checkRedisRateLimit(ctx context.Context, rdb *redis.Client, key string, maxCount int, duration int64) (bool, error) {
	if maxCount == 0 {
		return true, nil
	}

	length, err := rdb.LLen(ctx, key).Result()
	if err != nil {
		return false, err
	}

	if length < int64(maxCount) {
		return true, nil
	}

	oldTimeStr, _ := rdb.LIndex(ctx, key, -1).Result()
	oldTime, err := time.Parse(timeFormat, oldTimeStr)
	if err != nil {
		return false, err
	}

	nowTimeStr := time.Now().Format(timeFormat)
	nowTime, err := time.Parse(timeFormat, nowTimeStr)
	if err != nil {
		return false, err
	}

	subTime := nowTime.Sub(oldTime).Seconds()
	if int64(subTime) < duration {
		rdb.Expire(ctx, key, time.Duration(duration)*time.Second)
		return false, nil
	}

	return true, nil
}

// recordRedisRequest pushes a timestamp onto the counter list and trims it to
// maxCount entries. maxCount==0 is a no-op. duration is in seconds.
func recordRedisRequest(ctx context.Context, rdb *redis.Client, key string, maxCount int, duration int64) {
	if maxCount == 0 {
		return
	}

	now := time.Now().Format(timeFormat)
	rdb.LPush(ctx, key, now)
	rdb.LTrim(ctx, key, 0, int64(maxCount-1))
	rdb.Expire(ctx, key, time.Duration(duration)*time.Second)
}

// redisRateLimitHandler enforces four thresholds:
//   - userSuccess: user-aggregate successful-request cap (read-only check)
//   - tokenSuccess: per-token successful-request cap (read-only check)
//   - userTotal: user-aggregate total-request cap (atomic token-bucket consume)
//   - tokenTotal: per-token total-request cap (atomic token-bucket consume)
//
// duration is in seconds.
func redisRateLimitHandler(duration int64, userTotal, userSuccess, tokenTotal, tokenSuccess int) gin.HandlerFunc {
	return func(c *gin.Context) {
		userId := strconv.Itoa(c.GetInt("id"))
		tokenId := strconv.Itoa(c.GetInt("token_id"))
		ctx := context.Background()
		rdb := common.RDB

		// 1. User success check (read-only)
		if userSuccess > 0 {
			successKey := fmt.Sprintf("rateLimit:%s:%s", ModelRequestRateLimitSuccessCountMark, userId)
			allowed, err := checkRedisRateLimit(ctx, rdb, successKey, userSuccess, duration)
			if err != nil {
				fmt.Println("检查用户成功请求数限制失败:", err.Error())
				abortWithOpenAiMessage(c, http.StatusInternalServerError, "rate_limit_check_failed")
				return
			}
			if !allowed {
				abortWithOpenAiMessage(c, http.StatusTooManyRequests, fmt.Sprintf("您已达到请求数限制：%d分钟内最多请求%d次", setting.ModelRequestRateLimitDurationMinutes, userSuccess))
				return
			}
		}

		// 2. Token success check (read-only)
		if tokenSuccess > 0 && c.GetInt("token_id") != 0 {
			tokenSuccessKey := fmt.Sprintf("rateLimit:%s:%s:%s", ModelRequestRateLimitTokenSuccessCountMark, userId, tokenId)
			allowed, err := checkRedisRateLimit(ctx, rdb, tokenSuccessKey, tokenSuccess, duration)
			if err != nil {
				fmt.Println("检查令牌成功请求数限制失败:", err.Error())
				abortWithOpenAiMessage(c, http.StatusInternalServerError, "rate_limit_check_failed")
				return
			}
			if !allowed {
				abortWithOpenAiMessage(c, http.StatusTooManyRequests, fmt.Sprintf("该令牌的成功请求次数已达到上限：%d分钟内最多请求%d次", setting.ModelRequestRateLimitDurationMinutes, tokenSuccess))
				return
			}
		}

		// 3. User total consume (atomic token bucket)
		if userTotal > 0 {
			userTotalKey := fmt.Sprintf("rateLimit:%s:%s", ModelRequestRateLimitCountMark, userId)
			tb := limiter.New(ctx, rdb)
			allowed, err := tb.Allow(
				ctx,
				userTotalKey,
				limiter.WithCapacity(int64(userTotal)*duration),
				limiter.WithRate(int64(userTotal)),
				limiter.WithRequested(duration),
			)
			if err != nil {
				fmt.Println("检查用户总请求数限制失败:", err.Error())
				abortWithOpenAiMessage(c, http.StatusInternalServerError, "rate_limit_check_failed")
				return
			}
			if !allowed {
				abortWithOpenAiMessage(c, http.StatusTooManyRequests, fmt.Sprintf("您已达到总请求数限制：%d分钟内最多请求%d次，包括失败次数，请检查您的请求是否正确", setting.ModelRequestRateLimitDurationMinutes, userTotal))
				return
			}
		}

		// 4. Token total consume (atomic token bucket)
		if tokenTotal > 0 && c.GetInt("token_id") != 0 {
			tokenTotalKey := fmt.Sprintf("rateLimit:%s:%s:%s", ModelRequestRateLimitTokenCountMark, userId, tokenId)
			tb := limiter.New(ctx, rdb)
			allowed, err := tb.Allow(
				ctx,
				tokenTotalKey,
				limiter.WithCapacity(int64(tokenTotal)*duration),
				limiter.WithRate(int64(tokenTotal)),
				limiter.WithRequested(duration),
			)
			if err != nil {
				fmt.Println("检查令牌总请求数限制失败:", err.Error())
				abortWithOpenAiMessage(c, http.StatusInternalServerError, "rate_limit_check_failed")
				return
			}
			if !allowed {
				abortWithOpenAiMessage(c, http.StatusTooManyRequests, fmt.Sprintf("该令牌的请求次数已达到上限：%d分钟内最多请求%d次，包括失败次数，请检查您的请求是否正确", setting.ModelRequestRateLimitDurationMinutes, tokenTotal))
				return
			}
		}

		// 5. Process request
		c.Next()

		// 6. Record success counters
		if c.Writer.Status() < 400 {
			if userSuccess > 0 {
				successKey := fmt.Sprintf("rateLimit:%s:%s", ModelRequestRateLimitSuccessCountMark, userId)
				recordRedisRequest(ctx, rdb, successKey, userSuccess, duration)
			}
			if tokenSuccess > 0 && c.GetInt("token_id") != 0 {
				tokenSuccessKey := fmt.Sprintf("rateLimit:%s:%s:%s", ModelRequestRateLimitTokenSuccessCountMark, userId, tokenId)
				recordRedisRequest(ctx, rdb, tokenSuccessKey, tokenSuccess, duration)
			}
		}
	}
}

// memoryRateLimitHandler mirrors the Redis handler's dual-layer logic using
// the in-memory sliding-window limiter. duration is in seconds.
func memoryRateLimitHandler(duration int64, userTotal, userSuccess, tokenTotal, tokenSuccess int) gin.HandlerFunc {
	inMemoryRateLimiter.Init(time.Duration(setting.ModelRequestRateLimitDurationMinutes) * time.Minute)

	return func(c *gin.Context) {
		userId := strconv.Itoa(c.GetInt("id"))
		tokenId := strconv.Itoa(c.GetInt("token_id"))

		userTotalKey := ModelRequestRateLimitCountMark + userId
		userSuccessKey := ModelRequestRateLimitSuccessCountMark + userId
		tokenTotalKey := ModelRequestRateLimitTokenCountMark + userId + ":" + tokenId
		tokenSuccessKey := ModelRequestRateLimitTokenSuccessCountMark + userId + ":" + tokenId

		// 1. User success check (read-only)
		if userSuccess > 0 && !inMemoryRateLimiter.Check(userSuccessKey, userSuccess, duration) {
			c.Status(http.StatusTooManyRequests)
			c.Abort()
			return
		}

		// 2. Token success check (read-only)
		if tokenSuccess > 0 && tokenId != "0" && !inMemoryRateLimiter.Check(tokenSuccessKey, tokenSuccess, duration) {
			c.Status(http.StatusTooManyRequests)
			c.Abort()
			return
		}

		// 3. User total consume (atomic)
		if userTotal > 0 && !inMemoryRateLimiter.Request(userTotalKey, userTotal, duration) {
			c.Status(http.StatusTooManyRequests)
			c.Abort()
			return
		}

		// 4. Token total consume (atomic)
		if tokenTotal > 0 && tokenId != "0" && !inMemoryRateLimiter.Request(tokenTotalKey, tokenTotal, duration) {
			c.Status(http.StatusTooManyRequests)
			c.Abort()
			return
		}

		// 5. Process request
		c.Next()

		// 6. Record success counters
		if c.Writer.Status() < 400 {
			if userSuccess > 0 {
				inMemoryRateLimiter.Request(userSuccessKey, userSuccess, duration)
			}
			if tokenSuccess > 0 && tokenId != "0" {
				inMemoryRateLimiter.Request(tokenSuccessKey, tokenSuccess, duration)
			}
		}
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
