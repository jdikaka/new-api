-- Atomic sliding-window rate limiter.
-- KEYS[1]:   rate-limit list key (newest at head, oldest at tail)
-- ARGV[1]:   now         — current unix timestamp in seconds (caller-supplied)
-- ARGV[2]:   maxCount    — max requests allowed inside the window (>0)
-- ARGV[3]:   duration    — window length in seconds
--
-- Returns 1 when the request is allowed (and recorded), 0 when denied.
-- The deny branch still sets EXPIRE so the key cannot leak past window+1
-- seconds of inactivity.

local key      = KEYS[1]
local now      = tonumber(ARGV[1])
local maxCount = tonumber(ARGV[2])
local duration = tonumber(ARGV[3])

local length = redis.call('LLEN', key)

if length < maxCount then
    redis.call('LPUSH', key, now)
    redis.call('EXPIRE', key, duration + 1)
    return 1
end

-- Oldest entry is at the tail (LIFO: LPUSH adds to head).
local oldest = tonumber(redis.call('LINDEX', key, -1))
if now - oldest >= duration then
    redis.call('LPUSH', key, now)
    redis.call('LTRIM', key, 0, maxCount - 1)
    redis.call('EXPIRE', key, duration + 1)
    return 1
end

-- Deny: refresh TTL unconditionally so a saturated key cannot outlive the
-- oldest entry by more than duration+1 seconds.
redis.call('EXPIRE', key, duration + 1)
return 0
