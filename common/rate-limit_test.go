package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestRateLimiter builds an InMemoryRateLimiter with an initialized store
// but without spawning the clearExpiredItems goroutine, so tests stay isolated
// and leak no goroutines.
func newTestRateLimiter() *InMemoryRateLimiter {
	return &InMemoryRateLimiter{
		store: make(map[string]*[]int64),
	}
}

// TestInMemoryRateLimiterCheck_FreshKeyReturnsTrueAndLeavesStoreEmpty
// verifies that Check on a key with no prior requests reports allowed and
// performs no write, so the store remains empty.
func TestInMemoryRateLimiterCheck_FreshKeyReturnsTrueAndLeavesStoreEmpty(t *testing.T) {
	l := newTestRateLimiter()

	allowed := l.Check("fresh-key", 5, 60)

	assert.True(t, allowed, "fresh key should be allowed")
	assert.Empty(t, l.store, "Check must not record into the store")
}

// TestInMemoryRateLimiterCheck_PartialFillReturnsTrueAndPreservesEntries
// verifies that after fewer Request calls than the limit, Check reports
// allowed and does not alter the recorded entry count.
func TestInMemoryRateLimiterCheck_PartialFillReturnsTrueAndPreservesEntries(t *testing.T) {
	l := newTestRateLimiter()
	const key = "partial-key"

	require.True(t, l.Request(key, 5, 60))
	require.True(t, l.Request(key, 5, 60))

	allowed := l.Check(key, 5, 60)

	assert.True(t, allowed, "partially filled window should be allowed")
	queue, ok := l.store[key]
	require.True(t, ok, "store entry must still exist")
	assert.Len(t, *queue, 2, "Check must not add entries to the queue")
}

// TestInMemoryRateLimiterCheck_FullWindowReturnsFalseAndPreservesEntries
// verifies that once the sliding window is full and unexpired, Check reports
// denied and does not alter the recorded entry count.
func TestInMemoryRateLimiterCheck_FullWindowReturnsFalseAndPreservesEntries(t *testing.T) {
	l := newTestRateLimiter()
	const key = "full-key"

	require.True(t, l.Request(key, 3, 60))
	require.True(t, l.Request(key, 3, 60))
	require.True(t, l.Request(key, 3, 60))

	allowed := l.Check(key, 3, 60)

	assert.False(t, allowed, "full unexpired window should be denied")
	queue, ok := l.store[key]
	require.True(t, ok, "store entry must still exist")
	assert.Len(t, *queue, 3, "Check must not add entries to the queue")
}

// TestInMemoryRateLimiterCheck_UnlimitedAlwaysReturnsTrue verifies that a
// maxRequestNum of 0 means unlimited: Check returns true even when the window
// is already full and would otherwise deny.
func TestInMemoryRateLimiterCheck_UnlimitedAlwaysReturnsTrue(t *testing.T) {
	l := newTestRateLimiter()
	const key = "unlimited-key"

	require.True(t, l.Request(key, 1, 60))
	require.False(t, l.Check(key, 1, 60), "sanity: window is full and should deny")

	allowed := l.Check(key, 0, 60)

	assert.True(t, allowed, "maxRequestNum==0 must be unlimited regardless of state")
}

// TestInMemoryRateLimiterCheck_SlidingWindowExpires verifies that after the
// duration elapses, Check reports allowed again because the oldest entry has
// aged out of the sliding window.
func TestInMemoryRateLimiterCheck_SlidingWindowExpires(t *testing.T) {
	l := newTestRateLimiter()
	const key = "slide-key"

	require.True(t, l.Request(key, 3, 1))
	require.True(t, l.Request(key, 3, 1))
	require.True(t, l.Request(key, 3, 1))
	require.False(t, l.Check(key, 3, 1), "sanity: window is full within duration")

	time.Sleep(1100 * time.Millisecond)

	allowed := l.Check(key, 3, 1)

	assert.True(t, allowed, "window should allow after duration has elapsed")
}
