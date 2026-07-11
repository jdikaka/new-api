package setting

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUpdateRateLimitGroupConcurrent exercises the write path
// (UpdateModelRequestRateLimitGroupByJSONString) against a concurrent reader
// (GetGroupRateLimit). UpdateModelRequestRateLimitGroupByJSONString reassigns
// the shared map and unmarshals into it, so it MUST hold the write lock; using
// RLock here would race against any RLock-holding reader and trigger a
// concurrent map read/write panic under the race detector.
func TestUpdateRateLimitGroupConcurrent(t *testing.T) {
	orig := ModelRequestRateLimitGroup
	t.Cleanup(func() { ModelRequestRateLimitGroup = orig })

	const writers = 100
	const readers = 100
	const iterations = 50

	const jsonStr = `{"test":[10,100]}`

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				require.NoError(t, UpdateModelRequestRateLimitGroupByJSONString(jsonStr))
			}
		}()
	}

	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				totalCount, successCount, found := GetGroupRateLimit("test")
				if found {
					require.Equal(t, 10, totalCount)
					require.Equal(t, 100, successCount)
				}
			}
		}()
	}

	wg.Wait()

	// Final state: the writers always set the same payload, so after all
	// goroutines complete the map must contain exactly that entry.
	totalCount, successCount, found := GetGroupRateLimit("test")
	require.True(t, found)
	require.Equal(t, 10, totalCount)
	require.Equal(t, 100, successCount)
}
