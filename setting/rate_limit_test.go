package setting

import (
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
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

// TestCheckModelRequestRateLimitGroup covers the validation contract: total
// count (limits[0]) may be 0 (unlimited) but not negative; success count
// (limits[1]) must be at least 1; both must fit in int32; and malformed JSON
// is rejected. Each branch must report a distinct, accurate error.
func TestCheckModelRequestRateLimitGroup(t *testing.T) {
	cases := []struct {
		name      string
		jsonStr   string
		wantErr   bool
		wantInErr string // empty → not checked
	}{
		{name: "valid unlimited total", jsonStr: `{"g":[0,5]}`, wantErr: false},
		{name: "valid positive pair", jsonStr: `{"g":[10,100]}`, wantErr: false},
		{name: "negative total rejected", jsonStr: `{"g":[-1,5]}`, wantErr: true, wantInErr: "total count must be >= 0"},
		{name: "zero success rejected", jsonStr: `{"g":[10,0]}`, wantErr: true, wantInErr: "success count must be >= 1"},
		{name: "negative success rejected", jsonStr: `{"g":[10,-3]}`, wantErr: true, wantInErr: "success count must be >= 1"},
		{name: "total overflow rejected", jsonStr: `{"g":[2147483648,5]}`, wantErr: true, wantInErr: "exceeds maximum value"},
		{name: "success overflow rejected", jsonStr: `{"g":[10,2147483648]}`, wantErr: true, wantInErr: "exceeds maximum value"},
		{name: "malformed json rejected", jsonStr: `{not json`, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckModelRequestRateLimitGroup(tc.jsonStr)
			if tc.wantErr {
				require.Error(t, err)
				if tc.wantInErr != "" {
					assert.True(t, strings.Contains(err.Error(), tc.wantInErr),
						"error %q should contain %q", err.Error(), tc.wantInErr)
				}
				return
			}
			require.NoError(t, err)
		})
	}

	// Sanity: the upper bound the overflow branch compares against is int32.
	assert.Equal(t, math.MaxInt32, 2147483647)
}
