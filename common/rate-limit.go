package common

import (
	"sync"
	"time"
)

type InMemoryRateLimiter struct {
	store              map[string]*[]int64
	mutex              sync.Mutex
	expirationDuration time.Duration
}

func (l *InMemoryRateLimiter) Init(expirationDuration time.Duration) {
	if l.store == nil {
		l.mutex.Lock()
		if l.store == nil {
			l.store = make(map[string]*[]int64)
			l.expirationDuration = expirationDuration
			if expirationDuration > 0 {
				go l.clearExpiredItems()
			}
		}
		l.mutex.Unlock()
	}
}

func (l *InMemoryRateLimiter) clearExpiredItems() {
	for {
		time.Sleep(l.expirationDuration)
		l.mutex.Lock()
		now := time.Now().Unix()
		for key := range l.store {
			queue := l.store[key]
			size := len(*queue)
			if size == 0 || now-(*queue)[size-1] > int64(l.expirationDuration.Seconds()) {
				delete(l.store, key)
			}
		}
		l.mutex.Unlock()
	}
}

// Check performs a read-only sliding window check with the same logic as
// Request but without recording the request. It returns true if a request to
// key would be allowed. maxRequestNum == 0 means unlimited.
func (l *InMemoryRateLimiter) Check(key string, maxRequestNum int, duration int64) bool {
	if maxRequestNum == 0 {
		return true
	}
	l.mutex.Lock()
	defer l.mutex.Unlock()
	queue, ok := l.store[key]
	if !ok {
		return true
	}
	now := time.Now().Unix()
	if len(*queue) < maxRequestNum {
		return true
	}
	return now-(*queue)[0] >= duration
}

// Request records a request and reports whether it is allowed under the
// sliding-window limit. duration's unit is seconds. It is a thin wrapper over
// RequestAt using the current time, kept for callers that do not need to roll
// back a reservation.
func (l *InMemoryRateLimiter) Request(key string, maxRequestNum int, duration int64) bool {
	return l.RequestAt(key, maxRequestNum, duration, time.Now().Unix())
}

// RequestAt is like Request but takes a caller-supplied timestamp (seconds) so
// the caller can later release the slot with Cancel using the same value. The
// queue is ordered [oldest <-- newest]; maxRequestNum==0 is treated as
// unlimited by the caller, never passed here.
func (l *InMemoryRateLimiter) RequestAt(key string, maxRequestNum int, duration, now int64) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	queue, ok := l.store[key]
	if ok {
		if len(*queue) < maxRequestNum {
			*queue = append(*queue, now)
			return true
		}
		if now-(*queue)[0] >= duration {
			*queue = (*queue)[1:]
			*queue = append(*queue, now)
			return true
		}
		return false
	}
	s := make([]int64, 0, maxRequestNum)
	l.store[key] = &s
	*(l.store[key]) = append(*(l.store[key]), now)
	return true
}

// Cancel releases one slot previously reserved by RequestAt, removing the
// newest entry equal to now. It rolls back a success-counter reservation when
// the owning request fails (HTTP >= 400). A no-op if the entry has already
// aged out — the limiter then stays slightly stricter, never too lax.
func (l *InMemoryRateLimiter) Cancel(key string, now int64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	queue, ok := l.store[key]
	if !ok {
		return
	}
	q := *queue
	for i := len(q) - 1; i >= 0; i-- {
		if q[i] == now {
			*queue = append(q[:i], q[i+1:]...)
			return
		}
	}
}
