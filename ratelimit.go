package main

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	rlWindow      = 60 * time.Second
	rlMaxAttempts = 10
)

type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string][]time.Time
}

var rl *rateLimiter

func newRateLimiter() *rateLimiter {
	r := &rateLimiter{buckets: make(map[string][]time.Time)}
	go r.cleanup()
	return r
}

func (r *rateLimiter) allow(ip string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-rlWindow)

	prev := r.buckets[ip]
	fresh := prev[:0]
	for _, t := range prev {
		if t.After(cutoff) {
			fresh = append(fresh, t)
		}
	}

	if len(fresh) >= rlMaxAttempts {
		r.buckets[ip] = fresh
		return false
	}

	r.buckets[ip] = append(fresh, now)
	return true
}

func (r *rateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	for range ticker.C {
		r.mu.Lock()
		cutoff := time.Now().Add(-rlWindow)
		for ip, times := range r.buckets {
			fresh := times[:0]
			for _, t := range times {
				if t.After(cutoff) {
					fresh = append(fresh, t)
				}
			}
			if len(fresh) == 0 {
				delete(r.buckets, ip)
			} else {
				r.buckets[ip] = fresh
			}
		}
		r.mu.Unlock()
	}
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.SplitN(xff, ",", 2)[0])
	}
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return host
}
