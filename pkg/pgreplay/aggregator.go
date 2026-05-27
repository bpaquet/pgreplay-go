package pgreplay

import (
	"regexp"
	"sort"
	"sync"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	queryDurationRatio = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "pgreplay_query_duration_ratio",
			Help:    "Ratio of replayed query duration over original (replay/original). 1.0 = same speed; >1.0 = replay slower.",
			Buckets: []float64{0.1, 0.5, 0.8, 1.0, 1.5, 2, 5, 10, 50, 100},
		},
	)
	queryDurationDiffMs = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "pgreplay_query_duration_diff_ms",
			Help:    "Replayed minus original duration, in ms.",
			Buckets: []float64{-100, -10, -1, 0, 1, 10, 100, 1000, 10000},
		},
	)
)

// FingerprintStats accumulates per-fingerprint replay statistics. Individual samples are
// retained so we can compute percentiles at summary time.
type FingerprintStats struct {
	OriginalMs  []float64
	ReplayMs    []float64
	SampleQuery string
}

// Aggregator records replay durations per query fingerprint and (optionally) logs queries
// that exceed slowdown thresholds.
type Aggregator struct {
	mu               sync.Mutex
	stats            map[string]*FingerprintStats
	logSlowdownAbove float64          // ratio threshold (0 = disabled)
	logSlowdownMin   time.Duration    // skip queries whose ORIGINAL duration is below this
	ignorePatterns   []*regexp.Regexp // queries matching any pattern are not recorded
	logger           kitlog.Logger
}

func NewAggregator(logSlowdownAbove float64, logSlowdownMin time.Duration, ignorePatterns []*regexp.Regexp, logger kitlog.Logger) *Aggregator {
	return &Aggregator{
		stats:            map[string]*FingerprintStats{},
		logSlowdownAbove: logSlowdownAbove,
		logSlowdownMin:   logSlowdownMin,
		ignorePatterns:   ignorePatterns,
		logger:           logger,
	}
}

// Record observes a single replay. originalMs may be 0 if the source log had no duration.
// Items whose query matches any of the configured ignore patterns are skipped entirely
// (no histogram update, no slow-query log, no per-fingerprint stats).
func (a *Aggregator) Record(item Item, replay time.Duration) {
	query := item.GetQuery()
	for _, re := range a.ignorePatterns {
		if re.MatchString(query) {
			return
		}
	}

	originalMs := item.GetOriginalDurationMs()
	replayMs := float64(replay.Microseconds()) / 1000.0

	if originalMs > 0 {
		ratio := replayMs / originalMs
		queryDurationRatio.Observe(ratio)
		queryDurationDiffMs.Observe(replayMs - originalMs)

		if a.logSlowdownAbove > 0 &&
			originalMs >= float64(a.logSlowdownMin)/float64(time.Millisecond) &&
			ratio >= a.logSlowdownAbove {
			level.Warn(a.logger).Log(
				"event", "slow_query",
				"fingerprint", item.GetFingerprint(),
				"original_ms", originalMs,
				"replay_ms", replayMs,
				"ratio", ratio,
				"user", item.GetUser(),
				"query", truncate(item.GetQuery(), 240),
			)
		}
	}

	fp := item.GetFingerprint()
	if fp == "" {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	s, ok := a.stats[fp]
	if !ok {
		s = &FingerprintStats{SampleQuery: item.GetQuery()}
		a.stats[fp] = s
	}
	s.OriginalMs = append(s.OriginalMs, originalMs)
	s.ReplayMs = append(s.ReplayMs, replayMs)
}

// FingerprintSummary is one row of an Aggregator summary.
type FingerprintSummary struct {
	Fingerprint   string
	Count         int64
	AvgOriginalMs float64
	AvgReplayMs   float64
	AvgRatio      float64

	P50OriginalMs float64
	P95OriginalMs float64
	P99OriginalMs float64
	P50ReplayMs   float64
	P95ReplayMs   float64
	P99ReplayMs   float64

	// Ratios computed at the same percentile of each distribution.
	P50Ratio float64
	P95Ratio float64
	P99Ratio float64

	SampleQuery string
}

// TopByP95Ratio returns the n fingerprints with the highest p95 slowdown ratio.
// Only fingerprints with both replay and original samples are considered.
func (a *Aggregator) TopByP95Ratio(n int) []FingerprintSummary {
	a.mu.Lock()
	out := make([]FingerprintSummary, 0, len(a.stats))
	for fp, s := range a.stats {
		if len(s.OriginalMs) == 0 || sum(s.OriginalMs) == 0 {
			continue
		}
		out = append(out, summarize(fp, s))
	}
	a.mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return out[i].P95Ratio > out[j].P95Ratio })
	if n > 0 && len(out) > n {
		out = out[:n]
	}
	return out
}

func summarize(fp string, s *FingerprintStats) FingerprintSummary {
	orig := append([]float64(nil), s.OriginalMs...)
	repl := append([]float64(nil), s.ReplayMs...)
	sort.Float64s(orig)
	sort.Float64s(repl)

	sumOrig, sumRepl := sum(s.OriginalMs), sum(s.ReplayMs)
	n := float64(len(s.OriginalMs))

	p50o, p95o, p99o := percentile(orig, 0.50), percentile(orig, 0.95), percentile(orig, 0.99)
	p50r, p95r, p99r := percentile(repl, 0.50), percentile(repl, 0.95), percentile(repl, 0.99)

	return FingerprintSummary{
		Fingerprint:   fp,
		Count:         int64(len(s.OriginalMs)),
		AvgOriginalMs: sumOrig / n,
		AvgReplayMs:   sumRepl / n,
		AvgRatio:      sumRepl / sumOrig,
		P50OriginalMs: p50o,
		P95OriginalMs: p95o,
		P99OriginalMs: p99o,
		P50ReplayMs:   p50r,
		P95ReplayMs:   p95r,
		P99ReplayMs:   p99r,
		P50Ratio:      safeDiv(p50r, p50o),
		P95Ratio:      safeDiv(p95r, p95o),
		P99Ratio:      safeDiv(p99r, p99o),
		SampleQuery:   s.SampleQuery,
	}
}

func sum(xs []float64) float64 {
	var s float64
	for _, x := range xs {
		s += x
	}
	return s
}

// percentile returns the q-th percentile of a sorted slice (linear interpolation).
func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	pos := q * float64(len(sorted)-1)
	lo := int(pos)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
