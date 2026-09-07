package sluice

import (
	"sync/atomic"
	"time"
)

// monoEpoch anchors Sluice's timeline at package initialisation. Every
// timestamp the engine stores is a monotonic nanosecond delta from this point,
// never a wall-clock instant.
//
// This matters for correctness, not just taste. A wall clock can step backwards
// (NTP slew, a leap-second smear, an operator running date -s) and a token
// bucket driven by a clock that steps backwards either stalls for the length of
// the step or, worse, hands out an unbounded burst. time.Since() on a Time that
// carries a monotonic reading compiles down to a nanotime() read minus a
// constant and is immune to all of that.
var monoEpoch = time.Now()

// Now returns the engine's current timestamp in nanoseconds since process
// start. Callers that already maintain their own cached timestamp (a common
// pattern in event-loop network servers, where reading the clock per packet is
// itself a measurable cost) should source it from here and feed it to AllowAt.
func Now() uint64 {
	return uint64(time.Since(monoEpoch))
}

// now returns the timestamp the engine should use for a decision: either a
// direct clock read, or the coarse cached value maintained by the background
// ticker when the operator has traded refill granularity for clock-read cost.
//
// Using a coarse clock does not leak or duplicate tokens. Both the stored
// lastRefill and the incoming now come from the same quantised source, so the
// intervals still tile the timeline exactly; the only effect is that refills
// arrive in steps of the configured resolution instead of continuously.
func (e *Engine) now() uint64 {
	if e.coarse {
		return atomic.LoadUint64(&e.ctl.coarseNanos)
	}
	return uint64(time.Since(monoEpoch))
}

// startCoarseClock spins the single background goroutine Sluice ever creates.
// It writes one word per tick into the arena's control line, which every
// worker then reads with a plain atomic load - turning a ~20ns clock syscall
// (vDSO or otherwise) on the request path into a ~1ns L1 hit.
func (e *Engine) startCoarseClock(res time.Duration) {
	atomic.StoreUint64(&e.ctl.coarseNanos, uint64(time.Since(monoEpoch)))
	e.coarse = true
	e.stop = make(chan struct{})
	e.clockDone = make(chan struct{})
	go func() {
		defer close(e.clockDone)
		t := time.NewTicker(res)
		defer t.Stop()
		for {
			select {
			case <-e.stop:
				return
			case <-t.C:
				atomic.StoreUint64(&e.ctl.coarseNanos, uint64(time.Since(monoEpoch)))
			}
		}
	}()
}

// atomicTouch dirties a word without changing it, forcing the kernel to commit
// a physical page behind the address.
func atomicTouch(p *uint64) {
	atomic.AddUint64(p, 0)
}

// ---------------------------------------------------------------------------
// Fixed-point rate arithmetic
// ---------------------------------------------------------------------------

// maxRateFP is the largest representable per-nanosecond refill rate.
const maxRateFP = uint64(1)<<32 - 1

// MaxRatePerSecond is the highest refill rate a slot can express
// (~1e9 tokens/sec, i.e. one token per nanosecond).
const MaxRatePerSecond = float64(maxRateFP) * nanosPerSecond / float64(TokenScale)

// MinRatePerSecond is the lowest non-zero refill rate a slot can express. A
// requested rate below this is clamped up to it rather than silently rounded to
// "never refills", which would turn a rate limiter into a one-shot quota.
const MinRatePerSecond = nanosPerSecond / float64(TokenScale)

// rateToFP converts tokens-per-second into the Q0.32 tokens-per-nanosecond
// value stored in a slot's rateFP field.
//
// Quantisation is inherent to packing a rate into 32 bits: the stored value is
// perSec * 2^32 / 1e9, so the relative error is bounded by 1/(2*rateFP). At
// 1000 tokens/sec that is 0.012%; at 1 token/sec it is 12%. EffectiveRate
// reports exactly what a slot will actually deliver, and callers needing
// precision at very low rates should scale their token unit (bill in
// milli-tokens) rather than fight the representation.
func rateToFP(perSec float64) (uint32, error) {
	if perSec < 0 || perSec != perSec { // negative or NaN
		return 0, ErrInvalidRate
	}
	if perSec == 0 {
		return 0, nil // a static quota: consumes, never refills
	}
	if perSec > MaxRatePerSecond {
		return 0, ErrRateOutOfRange
	}
	fp := uint64(perSec*(float64(TokenScale)/nanosPerSecond) + 0.5)
	if fp == 0 {
		fp = 1
	}
	if fp > maxRateFP {
		fp = maxRateFP
	}
	return uint32(fp), nil
}

// fpToRate is the inverse of rateToFP: the rate a slot genuinely delivers.
func fpToRate(fp uint32) float64 {
	return float64(fp) * nanosPerSecond / float64(TokenScale)
}

// scaledRefill computes elapsed * rateFP - the number of Q32.32 tokens accrued
// over an interval - saturating at 2^64-1 instead of wrapping.
//
// The product genuinely can exceed 64 bits: a slot that has been idle for a
// week (6e14 ns) at a high rate overflows trivially, and a wrapped product
// would hand the tenant a spurious near-empty bucket instead of a full one.
// Rather than pay for a division to pre-clamp elapsed, the multiply is done in
// two 32-bit limbs and the carry out of bit 63 is detected directly; any
// saturation is then absorbed by the capacity clamp in refill(), since a
// saturated value is by construction larger than any representable capacity.
//
// math/bits.Mul64 would express this in one line, but the package is held to
// syscall/unsafe/sync-atomic/time only, so the limbs are done by hand.
func scaledRefill(elapsed uint64, rateFP uint32) uint64 {
	r := uint64(rateFP)
	lo := (elapsed & 0xFFFFFFFF) * r // < 2^64, exact
	hi := (elapsed >> 32) * r        // < 2^64, exact
	if hi > 0xFFFFFFFF {             // hi<<32 would discard set bits
		return ^uint64(0)
	}
	sum := lo + (hi << 32)
	if sum < lo { // carry out of the top bit
		return ^uint64(0)
	}
	return sum
}
