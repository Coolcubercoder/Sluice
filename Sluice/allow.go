package sluice

import "sync/atomic"

// maxRequest is the largest token count a single call may ask for. Requests are
// converted into Q32.32 fixed point by a 32-bit left shift, so anything wider
// would overflow the shift rather than be rejected.
const maxRequest = uint64(1)<<32 - 1

// ---------------------------------------------------------------------------
// The hot path.
//
// A decision is two cooperating lock-free protocols over the slot's two 64-bit
// words. They cannot be fused into one CAS - there is no portable 128-bit
// compare-and-swap in Go - so instead each word is given a protocol that is
// individually linearisable and jointly conservative:
//
//	lastRefill  is an interval claim. A refiller CASes it from the timestamp it
//	            read to the timestamp it observed now. Exactly one thread can
//	            win any given nanosecond interval, and successive winners claim
//	            strictly disjoint, contiguous intervals that tile the timeline.
//	            Tokens for an interval are therefore minted exactly once, no
//	            matter how many threads race - the losers simply skip refilling,
//	            because the winner is already minting on their behalf.
//
//	tokens      is a bounded accumulator. Refill adds and clamps to capacity;
//	            consumption subtracts only when the balance covers the request.
//	            Both are ordinary CAS loops, and both are ABA-immune because the
//	            value is a quantity, not a pointer: a balance that returns to
//	            its original value has, by conservation, had exactly offsetting
//	            work applied to it, and re-running the arithmetic on it is
//	            correct by construction.
//
// The one behaviour worth stating plainly: a thread that wins the interval
// claim and is then descheduled before adding its tokens leaves those tokens
// temporarily unminted. They are not lost - they land when it resumes - and no
// thread can block waiting for it, so the algorithm remains lock-free in the
// formal sense (some thread always makes progress). The failure mode under
// extreme preemption is a rate limiter that is momentarily slightly stricter
// than configured, which is the correct direction to err in a system whose
// whole purpose is protecting a shared resource.
// ---------------------------------------------------------------------------

// refill mints the tokens that have accrued since the slot was last refilled.
//
// Its cost in the common case is one atomic load and one failed comparison: if
// no time has passed since the last refill - which is the norm when a single
// tenant is being hammered by many workers at once - it returns immediately
// without touching the token word at all.
func refill(s *slot, capScaled uint64, rateFP uint32, now uint64) {
	if rateFP == 0 {
		// A static quota. Still clamp, in case capacity was lowered live.
		clampToCapacity(s, capScaled)
		return
	}

	last := atomic.LoadUint64(&s.lastRefill)
	if now <= last {
		// Either no measurable time has passed, or another thread has already
		// claimed an interval extending past our own clock reading. Both mean
		// there is nothing for this thread to mint.
		return
	}
	add := scaledRefill(now-last, rateFP)
	if add == 0 {
		// The interval is too short to have earned even one unit in the last
		// place. Do not claim it: leaving lastRefill alone lets the interval
		// roll into the next accrual instead of being consumed for nothing,
		// and it spares a contended CAS on a line other workers are reading.
		return
	}

	if !atomic.CompareAndSwapUint64(&s.lastRefill, last, now) {
		// Lost the claim for [last, now). The winner is minting this interval;
		// double-minting it here is precisely the bug this CAS prevents.
		return
	}

	for {
		cur := atomic.LoadUint64(&s.tokens)
		if cur >= capScaled {
			// Full. Only write if the balance actually exceeds capacity, which
			// happens when SetLimit shrinks a bucket that was already full.
			if cur == capScaled {
				return
			}
			if atomic.CompareAndSwapUint64(&s.tokens, cur, capScaled) {
				return
			}
			continue
		}
		next := cur + add
		if next < cur || next > capScaled { // wrapped, or overfilled
			next = capScaled
		}
		if atomic.CompareAndSwapUint64(&s.tokens, cur, next) {
			return
		}
		// Contended: a consumer moved the balance underneath us. Re-read and
		// re-apply the same accrual - it has not been minted yet, so retrying
		// neither loses nor duplicates it.
	}
}

// clampToCapacity trims a balance left above capacity by a live SetLimit.
func clampToCapacity(s *slot, capScaled uint64) {
	for {
		cur := atomic.LoadUint64(&s.tokens)
		if cur <= capScaled {
			return
		}
		if atomic.CompareAndSwapUint64(&s.tokens, cur, capScaled) {
			return
		}
	}
}

// Allow is the engine's reason for existing: it decides, in nanoseconds and
// without ever taking a lock, whether a tenant may spend tokensRequested tokens
// right now.
//
// tenantSlotOffset is the handle returned by Register. It is passed rather than
// a tenant id so that the fast path performs no hashing, no directory probe and
// no pointer chase - just one bounds check and one cache line.
//
// It returns true if the tokens were debited, false if the bucket could not
// cover the request, if the offset does not address a live slot, or if the
// request exceeds the bucket's total capacity and so could never succeed.
// Denial never blocks and never sleeps; deciding what to do with a rejection -
// queue, shed, or return 429 - belongs to the caller.
func (e *Engine) Allow(tenantSlotOffset uintptr, tokensRequested uint64) bool {
	return e.AllowAt(tenantSlotOffset, tokensRequested, e.now())
}

// AllowAt is Allow against a caller-supplied timestamp, in nanoseconds on the
// same timeline as Now(). Event-loop servers that already stamp each batch of
// packets with a timestamp should use it: it removes the clock read from the
// per-decision cost entirely. Tests use it to drive time deterministically.
//
// Timestamps that move backwards are ignored rather than trusted, so feeding
// slightly stale values from parallel workers is safe - it defers refills, it
// does not corrupt the bucket.
func (e *Engine) AllowAt(tenantSlotOffset uintptr, tokensRequested uint64, nowNanos uint64) bool {
	s := e.slotAt(tenantSlotOffset)
	if s == nil {
		return false
	}
	if atomic.LoadUint32(&s.state) != slotLive {
		return false
	}
	return e.consume(s, tokensRequested, nowNanos)
}

// AllowOwned is Allow with an ownership assertion: the decision is applied only
// if the slot still belongs to tenantID.
//
// It exists for the one hazard inherent to a raw-offset fast path. If a tenant
// is unregistered and its slot recycled, a cached offset elsewhere in the
// process now points at a different tenant's bucket, and spending against it
// would debit an innocent third party. One extra load from the same cache line
// closes that window. Systems with tenant churn should prefer this over Allow.
func (e *Engine) AllowOwned(tenantSlotOffset uintptr, tenantID uint64, tokensRequested uint64) bool {
	s := e.slotAt(tenantSlotOffset)
	if s == nil {
		return false
	}
	if atomic.LoadUint32(&s.state) != slotLive {
		return false
	}
	if atomic.LoadUint64(&s.tenantID) != tenantID {
		return false
	}
	return e.consume(s, tokensRequested, e.now())
}

// AllowTenant resolves the tenant through the directory and then decides. It is
// the convenient form, not the fast one: it costs a hash and a probe on top of
// everything Allow does. Unregistered tenants are denied.
func (e *Engine) AllowTenant(tenantID uint64, tokensRequested uint64) bool {
	off, ok := e.Lookup(tenantID)
	if !ok {
		return false
	}
	return e.AllowAt(off, tokensRequested, e.now())
}

// consume is the shared decision core, operating on an already-validated slot.
func (e *Engine) consume(s *slot, n uint64, now uint64) bool {
	if n == 0 {
		return true
	}

	capacity := uint64(atomic.LoadUint32(&s.capacity))
	if n > capacity || n > maxRequest {
		// Unsatisfiable at any point in the future. Fail it now instead of
		// spinning a CAS loop that provably cannot succeed.
		if e.counters {
			atomic.AddUint64(&s.rejected, 1)
		}
		return false
	}

	capScaled := capacity << TokenShift
	refill(s, capScaled, atomic.LoadUint32(&s.rateFP), now)

	want := n << TokenShift
	for {
		cur := atomic.LoadUint64(&s.tokens)
		if cur < want {
			if e.counters {
				atomic.AddUint64(&s.rejected, 1)
			}
			return false
		}
		if atomic.CompareAndSwapUint64(&s.tokens, cur, cur-want) {
			if e.counters {
				atomic.AddUint64(&s.granted, 1)
			}
			return true
		}
		// Another worker on this tenant won the line. Re-read and retry: the
		// loop is bounded in practice by the number of contending cores, and
		// every iteration means some other thread made progress.
	}
}

// ---------------------------------------------------------------------------
// Slot inspection and live reconfiguration
// ---------------------------------------------------------------------------

// Stats is a point-in-time view of one bucket. It is sampled with individual
// atomic loads rather than a lock, so the fields are each internally consistent
// but not a single instant's snapshot of the whole slot. That is the right
// trade for telemetry: reporting must never perturb the data plane.
type Stats struct {
	TenantID               uint64
	Capacity               uint32
	Tokens                 float64 // whole tokens, including the fraction
	RefillRatePerNano      uint32  // raw Q0.32 storage value
	EffectiveRatePerSecond float64 // what the quantised rate actually delivers
	LastRefillNanos        uint64
	Granted                uint64 // zero unless EnableCounters was set
	Rejected               uint64 // zero unless EnableCounters was set
	Live                   bool
}

// Snapshot reports the current state of a slot, refilling it first so the token
// balance reflects the present moment rather than the last request.
func (e *Engine) Snapshot(tenantSlotOffset uintptr) (Stats, error) {
	s := e.slotAt(tenantSlotOffset)
	if s == nil {
		return Stats{}, ErrBadOffset
	}
	capacity := atomic.LoadUint32(&s.capacity)
	rate := atomic.LoadUint32(&s.rateFP)
	if atomic.LoadUint32(&s.state) == slotLive {
		refill(s, uint64(capacity)<<TokenShift, rate, e.now())
	}
	return Stats{
		TenantID:               atomic.LoadUint64(&s.tenantID),
		Capacity:               capacity,
		Tokens:                 float64(atomic.LoadUint64(&s.tokens)) / float64(TokenScale),
		RefillRatePerNano:      rate,
		EffectiveRatePerSecond: fpToRate(rate),
		LastRefillNanos:        atomic.LoadUint64(&s.lastRefill),
		Granted:                atomic.LoadUint64(&s.granted),
		Rejected:               atomic.LoadUint64(&s.rejected),
		Live:                   atomic.LoadUint32(&s.state) == slotLive,
	}, nil
}

// Tokens returns the whole-and-fractional token balance after refilling.
func (e *Engine) Tokens(tenantSlotOffset uintptr) (float64, error) {
	st, err := e.Snapshot(tenantSlotOffset)
	if err != nil {
		return 0, err
	}
	return st.Tokens, nil
}

// SetLimit reconfigures a live bucket without interrupting traffic through it.
//
// The order of operations is deliberate. Time is settled under the *old* rate
// first, so tokens already earned are minted at the rate that earned them; only
// then does the new rate take effect. Raising capacity leaves the balance
// alone - headroom accrues rather than being granted retroactively - and
// lowering it trims any excess on the next refill.
func (e *Engine) SetLimit(tenantSlotOffset uintptr, capacity uint32, ratePerSecond float64) error {
	if capacity == 0 {
		return ErrInvalidCapacity
	}
	fp, err := rateToFP(ratePerSecond)
	if err != nil {
		return err
	}
	s := e.slotAt(tenantSlotOffset)
	if s == nil {
		return ErrBadOffset
	}

	oldCap := uint64(atomic.LoadUint32(&s.capacity))
	refill(s, oldCap<<TokenShift, atomic.LoadUint32(&s.rateFP), e.now())

	atomic.StoreUint32(&s.rateFP, fp)
	atomic.StoreUint32(&s.capacity, capacity)
	clampToCapacity(s, uint64(capacity)<<TokenShift)
	return nil
}

// Reset refills a bucket to capacity and restarts its accrual clock. Intended
// for administrative use - clearing a limit after an incident - not for the
// request path.
func (e *Engine) Reset(tenantSlotOffset uintptr) error {
	s := e.slotAt(tenantSlotOffset)
	if s == nil {
		return ErrBadOffset
	}
	atomic.StoreUint64(&s.lastRefill, e.now())
	atomic.StoreUint64(&s.tokens, uint64(atomic.LoadUint32(&s.capacity))<<TokenShift)
	return nil
}
