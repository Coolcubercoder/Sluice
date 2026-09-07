package sluice

import "sync/atomic"

// ---------------------------------------------------------------------------
// Tenant directory: an off-heap, open-addressed, lock-free hash table.
//
// Every byte of it lives in the arena, so a directory holding ten million
// tenant identities contributes nothing to the GC's mark set. Entries are
// claimed with a single CAS on the key word; the value word is published only
// after the bucket slot behind it is fully initialised, which is what makes a
// concurrent Lookup either miss cleanly or observe a completely valid slot,
// never a half-built one.
// ---------------------------------------------------------------------------

// hashTenant is the SplitMix64 finaliser. Tenant identifiers in the wild are
// frequently sequential (customer 1000, 1001, 1002) or bit-structured (a shard
// id in the high bits), and linear probing is brutally sensitive to clustering
// in either case. This avalanches every input bit across the whole word for the
// cost of three multiplies and three shifts.
func hashTenant(id uint64) uint64 {
	id ^= id >> 30
	id *= 0xbf58476d1ce4e5b9
	id ^= id >> 27
	id *= 0x94d049bb133111eb
	id ^= id >> 31
	return id
}

// findEntry locates the directory bucket owning tenantID, or nil if the tenant
// has never been claimed. An empty key terminates the probe: because entries
// are never deleted (only detached by clearing their value), a run of occupied
// buckets is never broken by a hole, so the first empty bucket proves absence.
func (e *Engine) findEntry(tenantID uint64) *regEntry {
	idx := hashTenant(tenantID) & e.regMask
	for probe := uint64(0); probe < e.regCap; probe++ {
		ent := e.entryAt(idx)
		key := atomic.LoadUint64(&ent.key)
		if key == tenantID {
			return ent
		}
		if key == 0 {
			return nil
		}
		idx = (idx + 1) & e.regMask
	}
	return nil
}

// claimEntry returns the bucket owning tenantID, creating it if necessary.
func (e *Engine) claimEntry(tenantID uint64) (*regEntry, error) {
	idx := hashTenant(tenantID) & e.regMask
	for probe := uint64(0); probe < e.regCap; probe++ {
		ent := e.entryAt(idx)
		key := atomic.LoadUint64(&ent.key)
		if key == tenantID {
			return ent, nil
		}
		if key == 0 {
			if atomic.CompareAndSwapUint64(&ent.key, 0, tenantID) {
				atomic.AddUint64(&e.ctl.regUsed, 1)
				return ent, nil
			}
			// Lost the claim. Whoever won may have been claiming this very
			// tenant, in which case the bucket is still ours to use.
			if atomic.LoadUint64(&ent.key) == tenantID {
				return ent, nil
			}
		}
		idx = (idx + 1) & e.regMask
	}
	return nil, ErrRegistryFull
}

// ---------------------------------------------------------------------------
// Slot allocation: bump cursor with an ABA-tagged intrusive free list.
// ---------------------------------------------------------------------------

// allocSlot returns the index of an unused slot, preferring recycled ones so
// that a long-running process with tenant churn keeps reusing the same warm,
// already-faulted pages instead of walking further into the arena.
func (e *Engine) allocSlot() (uint32, bool) {
	if idx, ok := e.popFree(); ok {
		return idx, true
	}
	for {
		cur := atomic.LoadUint64(&e.ctl.nextSlot)
		if cur >= uint64(e.slotCount) {
			return 0, false
		}
		if atomic.CompareAndSwapUint64(&e.ctl.nextSlot, cur, cur+1) {
			return uint32(cur), true
		}
	}
}

// pushFree returns a slot to the free list.
//
// The head word packs a 32-bit generation tag above a 32-bit index-plus-one, so
// the classic Treiber-stack ABA hazard - pop reads next, is descheduled, the
// node is popped and pushed back by other threads, and the stale CAS then
// succeeds against a head that means something entirely different - cannot
// occur: any intervening push or pop advances the tag and invalidates the CAS.
func (e *Engine) pushFree(idx uint32) {
	s := e.slotByIndex(idx)
	for {
		head := atomic.LoadUint64(&e.ctl.freeHead)
		atomic.StoreUint64(&s.link, head&0xFFFFFFFF)
		next := ((head>>32 + 1) << 32) | uint64(idx+1)
		if atomic.CompareAndSwapUint64(&e.ctl.freeHead, head, next) {
			return
		}
	}
}

// popFree takes a slot off the free list, or reports that it is empty.
func (e *Engine) popFree() (uint32, bool) {
	for {
		head := atomic.LoadUint64(&e.ctl.freeHead)
		enc := head & 0xFFFFFFFF
		if enc == 0 {
			return 0, false
		}
		idx := uint32(enc - 1)
		// Reading the link of a node another thread may already have popped is
		// safe here in a way it is not in a heap-based Treiber stack: arena
		// slots are never unmapped while the engine lives, so the load can
		// only be stale, never a use-after-free. A stale value simply loses
		// the CAS below, because the tag will have moved.
		link := atomic.LoadUint64(&e.slotByIndex(idx).link)
		next := ((head>>32 + 1) << 32) | (link & 0xFFFFFFFF)
		if atomic.CompareAndSwapUint64(&e.ctl.freeHead, head, next) {
			return idx, true
		}
	}
}

// ---------------------------------------------------------------------------
// Public registration API
// ---------------------------------------------------------------------------

// Register binds a tenant to a bucket slot and returns the slot's byte offset
// within the arena. That offset is the handle the fast path takes: callers are
// expected to cache it next to their connection or session state so that a
// rate-limit decision never has to hash anything at all.
//
// capacity is the burst ceiling in whole tokens. ratePerSecond is the sustained
// refill rate; zero makes the bucket a one-shot quota that never refills. The
// rate is quantised into a 32-bit per-nanosecond fixed-point field - see
// Snapshot's EffectiveRatePerSecond for what a slot will genuinely deliver.
//
// If the tenant is already registered, the existing offset is returned together
// with ErrAlreadyRegistered, so a benign registration race needs no retry.
func (e *Engine) Register(tenantID uint64, capacity uint32, ratePerSecond float64) (uintptr, error) {
	fp, err := rateToFP(ratePerSecond)
	if err != nil {
		return 0, err
	}
	return e.RegisterRaw(tenantID, capacity, fp)
}

// RegisterRaw is Register with the refill rate given directly in the slot's
// storage format: Q0.32 tokens per nanosecond, i.e. tokensPerSecond * 2^32/1e9.
// Use it when the caller wants exact, reproducible control over quantisation
// rather than a float conversion.
func (e *Engine) RegisterRaw(tenantID uint64, capacity uint32, refillRatePerNano uint32) (uintptr, error) {
	if atomic.LoadUint32(&e.closed) != 0 {
		return 0, ErrClosed
	}
	if tenantID == 0 {
		return 0, ErrInvalidTenant
	}
	if capacity == 0 {
		return 0, ErrInvalidCapacity
	}

	ent, err := e.claimEntry(tenantID)
	if err != nil {
		return 0, err
	}

	// Fast rejection: the tenant already has a published slot.
	if cur := atomic.LoadUint64(&ent.val); cur != 0 {
		return e.offsetOfIndex(uint32(cur - 1)), ErrAlreadyRegistered
	}

	idx, ok := e.allocSlot()
	if !ok {
		return 0, ErrArenaExhausted
	}
	s := e.slotByIndex(idx)

	// Initialise the slot completely before it becomes reachable. A new bucket
	// starts full: a tenant that has never sent a request has, by definition,
	// consumed nothing, and starting empty would penalise it for arriving.
	now := e.now()
	atomic.StoreUint64(&s.tokens, uint64(capacity)<<TokenShift)
	atomic.StoreUint64(&s.lastRefill, now)
	atomic.StoreUint32(&s.capacity, capacity)
	atomic.StoreUint32(&s.rateFP, refillRatePerNano)
	atomic.StoreUint64(&s.granted, 0)
	atomic.StoreUint64(&s.rejected, 0)
	atomic.StoreUint64(&s.tenantID, tenantID)
	atomic.StoreUint32(&s.state, slotLive)

	// Publish. The CAS is the release point: every store above is ordered
	// before it, so any goroutine that observes a non-zero value word also
	// observes a fully constructed slot.
	if !atomic.CompareAndSwapUint64(&ent.val, 0, uint64(idx)+1) {
		// A concurrent Register for the same tenant beat us. Roll ours back and
		// hand the caller the winner's slot.
		atomic.StoreUint32(&s.state, slotFree)
		atomic.StoreUint64(&s.tenantID, 0)
		e.pushFree(idx)
		return e.offsetOfIndex(uint32(atomic.LoadUint64(&ent.val) - 1)), ErrAlreadyRegistered
	}

	atomic.AddUint64(&e.ctl.liveTenants, 1)
	return e.offsetOfIndex(idx), nil
}

// Unregister detaches a tenant and recycles its slot.
//
// The directory key itself is retained forever - re-registering the same tenant
// reuses the same bucket - because reclaiming a key would require breaking a
// probe chain, and doing that safely under concurrent lookups is exactly the
// kind of quiescence problem a lock-free data plane exists to avoid.
//
// Any slot offset the caller still holds for this tenant is stale the moment
// this returns. The slot is immediately eligible for reuse by a different
// tenant, so callers that cannot guarantee they have dropped every cached
// offset should use AllowOwned, which re-checks slot ownership.
func (e *Engine) Unregister(tenantID uint64) error {
	if tenantID == 0 {
		return ErrInvalidTenant
	}
	ent := e.findEntry(tenantID)
	if ent == nil {
		return ErrUnknownTenant
	}
	for {
		cur := atomic.LoadUint64(&ent.val)
		if cur == 0 {
			return ErrUnknownTenant
		}
		if atomic.CompareAndSwapUint64(&ent.val, cur, 0) {
			idx := uint32(cur - 1)
			s := e.slotByIndex(idx)
			atomic.StoreUint32(&s.state, slotFree)
			atomic.StoreUint64(&s.tenantID, 0)
			atomic.AddUint64(&e.ctl.liveTenants, ^uint64(0)) // -1
			e.pushFree(idx)
			return nil
		}
	}
}

// Lookup resolves a tenant id to its slot offset. It is safe on the fast path,
// but it is a hash probe: prefer to resolve once at connection setup and cache
// the offset.
func (e *Engine) Lookup(tenantID uint64) (uintptr, bool) {
	if tenantID == 0 {
		return 0, false
	}
	ent := e.findEntry(tenantID)
	if ent == nil {
		return 0, false
	}
	val := atomic.LoadUint64(&ent.val)
	if val == 0 {
		return 0, false
	}
	return e.offsetOfIndex(uint32(val - 1)), true
}
