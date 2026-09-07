// Package sluice implements a multi-tenant, lock-free token bucket rate
// limiting engine whose entire tracking matrix lives outside the Go heap.
//
// # Why this exists
//
// The noisy-neighbour problem in a multi-tenant data plane is not hard to
// describe: one tenant's traffic burst must not degrade anybody else's
// latency. It is hard to *solve in Go* because the obvious implementation - a
// map[string]*bucket behind a mutex, or even a sharded map of pointer-bearing
// structs - fails in three separate ways at scale:
//
//  1. The map is a GC root holding millions of pointers. Every mark phase walks
//     all of them. At ten million tenants the mark cost alone puts a periodic
//     latency cliff into the request path that no amount of tuning removes.
//  2. Tenant churn allocates. Allocation is what triggers collection, so a
//     surge of new tenants provokes exactly the GC pause the data plane can
//     least afford, at exactly the moment it is busiest.
//  3. The mutex serialises unrelated tenants. Tenant A's bucket update should
//     have nothing to do with tenant B's, yet they queue behind one lock.
//
// Sluice removes all three by construction. State lives in a single anonymous
// mmap'd, mlock'd arena that the collector never scans and the pacer never
// accounts for; tenant registration is a CAS into a preallocated off-heap
// directory rather than an allocation; and a decision is a short atomic
// compare-and-swap loop against one 64-byte slot that no other tenant shares.
//
// # Cost model
//
// After New returns, the steady-state hot path performs zero allocations, takes
// zero locks, makes zero syscalls, and touches exactly one cache line per
// decision. Its cost is one clock read (or one L1 load with a coarse clock),
// two to four atomic operations, and no memory barriers beyond those the
// atomics themselves imply.
//
// # Concurrency contract
//
//   - Allow, AllowAt, Lookup, Snapshot, Tokens and SetLimit are safe from any
//     number of goroutines simultaneously.
//   - Register and Unregister are safe to call concurrently with each other and
//     with the fast path.
//   - Close is not safe to call while any other operation is in flight; the
//     arena is unmapped and a concurrent Allow would fault on freed pages.
//     Shut down traffic first, then close.
package sluice

import (
	"sync/atomic"
	"time"
	"unsafe"
)

// Config describes an engine's fixed, startup-time geometry. Nothing here can
// change once New returns: a rate limiter whose memory footprint is allowed to
// grow under load is a rate limiter that fails when it is needed most.
type Config struct {
	// MaxTenants is the number of bucket slots carved out of the arena, and
	// therefore the hard ceiling on concurrently registered tenants.
	//
	// Each tenant costs a 64-byte slot plus directory space. The directory is
	// a power-of-two table sized for a 0.5 load factor, so the per-tenant
	// total is 96 bytes when MaxTenants is itself a power of two and up to 128
	// bytes when it sits just above one: 8388608 tenants is a 768MiB arena,
	// while 10000000 costs 1.10GiB. Round to a power of two if the footprint
	// matters.
	//
	// Size this for the peak identity population, not the average: slots are
	// recycled on Unregister, but never created.
	MaxTenants uint32

	// PinMemory wires the arena into physical memory with mlock, guaranteeing
	// no rate-limit decision can ever block on a page fault. Recommended for
	// production data planes. Requires sufficient RLIMIT_MEMLOCK.
	PinMemory bool

	// PinBestEffort downgrades an mlock failure from a fatal error to a
	// recorded warning (see Engine.PinError). The arena is prefaulted instead,
	// so it is resident but evictable. Useful in containers that ship with a
	// 64KiB memlock limit.
	PinBestEffort bool

	// ClockResolution, when non-zero, starts a single background goroutine
	// that caches the monotonic clock in the arena at this interval. Workers
	// then read the timestamp with an L1 load instead of a clock call, which
	// on a hot path measured in tens of nanoseconds is a material fraction of
	// the total. The cost is that refills become granular at this resolution -
	// no tokens are lost or duplicated, they simply arrive in steps. 100us is
	// a sane starting point; leave zero to read the clock per decision.
	ClockResolution time.Duration

	// EnableCounters maintains per-tenant granted/rejected counts. They are
	// two extra atomic increments on the tenant's own already-dirty cache
	// line - cheap, but not free, so the fast path is the default.
	EnableCounters bool
}

// Engine is the rate limiting core. Create one per process and share it.
type Engine struct {
	arena *arena
	base  unsafe.Pointer // arena origin; all public offsets are relative to it
	ctl   *control       // arena header, first cache lines of the mapping
	reg   unsafe.Pointer // tenant directory origin

	regCap  uint64 // directory capacity, a power of two
	regMask uint64 // regCap-1

	slotsOff  uintptr // byte offset of the slot region from base
	slotSpan  uintptr // slotCount * SlotSize
	slotCount uint32

	counters bool
	coarse   bool

	stop      chan struct{}
	clockDone chan struct{}
	closed    uint32
}

// New maps, optionally pins, and initialises an engine.
//
// The mapping is anonymous and therefore zero-filled by the kernel on first
// touch, which is exactly the empty state of every structure Sluice stores. No
// initialisation loop runs over the arena, so New is O(1) with respect to
// MaxTenants apart from the pinning walk.
func New(cfg Config) (*Engine, error) {
	if cfg.MaxTenants == 0 {
		return nil, ErrInvalidConfig
	}

	// Load factor 0.5. Open addressing with linear probing degrades sharply
	// past ~0.7, and the directory is the one structure whose lookup cost is
	// paid by tenants that did nothing wrong, so it is kept generously sparse.
	regCap := nextPow2(uint64(cfg.MaxTenants) * 2)
	if regCap < 16 {
		regCap = 16
	}

	regBytes := regCap * uint64(regEntrySize)
	slotBytes := uint64(cfg.MaxTenants) * SlotSize
	total := uint64(controlSize) + regBytes + slotBytes

	// Refuse geometries that cannot be addressed on this platform rather than
	// silently truncating a size argument.
	if total > uint64(^uintptr(0)) || total > uint64(int(^uint(0)>>1)) {
		return nil, ErrInvalidConfig
	}

	a, err := mapArena(uintptr(total), cfg.PinMemory, cfg.PinBestEffort)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		arena:     a,
		base:      a.base,
		ctl:       (*control)(a.base),
		reg:       unsafe.Add(a.base, controlSize),
		regCap:    regCap,
		regMask:   regCap - 1,
		slotsOff:  uintptr(uint64(controlSize) + regBytes),
		slotSpan:  uintptr(slotBytes),
		slotCount: cfg.MaxTenants,
		counters:  cfg.EnableCounters,
	}

	// The slot region must itself begin on a cache line so that slot i is
	// aligned for every i. controlSize is 4 lines and the directory is a
	// power-of-two count of 16-byte entries, so this holds; assert it anyway.
	if e.slotsOff&(CacheLineSize-1) != 0 {
		_ = a.release()
		return nil, ErrMisalignedArena
	}

	if cfg.ClockResolution > 0 {
		e.startCoarseClock(cfg.ClockResolution)
	}
	return e, nil
}

// Close stops the background clock, if any, and returns the arena to the
// kernel. It must not race with in-flight Allow calls: unmapping memory another
// goroutine is about to CAS against is a segmentation fault, not an error
// return. Drain traffic first. Close is idempotent.
func (e *Engine) Close() error {
	if !atomic.CompareAndSwapUint32(&e.closed, 0, 1) {
		return nil
	}
	if e.stop != nil {
		close(e.stop)
		<-e.clockDone
	}
	e.coarse = false
	return e.arena.release()
}

// ---------------------------------------------------------------------------
// Arena addressing
// ---------------------------------------------------------------------------

// slotAt validates a caller-supplied offset and converts it into a slot view.
//
// The offset is a byte offset from the arena origin, which is what Register
// hands back. Validation is three predictable branches - a range check and an
// alignment check - and it is not optional: an unvalidated offset would let a
// caller bug turn into a wild atomic write anywhere in the process image.
func (e *Engine) slotAt(off uintptr) *slot {
	rel := off - e.slotsOff
	if off < e.slotsOff || rel >= e.slotSpan || rel&(SlotSize-1) != 0 {
		return nil
	}
	return (*slot)(unsafe.Add(e.base, off))
}

// slotByIndex is the internal, already-trusted form of slotAt.
func (e *Engine) slotByIndex(i uint32) *slot {
	return (*slot)(unsafe.Add(e.base, e.slotsOff+uintptr(i)*SlotSize))
}

// offsetOfIndex converts a slot index into the public byte offset.
func (e *Engine) offsetOfIndex(i uint32) uintptr {
	return e.slotsOff + uintptr(i)*SlotSize
}

// entryAt returns the directory bucket at the given probe position.
func (e *Engine) entryAt(i uint64) *regEntry {
	return (*regEntry)(unsafe.Add(e.reg, uintptr(i)*regEntrySize))
}

// ---------------------------------------------------------------------------
// Introspection
// ---------------------------------------------------------------------------

// Pinned reports whether the arena is wired into physical memory.
func (e *Engine) Pinned() bool { return e.arena != nil && e.arena.pinned }

// PinError returns the mlock failure that was tolerated under PinBestEffort,
// or nil if the arena is pinned or pinning was never requested.
func (e *Engine) PinError() error {
	if e.arena == nil {
		return nil
	}
	return e.arena.pinErr
}

// ArenaBytes reports the size of the mapping. This is the engine's entire
// steady-state memory footprint, and none of it is visible to the collector.
func (e *Engine) ArenaBytes() uintptr {
	if e.arena == nil {
		return 0
	}
	return e.arena.size
}

// Capacity reports the total number of bucket slots.
func (e *Engine) Capacity() uint32 { return e.slotCount }

// LiveTenants reports how many slots are currently published.
func (e *Engine) LiveTenants() uint64 { return atomic.LoadUint64(&e.ctl.liveTenants) }

// DirectoryLoad reports the fraction of directory keys that have been claimed.
// Keys are permanent per distinct tenant id, so this rises monotonically with
// the number of unique identities the engine has ever seen. Past ~0.7, probe
// chains lengthen and Register should be considered at risk of ErrRegistryFull.
func (e *Engine) DirectoryLoad() float64 {
	return float64(atomic.LoadUint64(&e.ctl.regUsed)) / float64(e.regCap)
}

// nextPow2 rounds up to a power of two, which is what turns the directory's
// modulo into a single AND on the lookup path.
func nextPow2(v uint64) uint64 {
	if v == 0 {
		return 1
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	return v + 1
}
