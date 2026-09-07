package sluice

import "unsafe"

// ---------------------------------------------------------------------------
// Fixed-width, layout-stable memory geometry.
//
// Every structure Sluice writes into the off-heap arena is described here as a
// Go mirror type whose size and field offsets are asserted at compile time.
// The mirror types exist purely so that the compiler can validate the geometry;
// the arena itself is raw mmap'd bytes and the Go GC never sees any of it.
// ---------------------------------------------------------------------------

const (
	// CacheLineSize is the destructive interference size on every CPU Sluice
	// targets (x86-64, arm64 with 64B lines). Slots are sized *and* aligned to
	// this value so two network workers hammering two different tenants can
	// never contend on the same physical cache line (false sharing).
	CacheLineSize = 64

	// SlotSize is the fixed width of a tenant bucket slot. It is deliberately
	// equal to CacheLineSize: one tenant, one line, one owner of the dirty
	// line at any instant.
	SlotSize = 64

	// TokenShift is the number of fractional bits used by the fixed-point
	// token representation. Tokens are tracked as Q32.32 unsigned values:
	// the high 32 bits are whole tokens, the low 32 bits are the fraction.
	//
	// Fixed point (rather than float or integer-nanosecond) accounting is what
	// makes the refill arithmetic exact under a lock-free CAS loop: two racing
	// refills over disjoint time intervals sum to precisely the same value as
	// one refill over the union of those intervals, with no drift and no
	// truncation-induced starvation at high call rates.
	TokenShift = 32

	// TokenScale is one whole token in Q32.32 fixed point.
	TokenScale = uint64(1) << TokenShift

	// nanosPerSecond is used to convert human-facing rates into the
	// per-nanosecond fixed-point rate stored in a slot.
	nanosPerSecond = 1e9
)

// Slot lifecycle states. Stored as a uint32 inside the slot itself.
const (
	slotFree uint32 = 0 // on the free list (or never allocated)
	slotLive uint32 = 1 // published, serving Allow() traffic
)

// slot is the Go mirror of one tenant bucket. It is never allocated on the Go
// heap: instances only ever exist as *slot views onto mmap'd arena memory.
//
// Field placement is load-bearing, not incidental:
//
//	offset  0 lastRefill  uint64 - 8B aligned, CAS target (refill interval claim)
//	offset  8 tokens      uint64 - 8B aligned, CAS target (token consumption)
//	offset 16 capacity    uint32 - hot, read-only on the fast path
//	offset 20 rateFP      uint32 - hot, read-only on the fast path
//	offset 24 granted     uint64 - 8B aligned, optional atomic counter
//	offset 32 rejected    uint64 - 8B aligned, optional atomic counter
//	offset 40 state       uint32 - lifecycle
//	offset 44 _pad0       uint32 - explicit padding, keeps tenantID 8B aligned
//	offset 48 tenantID    uint64 - 8B aligned, owner identity
//	offset 56 link        uint64 - 8B aligned, intrusive free-list next pointer
//	                             - also the trailing pad to a full 64B line
//
// The two 32-bit fields are packed into a single 8-byte lane so that *every*
// 64-bit field lands on a natural 8-byte boundary. On arm64 an unaligned LDXR/
// STXR pair faults outright; on x86-64 a split-line LOCK CMPXCHG is silently
// but catastrophically slow. The compile-time assertions below make either
// mistake a build failure rather than a 3am page.
type slot struct {
	lastRefill uint64
	tokens     uint64
	capacity   uint32
	rateFP     uint32
	granted    uint64
	rejected   uint64
	state      uint32
	_pad0      uint32
	tenantID   uint64
	link       uint64
}

// regEntry is one bucket of the off-heap open-addressed tenant directory.
//
//	offset 0 key uint64 - tenant ID, 0 means "never claimed"
//	offset 8 val uint64 - slotIndex+1, 0 means "claimed but not published"
//
// Publishing val with a release-ordered atomic store *after* the slot body is
// fully initialised is what makes lookup safe without any lock.
type regEntry struct {
	key uint64
	val uint64
}

// control is the arena header: three independently mutated hot words, each
// isolated on its own cache line so that the slot allocator, the free list and
// the coarse clock never invalidate one another.
type control struct {
	nextSlot uint64 // bump allocator cursor
	_pad0    [CacheLineSize - 8]byte

	freeHead uint64 // ABA-tagged Treiber stack head: (tag<<32)|(index+1)
	_pad1    [CacheLineSize - 8]byte

	coarseNanos uint64 // cached clock, only written when the coarse clock runs
	_pad2       [CacheLineSize - 8]byte

	liveTenants uint64 // currently published slots
	regUsed     uint64 // directory keys ever claimed (monotonic)
	_pad3       [CacheLineSize - 16]byte
}

const (
	controlSize  = unsafe.Sizeof(control{})
	regEntrySize = unsafe.Sizeof(regEntry{})
)

// --- compile-time geometry assertions --------------------------------------
//
// Each pair of constants below is a two-sided equality proof: converting a
// negative untyped constant to uintptr is a compile error, so if either side of
// a size/offset invariant drifts, the package refuses to build.

var geometryProbe slot

const (
	offLastRefill = unsafe.Offsetof(geometryProbe.lastRefill)
	offTokens     = unsafe.Offsetof(geometryProbe.tokens)
	offCapacity   = unsafe.Offsetof(geometryProbe.capacity)
	offRateFP     = unsafe.Offsetof(geometryProbe.rateFP)
	offGranted    = unsafe.Offsetof(geometryProbe.granted)
	offRejected   = unsafe.Offsetof(geometryProbe.rejected)
	offState      = unsafe.Offsetof(geometryProbe.state)
	offTenantID   = unsafe.Offsetof(geometryProbe.tenantID)
	offLink       = unsafe.Offsetof(geometryProbe.link)
)

const (
	// The slot occupies exactly one cache line - no more, no less.
	_ = uintptr(unsafe.Sizeof(slot{}) - SlotSize)
	_ = uintptr(SlotSize - unsafe.Sizeof(slot{}))
	_ = uintptr(SlotSize - CacheLineSize)
	_ = uintptr(CacheLineSize - SlotSize)

	// Declared field offsets match the compiler's actual layout.
	_ = uintptr(offLastRefill - 0)
	_ = uintptr(0 - offLastRefill)
	_ = uintptr(offTokens - 8)
	_ = uintptr(8 - offTokens)
	_ = uintptr(offCapacity - 16)
	_ = uintptr(16 - offCapacity)
	_ = uintptr(offRateFP - 20)
	_ = uintptr(20 - offRateFP)
	_ = uintptr(offGranted - 24)
	_ = uintptr(24 - offGranted)
	_ = uintptr(offRejected - 32)
	_ = uintptr(32 - offRejected)
	_ = uintptr(offState - 40)
	_ = uintptr(40 - offState)
	_ = uintptr(offTenantID - 48)
	_ = uintptr(48 - offTenantID)
	_ = uintptr(offLink - 56)
	_ = uintptr(56 - offLink)

	// Every 64-bit atomic target sits on an 8-byte boundary.
	_ = uintptr(0 - offLastRefill%8)
	_ = uintptr(0 - offTokens%8)
	_ = uintptr(0 - offGranted%8)
	_ = uintptr(0 - offRejected%8)
	_ = uintptr(0 - offTenantID%8)
	_ = uintptr(0 - offLink%8)

	// Directory entries tile a cache line exactly (4 per line), and the
	// control header is a whole number of cache lines.
	_ = uintptr(regEntrySize - 16)
	_ = uintptr(16 - regEntrySize)
	_ = uintptr(0 - CacheLineSize%regEntrySize)
	_ = uintptr(0 - controlSize%CacheLineSize)
	_ = uintptr(controlSize - 4*CacheLineSize)
	_ = uintptr(4*CacheLineSize - controlSize)
)
