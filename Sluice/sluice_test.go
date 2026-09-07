package sluice

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func newEngine(t testing.TB, n uint32) *Engine {
	t.Helper()
	e, err := New(Config{MaxTenants: n, PinMemory: true, PinBestEffort: true, EnableCounters: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// --- geometry --------------------------------------------------------------

func TestSlotGeometry(t *testing.T) {
	if unsafe.Sizeof(slot{}) != SlotSize {
		t.Fatalf("slot size = %d, want %d", unsafe.Sizeof(slot{}), SlotSize)
	}
	if SlotSize != CacheLineSize {
		t.Fatalf("slot must occupy exactly one cache line")
	}
	for name, off := range map[string]uintptr{
		"lastRefill": offLastRefill,
		"tokens":     offTokens,
		"granted":    offGranted,
		"rejected":   offRejected,
		"tenantID":   offTenantID,
		"link":       offLink,
	} {
		if off%8 != 0 {
			t.Errorf("64-bit field %s at offset %d is not 8-byte aligned", name, off)
		}
	}
}

func TestArenaAlignment(t *testing.T) {
	e := newEngine(t, 4096)
	if uintptr(e.base)%CacheLineSize != 0 {
		t.Fatalf("arena base %#x is not cache-line aligned", uintptr(e.base))
	}
	if e.slotsOff%CacheLineSize != 0 {
		t.Fatalf("slot region offset %d is not cache-line aligned", e.slotsOff)
	}
	for i := uint32(0); i < 64; i++ {
		s := e.slotByIndex(i)
		if uintptr(unsafe.Pointer(s))%CacheLineSize != 0 {
			t.Fatalf("slot %d is not cache-line aligned", i)
		}
		// Distinct slots must never share a line.
		if i > 0 {
			prev := uintptr(unsafe.Pointer(e.slotByIndex(i - 1)))
			if uintptr(unsafe.Pointer(s))-prev != SlotSize {
				t.Fatalf("slots %d and %d are not exactly one line apart", i-1, i)
			}
		}
	}
	if e.ArenaBytes()%uintptr(pageSize) != 0 {
		t.Fatalf("arena size %d is not a page multiple", e.ArenaBytes())
	}
	t.Logf("arena=%d bytes pinned=%v pinErr=%v", e.ArenaBytes(), e.Pinned(), e.PinError())
}

// --- fixed point arithmetic ------------------------------------------------

func TestScaledRefillSaturates(t *testing.T) {
	cases := []struct {
		elapsed uint64
		rate    uint32
		want    uint64
	}{
		{0, 1000, 0},
		{1000, 1000, 1_000_000},
		{1 << 32, 1, 1 << 32},
		{0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF * 0xFFFFFFFF},
		{^uint64(0), 2, ^uint64(0)},                   // overflows 64 bits
		{1 << 40, 0xFFFFFFFF, ^uint64(0)},             // overflows 64 bits
		{1 << 31, 0xFFFFFFFF, (1 << 31) * 0xFFFFFFFF}, // exactly representable
	}
	for _, c := range cases {
		if got := scaledRefill(c.elapsed, c.rate); got != c.want {
			t.Errorf("scaledRefill(%d,%d) = %d, want %d", c.elapsed, c.rate, got, c.want)
		}
	}
}

func TestRateQuantisation(t *testing.T) {
	for _, perSec := range []float64{1, 100, 1000, 50_000, 1_000_000} {
		fp, err := rateToFP(perSec)
		if err != nil {
			t.Fatalf("rateToFP(%v): %v", perSec, err)
		}
		eff := fpToRate(fp)
		rel := (eff - perSec) / perSec
		if rel < 0 {
			rel = -rel
		}
		limit := 1.0 / (2 * float64(fp))
		if rel > limit+1e-12 {
			t.Errorf("rate %v: effective %v, relative error %v exceeds bound %v", perSec, eff, rel, limit)
		}
	}
	if _, err := rateToFP(MaxRatePerSecond * 2); err != ErrRateOutOfRange {
		t.Errorf("expected ErrRateOutOfRange, got %v", err)
	}
	if _, err := rateToFP(-1); err != ErrInvalidRate {
		t.Errorf("expected ErrInvalidRate, got %v", err)
	}
	// A rate too small to represent must clamp up, never silently become "no
	// refill at all".
	fp, err := rateToFP(MinRatePerSecond / 1000)
	if err != nil || fp == 0 {
		t.Errorf("tiny rate collapsed to zero refill: fp=%d err=%v", fp, err)
	}
}

// --- single-threaded semantics --------------------------------------------

func TestAllowDrainsExactly(t *testing.T) {
	e := newEngine(t, 16)
	off, err := e.Register(7, 10, 0) // static quota, no refill
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if !e.Allow(off, 1) {
			t.Fatalf("request %d denied while tokens remained", i)
		}
	}
	if e.Allow(off, 1) {
		t.Fatal("granted an 11th token from a 10-token bucket")
	}
	st, _ := e.Snapshot(off)
	if st.Granted != 10 || st.Rejected != 1 {
		t.Fatalf("counters = %d granted / %d rejected, want 10/1", st.Granted, st.Rejected)
	}
	if st.Tokens != 0 {
		t.Fatalf("tokens = %v, want 0", st.Tokens)
	}
}

func TestRequestLargerThanCapacityFailsFast(t *testing.T) {
	e := newEngine(t, 16)
	off, _ := e.Register(1, 10, 100)
	if e.Allow(off, 11) {
		t.Fatal("granted a request larger than the bucket")
	}
	if e.Allow(off, maxRequest+1) {
		t.Fatal("granted an unrepresentable request")
	}
	if !e.Allow(off, 10) {
		t.Fatal("full bucket refused a request exactly its size")
	}
	if !e.Allow(off, 0) {
		t.Fatal("zero-token request denied")
	}
}

func TestDeterministicRefill(t *testing.T) {
	e := newEngine(t, 16)
	// 4294967 Q0.32 units/ns == 999999.93 tokens/sec: over 1e6 ns exactly
	// 999.99993 tokens accrue, so 999 whole tokens must be spendable and the
	// 1000th must not.
	const fp = 4294967
	off, err := e.RegisterRaw(42, 5000, fp)
	if err != nil {
		t.Fatal(err)
	}

	base := Now() + 1_000_000
	// Drain the (initially full) bucket at t=base.
	if !e.AllowAt(off, 5000, base) {
		t.Fatal("could not drain a full bucket")
	}
	if e.AllowAt(off, 1, base) {
		t.Fatal("granted a token from a drained bucket at the same instant")
	}

	// Advance one millisecond.
	at := base + 1_000_000
	granted := 0
	for e.AllowAt(off, 1, at) {
		granted++
		if granted > 2000 {
			t.Fatal("refill produced unbounded tokens")
		}
	}
	if granted != 999 {
		t.Fatalf("1ms of refill produced %d tokens, want 999", granted)
	}

	// Time going backwards must be inert, not destructive.
	if e.AllowAt(off, 1, base) {
		t.Fatal("a backwards timestamp minted tokens")
	}
}

func TestRefillNeverExceedsCapacity(t *testing.T) {
	e := newEngine(t, 16)
	off, _ := e.Register(9, 100, 1_000_000)
	base := Now()
	e.AllowAt(off, 100, base)
	// Idle for a simulated century. The accrual product overflows 64 bits and
	// must saturate into the capacity clamp rather than wrap to a small value.
	far := base + 3_153_600_000_000_000_000
	if !e.AllowAt(off, 100, far) {
		t.Fatal("bucket did not refill to capacity after a long idle period")
	}
	if e.AllowAt(off, 1, far) {
		t.Fatalf("bucket held more than capacity after a long idle period")
	}
}

func TestSetLimitSettlesUnderOldRate(t *testing.T) {
	e := newEngine(t, 16)
	off, _ := e.Register(11, 1000, 0)
	if !e.Allow(off, 1000) {
		t.Fatal("drain failed")
	}
	if err := e.SetLimit(off, 10, 0); err != nil {
		t.Fatal(err)
	}
	st, _ := e.Snapshot(off)
	if st.Capacity != 10 {
		t.Fatalf("capacity = %d, want 10", st.Capacity)
	}
	// Shrinking capacity below a full balance must trim the excess.
	if err := e.Reset(off); err != nil {
		t.Fatal(err)
	}
	if err := e.SetLimit(off, 4, 0); err != nil {
		t.Fatal(err)
	}
	if tok, _ := e.Tokens(off); tok != 4 {
		t.Fatalf("tokens after shrink = %v, want 4", tok)
	}
}

// --- registry --------------------------------------------------------------

func TestRegisterLookupUnregister(t *testing.T) {
	e := newEngine(t, 256)
	off, err := e.Register(1234, 10, 5)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := e.Lookup(1234)
	if !ok || got != off {
		t.Fatalf("Lookup = %v,%v want %v,true", got, ok, off)
	}
	if _, err := e.Register(1234, 10, 5); err != ErrAlreadyRegistered {
		t.Fatalf("duplicate Register = %v, want ErrAlreadyRegistered", err)
	}
	if e.LiveTenants() != 1 {
		t.Fatalf("LiveTenants = %d, want 1", e.LiveTenants())
	}
	if err := e.Unregister(1234); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.Lookup(1234); ok {
		t.Fatal("Lookup found an unregistered tenant")
	}
	if e.LiveTenants() != 0 {
		t.Fatalf("LiveTenants = %d, want 0", e.LiveTenants())
	}
	if err := e.Unregister(1234); err != ErrUnknownTenant {
		t.Fatalf("double Unregister = %v, want ErrUnknownTenant", err)
	}
	// The recycled slot must be handed straight back out.
	off2, err := e.Register(1234, 10, 5)
	if err != nil || off2 != off {
		t.Fatalf("re-register = %v,%v; want the recycled slot %v", off2, err, off)
	}
	if _, err := e.Register(0, 10, 5); err != ErrInvalidTenant {
		t.Fatalf("tenant 0 = %v, want ErrInvalidTenant", err)
	}
	if _, err := e.Register(2, 0, 5); err != ErrInvalidCapacity {
		t.Fatalf("zero capacity = %v, want ErrInvalidCapacity", err)
	}
}

func TestArenaExhaustionIsCleanAndRecoverable(t *testing.T) {
	e := newEngine(t, 8)
	offs := make([]uintptr, 0, 8)
	for i := uint64(1); i <= 8; i++ {
		off, err := e.Register(i, 4, 0)
		if err != nil {
			t.Fatalf("Register(%d): %v", i, err)
		}
		offs = append(offs, off)
	}
	if _, err := e.Register(9, 4, 0); err != ErrArenaExhausted {
		t.Fatalf("over-registration = %v, want ErrArenaExhausted", err)
	}
	if err := e.Unregister(3); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Register(9, 4, 0); err != nil {
		t.Fatalf("Register after freeing a slot: %v", err)
	}
}

func TestStaleOffsetIsCaughtByAllowOwned(t *testing.T) {
	e := newEngine(t, 8)
	off, _ := e.Register(100, 5, 0)
	if err := e.Unregister(100); err != nil {
		t.Fatal(err)
	}
	if e.Allow(off, 1) {
		t.Fatal("a freed slot served a request")
	}
	off2, _ := e.Register(200, 5, 0)
	if off2 != off {
		t.Fatal("expected slot recycling for this test to be meaningful")
	}
	if e.AllowOwned(off, 100, 1) {
		t.Fatal("AllowOwned debited a recycled slot on behalf of the old tenant")
	}
	if !e.AllowOwned(off, 200, 1) {
		t.Fatal("AllowOwned refused the slot's real owner")
	}
}

func TestBadOffsetsAreRejected(t *testing.T) {
	e := newEngine(t, 8)
	off, _ := e.Register(1, 5, 0)
	for _, bad := range []uintptr{0, off + 1, off + 33, e.slotsOff + e.slotSpan, ^uintptr(0)} {
		if e.Allow(bad, 1) {
			t.Fatalf("offset %#x was accepted", bad)
		}
		if _, err := e.Snapshot(bad); err != ErrBadOffset {
			t.Fatalf("Snapshot(%#x) = %v, want ErrBadOffset", bad, err)
		}
	}
}

func TestDirectoryProbesSurviveCollisions(t *testing.T) {
	e := newEngine(t, 1024)
	const n = 512
	offs := make(map[uint64]uintptr, n)
	for i := uint64(1); i <= n; i++ {
		// Stride by a power of two to force clustering in a mask-indexed table.
		id := i * 4096
		off, err := e.Register(id, 8, 0)
		if err != nil {
			t.Fatalf("Register(%d): %v", id, err)
		}
		offs[id] = off
	}
	for id, want := range offs {
		got, ok := e.Lookup(id)
		if !ok || got != want {
			t.Fatalf("Lookup(%d) = %v,%v want %v,true", id, got, ok, want)
		}
		st, _ := e.Snapshot(got)
		if st.TenantID != id {
			t.Fatalf("slot for %d reports tenant %d", id, st.TenantID)
		}
	}
	t.Logf("directory load = %.3f", e.DirectoryLoad())
}

// --- concurrency -----------------------------------------------------------

// TestConcurrentConservation is the load-bearing correctness test: with refill
// disabled, a bucket of N tokens must grant exactly N requests no matter how
// many threads race for them. A lost CAS that double-counts shows up as more
// than N; one that drops an update shows up as fewer.
func TestConcurrentConservation(t *testing.T) {
	e := newEngine(t, 16)
	const capacity = 100_000
	off, err := e.Register(1, capacity, 0)
	if err != nil {
		t.Fatal(err)
	}

	workers := runtime.NumCPU() * 4
	if workers < 8 {
		workers = 8
	}
	perWorker := (capacity * 2) / workers

	var granted int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			local := int64(0)
			for i := 0; i < perWorker; i++ {
				if e.Allow(off, 1) {
					local++
				}
			}
			atomic.AddInt64(&granted, local)
		}()
	}
	close(start)
	wg.Wait()

	if granted != capacity {
		t.Fatalf("granted %d of %d tokens across %d workers", granted, capacity, workers)
	}
	if tok, _ := e.Tokens(off); tok != 0 {
		t.Fatalf("residual balance %v, want 0", tok)
	}
}

// TestConcurrentRefillIsBounded checks the other direction: under continuous
// contention with refill enabled, the engine must never mint more than the
// configured rate allows over the observed window.
func TestConcurrentRefillIsBounded(t *testing.T) {
	e := newEngine(t, 16)
	const capacity = 1000
	const rate = 200_000.0 // tokens/sec
	off, err := e.Register(1, capacity, rate)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := e.Snapshot(off)
	effective := st.EffectiveRatePerSecond

	workers := runtime.NumCPU() * 2
	if workers < 4 {
		workers = 4
	}
	var granted int64
	var wg sync.WaitGroup
	begin := Now()
	deadline := time.Now().Add(100 * time.Millisecond)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := int64(0)
			for time.Now().Before(deadline) {
				for i := 0; i < 64; i++ {
					if e.Allow(off, 1) {
						local++
					}
				}
			}
			atomic.AddInt64(&granted, local)
		}()
	}
	wg.Wait()
	elapsed := float64(Now()-begin) / 1e9

	maxAllowed := int64(float64(capacity) + effective*elapsed + 1)
	if granted > maxAllowed {
		t.Fatalf("granted %d over %.4fs, ceiling is %d (cap %d + %.0f/s)",
			granted, elapsed, maxAllowed, capacity, effective)
	}
	// It must also actually be doing work, not just denying everything.
	if granted < capacity {
		t.Fatalf("granted only %d, expected at least the initial burst of %d", granted, capacity)
	}
	t.Logf("granted %d in %.4fs (ceiling %d)", granted, elapsed, maxAllowed)
}

func TestConcurrentRegistrationIsIdempotent(t *testing.T) {
	e := newEngine(t, 512)
	const racers = 32
	var wg sync.WaitGroup
	results := make([]uintptr, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = e.Register(77, 50, 100)
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	for i := range results {
		if errs[i] == nil {
			winners++
		} else if errs[i] != ErrAlreadyRegistered {
			t.Fatalf("racer %d: unexpected error %v", i, errs[i])
		}
		if results[i] != results[0] {
			t.Fatalf("racer %d got offset %v, racer 0 got %v", i, results[i], results[0])
		}
	}
	if winners != 1 {
		t.Fatalf("%d racers claimed the tenant, want exactly 1", winners)
	}
	if e.LiveTenants() != 1 {
		t.Fatalf("LiveTenants = %d, want 1 (rolled-back slots must be recycled)", e.LiveTenants())
	}
	// Slots must be conserved: every slot the racers pulled off the bump
	// allocator either backs the winner or went back on the free list. Losers
	// that noticed the published value before allocating never took one at
	// all, so the count is timing-dependent - the invariant is the balance.
	allocated := atomic.LoadUint64(&e.ctl.nextSlot)
	free := uint64(0)
	for {
		if _, ok := e.popFree(); !ok {
			break
		}
		free++
	}
	if allocated-free != 1 {
		t.Fatalf("%d slots allocated, %d recycled: %d leaked", allocated, free, allocated-free-1)
	}
}

func TestChurnUnderConcurrency(t *testing.T) {
	e := newEngine(t, 1024)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				id := uint64(w*1000 + i%64 + 1)
				off, err := e.Register(id, 16, 1000)
				if err != nil && err != ErrAlreadyRegistered {
					continue // arena pressure is a legitimate outcome here
				}
				e.AllowOwned(off, id, 1)
				_ = e.Unregister(id)
			}
		}(w)
	}
	wg.Wait()
	if lt := e.LiveTenants(); lt != 0 {
		t.Fatalf("LiveTenants = %d after full churn, want 0", lt)
	}
}

// --- allocation behaviour --------------------------------------------------

// TestHotPathIsAllocationFree is the GC claim, stated as an assertion. If this
// ever fails, the engine has started producing garbage on the request path and
// the entire premise of the library is void.
func TestHotPathIsAllocationFree(t *testing.T) {
	e := newEngine(t, 64)
	off, _ := e.Register(1, 1_000_000, 1_000_000)
	if n := testing.AllocsPerRun(2000, func() { e.Allow(off, 1) }); n != 0 {
		t.Fatalf("Allow allocated %v objects per call, want 0", n)
	}
	if n := testing.AllocsPerRun(2000, func() { e.AllowOwned(off, 1, 1) }); n != 0 {
		t.Fatalf("AllowOwned allocated %v objects per call, want 0", n)
	}
	if n := testing.AllocsPerRun(2000, func() { e.AllowTenant(1, 1) }); n != 0 {
		t.Fatalf("AllowTenant allocated %v objects per call, want 0", n)
	}
}

func TestCoarseClockRefills(t *testing.T) {
	e, err := New(Config{MaxTenants: 8, ClockResolution: 200 * time.Microsecond})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	off, err := e.Register(1, 100, 100_000)
	if err != nil {
		t.Fatal(err)
	}
	if !e.Allow(off, 100) {
		t.Fatal("drain failed")
	}
	if e.Allow(off, 1) {
		t.Fatal("drained bucket granted a token")
	}
	time.Sleep(20 * time.Millisecond)
	if !e.Allow(off, 1) {
		t.Fatal("coarse clock never advanced the refill")
	}
}

func TestClosedEngineRejectsRegistration(t *testing.T) {
	e, err := New(Config{MaxTenants: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := e.Register(1, 1, 1); err != ErrClosed {
		t.Fatalf("Register after Close = %v, want ErrClosed", err)
	}
}

func TestInvalidConfig(t *testing.T) {
	if _, err := New(Config{MaxTenants: 0}); err != ErrInvalidConfig {
		t.Fatalf("MaxTenants=0 gave %v, want ErrInvalidConfig", err)
	}
}

// --- benchmarks ------------------------------------------------------------

func BenchmarkAllowUncontended(b *testing.B) {
	e := newEngineB(b)
	off, _ := e.Register(1, 1<<30, MaxRatePerSecond/2)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Allow(off, 1)
	}
}

// BenchmarkAllowContended is the noisy-neighbour worst case: every core
// fighting over one tenant's single cache line.
func BenchmarkAllowContended(b *testing.B) {
	e := newEngineB(b)
	off, _ := e.Register(1, 1<<30, MaxRatePerSecond/2)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.Allow(off, 1)
		}
	})
}

// BenchmarkAllowDistinctTenants is the case Sluice is actually designed for:
// every core working on a different tenant, so no line is ever shared. The gap
// between this and the contended benchmark is the cache-line isolation payoff.
func BenchmarkAllowDistinctTenants(b *testing.B) {
	e := newEngineB(b)
	const tenants = 8192
	offs := make([]uintptr, tenants)
	for i := 0; i < tenants; i++ {
		offs[i], _ = e.Register(uint64(i+1), 1<<30, MaxRatePerSecond/2)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var seq int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(atomic.AddInt64(&seq, 1)) * 977
		for pb.Next() {
			i++
			e.Allow(offs[i&(tenants-1)], 1)
		}
	})
}

// BenchmarkAllowContendedCachedClock is the same worst case with the clock
// read hoisted out. With now constant between ticks the refill claim
// early-outs, leaving exactly one contended CAS per decision - which is the
// floor for any shared-counter design and the reason ClockResolution exists.
func BenchmarkAllowContendedCachedClock(b *testing.B) {
	e := newEngineB(b)
	off, _ := e.Register(1, 1<<30, MaxRatePerSecond/2)
	now := Now()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.AllowAt(off, 1, now)
		}
	})
}

func BenchmarkAllowAtSuppliedClock(b *testing.B) {
	e := newEngineB(b)
	off, _ := e.Register(1, 1<<30, MaxRatePerSecond/2)
	now := Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.AllowAt(off, 1, now)
	}
}

func BenchmarkLookup(b *testing.B) {
	e := newEngineB(b)
	for i := 0; i < 4096; i++ {
		e.Register(uint64(i+1), 1000, 1000)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Lookup(uint64(i&4095) + 1)
	}
}

func BenchmarkRegisterUnregister(b *testing.B) {
	e := newEngineB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uint64(i%4096) + 1
		off, err := e.Register(id, 100, 1000)
		if err == nil {
			_ = off
			e.Unregister(id)
		}
	}
}

func newEngineB(b *testing.B) *Engine {
	b.Helper()
	e, err := New(Config{MaxTenants: 16384, PinMemory: true, PinBestEffort: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = e.Close() })
	return e
}
