# Sluice

A multi-tenant, lock-free token bucket rate limiter for Go whose entire tracking
matrix lives outside the Go heap.

Sluice makes a rate-limit decision in **~5 ns** across **millions of tenants**
with **zero allocations**, **zero locks**, and **zero bytes visible to the
garbage collector**. State lives in one anonymous `mmap`'d, `mlock`'d arena
divided into fixed 64-byte, cache-line-isolated slots, mutated only by atomic
compare-and-swap.

Pure Go. The library imports `syscall`, `unsafe`, `sync/atomic` and `time`, and
nothing else.

---

## The problem

The noisy-neighbour problem in a multi-tenant data plane is easy to state — one
tenant's burst must not degrade anybody else's latency — and hard to solve *in
Go*, because the obvious implementation fails three ways at once:

1. **The GC sees everything.** A `map[string]*bucket` is a root holding millions
   of pointers. Every mark phase walks all of them. At ten million tenants the
   mark cost alone puts a periodic latency cliff into the request path that no
   amount of `GOGC` tuning removes.
2. **Tenant churn allocates.** Allocation is what triggers collection, so a
   surge of new tenants provokes exactly the pause the data plane can least
   afford, at exactly the moment it is busiest.
3. **The mutex serialises strangers.** Tenant A's bucket update has nothing to
   do with tenant B's, yet they queue behind one lock.

Sluice removes all three by construction: the arena is invisible to the
collector, registration is a CAS into preallocated off-heap memory rather than
an allocation, and a decision touches exactly one cache line that no other
tenant shares.

## Install

```sh
go get github.com/Coolcubercoder/Sluice
```

The import path ends in `Sluice`, the package clause is `sluice`:

```go
import "github.com/Coolcubercoder/Sluice" // package sluice
```

## Quick start

```go
package main

import (
	"log"
	"time"

	"github.com/Coolcubercoder/Sluice"
)

func main() {
	engine, err := sluice.New(sluice.Config{
		MaxTenants:      1 << 20,               // 1,048,576 buckets, ~96 MiB
		PinMemory:       true,                  // wire the arena down
		PinBestEffort:   true,                  // tolerate a low RLIMIT_MEMLOCK
		ClockResolution: 100 * time.Microsecond, // cache the clock; optional
	})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// Resolve once, at connection or session setup: 5000-token burst,
	// refilling at 2000 tokens/sec.
	const tenantID = 40219
	offset, err := engine.Register(tenantID, 5000, 2000)
	if err != nil && err != sluice.ErrAlreadyRegistered {
		log.Fatal(err)
	}

	// Then, on every request: no hashing, no locking, no allocation.
	if !engine.Allow(offset, 1) {
		// shed, queue, or return 429 - Sluice only decides
		return
	}
}
```

The `offset` returned by `Register` is the fast-path handle. Cache it next to
your connection state so a decision never has to hash anything. `Lookup` and
`AllowTenant` exist for when you can't, and cost a hash plus a probe.

## How it works

### One arena, invisible to the collector

`New` takes a single `MAP_PRIVATE|MAP_ANON` mapping and never grows it. The
mapping is kernel-zeroed on first touch, and zero is the canonical empty state
of every structure Sluice stores — so no initialisation pass runs over the
arena, and mapping a multi-gigabyte matrix is one syscall rather than a
page-fault storm.

```
┌──────────────┬─────────────────────────┬──────────────────────────────────┐
│ control      │ tenant directory        │ bucket slots                     │
│ 4 cache lines│ 2^k × 16B, open-addr.   │ MaxTenants × 64B                 │
│ (bump cursor,│ lock-free, load ≤ 0.5   │ one cache line each              │
│  free list,  │                         │                                  │
│  coarse clock│                         │                                  │
└──────────────┴─────────────────────────┴──────────────────────────────────┘
```

With `PinMemory`, `mlock` wires the pages down: no decision can block on a major
page fault (a swap-in on the request path is a multi-millisecond stall), and
every page is resident before the first request, so request #1 is as fast as
request #1,000,000.

### The slot: exactly one cache line

```
offset  0 ┌────────────────────────────────┐
          │ lastRefill      uint64         │  CAS target — interval claim
offset  8 ├────────────────────────────────┤
          │ tokens          uint64         │  CAS target — Q32.32 fixed point
offset 16 ├────────────────┬───────────────┤
          │ capacity u32   │ rateFP  u32   │  packed so every u64 stays 8-aligned
offset 24 ├────────────────┴───────────────┤
          │ granted         uint64         │  optional counters
offset 32 ├────────────────────────────────┤
          │ rejected        uint64         │
offset 40 ├────────────────┬───────────────┤
          │ state    u32   │ _pad0   u32   │
offset 48 ├────────────────┴───────────────┤
          │ tenantID        uint64         │  ownership check for AllowOwned
offset 56 ├────────────────────────────────┤
          │ link            uint64         │  intrusive free-list next
offset 64 └────────────────────────────────┘  = 64B, cache-line aligned
```

Two properties are load-bearing, and both are **asserted at compile time** in
[`layout.go`](layout.go) — a drifting field is a build failure, not a 3am page:

- **Every 64-bit atomic target is 8-byte aligned.** An unaligned `LDXR/STXR`
  pair faults outright on arm64; a split-line `LOCK CMPXCHG` on x86-64 is
  silently but catastrophically slow. The two `uint32` fields are packed into
  one 8-byte lane specifically so nothing after them is knocked off a boundary.
- **Slot size equals cache line size, and the region is line-aligned.** Two
  workers processing two different tenants can never touch the same physical
  line, so they never invalidate each other's caches. This is why throughput
  goes *up* with core count instead of down (see the benchmarks).

### The decision: two cooperating lock-free protocols

There is no portable 128-bit compare-and-swap in Go, so `lastRefill` and
`tokens` cannot be updated in one atomic step. Instead each word gets a protocol
that is individually linearisable and jointly conservative:

- **`lastRefill` is an interval claim.** A refiller CASes it from the timestamp
  it read to the timestamp it observed *now*. Exactly one thread can win any
  given interval, and successive winners claim strictly disjoint, contiguous
  intervals that tile the timeline. Tokens for an interval are therefore minted
  **exactly once**, no matter how many threads race — the losers skip refilling
  because the winner is already minting on their behalf.
- **`tokens` is a bounded accumulator.** Refill adds and clamps to capacity;
  consumption subtracts only when the balance covers the request. Both are
  ordinary CAS loops, and both are ABA-immune because the value is a *quantity*,
  not a pointer: a balance that returns to its original value has, by
  conservation, had exactly offsetting work applied to it, so re-running the
  arithmetic on it is correct by construction.

Tokens are tracked as **Q32.32 fixed point** — 32 bits of whole tokens, 32 bits
of fraction. That is what makes racing refills exact: the accrual over two
disjoint intervals sums to precisely the accrual over their union, with no
floating-point drift and no truncation-induced starvation at high call rates.
The accrual product `elapsed × rate` genuinely can exceed 64 bits (an idle slot
plus a high rate), so it is computed in two 32-bit limbs and **saturates**
rather than wrapping; the saturated value is then absorbed by the capacity
clamp.

`TestConcurrentConservation` is the proof: 100,000 tokens, no refill, ~40
goroutines racing — exactly 100,000 grants, every run.

## Performance

Apple M4, `go1.27`, `-benchtime 3000000x`. Every path is 0 B/op, 0 allocs/op.

| Benchmark | 1 core | 10 cores | What it measures |
|---|---:|---:|---|
| `AllowDistinctTenants` | 18.2 ns | **5.10 ns** | The design target: every core on a different tenant |
| `AllowUncontended` | 26.7 ns | 19.8 ns | One tenant, one core — dominated by the clock read |
| `AllowAtSuppliedClock` | 4.73 ns | 4.54 ns | Same, with the clock read hoisted out |
| `AllowContended` | 19.3 ns | 344 ns | Worst case: 10 cores fighting over one tenant's line |
| `AllowContendedCachedClock` | 4.67 ns | 246 ns | Same, with `now` constant between ticks |
| `Lookup` | 2.56 ns | 2.55 ns | Directory hash + probe |
| `RegisterUnregister` | 23.9 ns | 24.0 ns | Full registration lifecycle |

Two things worth reading out of that table:

**Distinct tenants get *faster* with more cores** (18.2 ns → 5.10 ns). That is
cache-line isolation working exactly as intended — ten cores doing ten
independent decisions with no coherence traffic between them.

**Contention on a single tenant costs ~344 ns**, and that is physics, not a bug:
ten cores serialising on one cache line. It is still lock-free, still
allocation-free, and still ~29M decisions/sec on that one tenant. Caching the
clock cuts it to 246 ns (−28%) because a constant `now` lets the refill claim
early-out, removing one contended CAS per decision — which is the main reason
`ClockResolution` exists.

## Memory

Per tenant: a 64-byte slot, plus directory space. The directory is a
power-of-two table held at a 0.5 load factor, so the total is 96 B/tenant when
`MaxTenants` is itself a power of two, and up to 128 B/tenant when it sits just
above one.

| `MaxTenants` | Arena | Per tenant |
|---:|---:|---:|
| 10,000 | 1.1 MiB | 116.5 B |
| 100,000 | 10.1 MiB | 105.9 B |
| 1,000,000 | 93.0 MiB | 97.6 B |
| 8,388,608 (2²³) | 768.0 MiB | 96.0 B |
| 10,000,000 | 1.10 GiB | 117.7 B |

Round to a power of two if the footprint matters. None of it counts toward the
Go heap goal, appears in `runtime.MemStats`, or is walked by the mark phase.

## API

```go
// Lifecycle
func New(cfg Config) (*Engine, error)
func (e *Engine) Close() error

// Registration (cold path)
func (e *Engine) Register(tenantID uint64, capacity uint32, ratePerSecond float64) (uintptr, error)
func (e *Engine) RegisterRaw(tenantID uint64, capacity uint32, refillRatePerNano uint32) (uintptr, error)
func (e *Engine) Unregister(tenantID uint64) error
func (e *Engine) Lookup(tenantID uint64) (uintptr, bool)

// Decisions (hot path)
func (e *Engine) Allow(tenantSlotOffset uintptr, tokensRequested uint64) bool
func (e *Engine) AllowAt(tenantSlotOffset uintptr, tokensRequested uint64, nowNanos uint64) bool
func (e *Engine) AllowOwned(tenantSlotOffset uintptr, tenantID uint64, tokensRequested uint64) bool
func (e *Engine) AllowTenant(tenantID uint64, tokensRequested uint64) bool

// Administration and telemetry
func (e *Engine) SetLimit(tenantSlotOffset uintptr, capacity uint32, ratePerSecond float64) error
func (e *Engine) Reset(tenantSlotOffset uintptr) error
func (e *Engine) Snapshot(tenantSlotOffset uintptr) (Stats, error)
func (e *Engine) Tokens(tenantSlotOffset uintptr) (float64, error)
func (e *Engine) Pinned() bool
func (e *Engine) PinError() error
func (e *Engine) ArenaBytes() uintptr
func (e *Engine) Capacity() uint32
func (e *Engine) LiveTenants() uint64
func (e *Engine) DirectoryLoad() float64
func Now() uint64
```

**`AllowAt`** takes a caller-supplied timestamp on the same timeline as `Now()`.
Event-loop servers that already stamp each batch of packets should use it: it
removes the clock read from the per-decision cost entirely. Timestamps that move
backwards are ignored rather than trusted, so feeding slightly stale values from
parallel workers is safe — it defers a refill, it does not corrupt the bucket.

**`AllowOwned`** adds an ownership assertion — one extra load from the same
already-hot cache line. Use it if you have tenant churn; see below.

**`SetLimit`** reconfigures a live bucket without interrupting traffic. Time is
settled under the *old* rate first, so tokens already earned are minted at the
rate that earned them.

## Operational notes

Five things to know before this goes near production traffic.

**Pinning needs `RLIMIT_MEMLOCK`.** Many container runtimes ship a 64 KiB limit.
Either raise it (`--ulimit memlock=…`, or `LimitMEMLOCK=` in a systemd unit) or
set `PinBestEffort: true`, which downgrades an `mlock` failure to a recorded
warning and prefaults the arena instead — resident, but evictable. Check
`Pinned()` and `PinError()` at startup and log the result; silently unpinned is
the failure mode you don't want to discover from a p99 graph.

**Directory keys are permanent per distinct tenant ID.** `Unregister` recycles
the 64-byte slot immediately, but not the hash key — reclaiming a key means
breaking a probe chain, and doing that safely under concurrent lookups needs
quiescence, which is the whole thing a lock-free data plane exists to avoid. So
size `MaxTenants` for the number of *distinct identities the process will ever
see*, not the number live at once, and alert on `DirectoryLoad()` crossing ~0.7.
Past that, probe chains lengthen and `Register` will eventually return
`ErrRegistryFull`.

**Cached offsets go stale on `Unregister`.** The slot is immediately eligible
for reuse, so a cached offset elsewhere in the process can end up pointing at a
*different* tenant's bucket — and spending against it would debit an innocent
third party. If you have tenant churn, use `AllowOwned`, which re-checks the
slot's owner from the same cache line it was going to read anyway.

**Low rates quantise.** The refill rate is a 32-bit per-nanosecond fixed-point
field, so relative error is bounded by `1/(2·rateFP)`: ~0.012% at 1000 tok/s,
but ~12% at 1 tok/s. `Snapshot().EffectiveRatePerSecond` reports exactly what a
slot will deliver, so this is observable rather than surprising. If you need
precision down there, scale your token unit — bill in milli-tokens — rather than
fighting the representation. Representable range is `MinRatePerSecond` (~0.233)
to `MaxRatePerSecond` (~1e9).

**`Close` is not safe against in-flight traffic.** It unmaps the arena;
a concurrent `Allow` would fault on freed pages. Drain traffic, then close.
Everything else — `Allow`, `Register`, `Unregister`, `Lookup`, `Snapshot`,
`SetLimit` — is safe from any number of goroutines simultaneously.

One honest note on the algorithm: a thread that wins an interval claim and is
then descheduled before adding its tokens leaves those tokens briefly unminted.
They are not lost — they land when it resumes — and no thread can block waiting
for it, so the engine remains lock-free in the formal sense. The transient
effect under extreme preemption is a limiter that is momentarily *stricter* than
configured, which is the correct direction to err for something whose job is
protecting a shared resource.

## Platforms

| Platform | Arena | Pinning |
|---|---|---|
| linux (amd64, arm64, 386) | ✅ `mmap` | ✅ `mlock` |
| darwin | ✅ `mmap` | ✅ `mlock` |
| freebsd, netbsd, openbsd, dragonfly | ✅ `mmap` | ⚠️ prefault only — `syscall` exposes no `mlock`; use `PinBestEffort` |
| everything else | ❌ `ErrUnsupportedPlatform` | — |

On unsupported platforms `New` refuses to start rather than silently degrading
to a Go-heap allocation the collector would scan — the failure is loud because
the premise of the library would otherwise be quietly void.

## Testing

```sh
go test -race -count=3 -cpu 1,10 ./...
go test -run XXX -bench . -benchtime 3000000x -cpu 1,10 ./...
```

The suite is written to falsify the claims on this page, not to cover lines:

- `TestConcurrentConservation` — exact token conservation under ~40 racing goroutines
- `TestConcurrentRefillIsBounded` — grants never exceed `capacity + rate × elapsed`
- `TestHotPathIsAllocationFree` — `AllocsPerRun == 0` for all three `Allow` variants
- `TestSlotGeometry` / `TestArenaAlignment` — 64-byte slots, 8-byte atomics, no shared lines
- `TestScaledRefillSaturates` — the accrual multiply saturates instead of wrapping
- `TestDeterministicRefill` — exact token counts against an injected clock, backwards time is inert
- `TestConcurrentRegistrationIsIdempotent` — 32 racers, one winner, zero leaked slots
- `TestStaleOffsetIsCaughtByAllowOwned` — recycled slots don't debit the wrong tenant
