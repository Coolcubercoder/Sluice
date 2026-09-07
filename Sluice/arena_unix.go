//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package sluice

import (
	"syscall"
	"unsafe"
)

// arena is a contiguous, page-aligned, optionally wired-down region of
// anonymous memory obtained straight from the kernel. It is deliberately *not*
// a Go allocation: the Go garbage collector neither scans it nor accounts for
// it, so a Sluice tracking matrix holding ten million tenants contributes
// exactly zero pointers to the mark phase and zero bytes to the heap goal that
// drives GC pacing.
type arena struct {
	mem    []byte         // the mapping, kept for munmap
	base   unsafe.Pointer // &mem[0], the origin all offsets are relative to
	size   uintptr        // usable bytes
	pinned bool           // true once mlock succeeded
	pinErr error          // non-nil if a best-effort mlock was refused
}

// pageSize is resolved once; mapArena rounds every request up to it so that
// the arena never shares a page with an unrelated mapping.
var pageSize = uintptr(syscall.Getpagesize())

// mapArena reserves size bytes of anonymous, private, read-write memory.
//
// MAP_ANON|MAP_PRIVATE guarantees the mapping is zero-filled on first touch,
// which is what lets Sluice treat "all zeroes" as the canonical empty state for
// the control header, the tenant directory and every unallocated slot: no
// initialisation pass over the arena is required at startup, so mapping a
// multi-gigabyte matrix costs a single syscall rather than a page-fault storm.
func mapArena(size uintptr, pin, pinBestEffort bool) (*arena, error) {
	if size == 0 {
		return nil, ErrInvalidConfig
	}
	size = (size + pageSize - 1) &^ (pageSize - 1)

	mem, err := syscall.Mmap(
		-1, 0, int(size),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE,
	)
	if err != nil {
		return nil, &SyscallError{Op: "mmap", Err: err}
	}

	a := &arena{mem: mem, base: unsafe.Pointer(&mem[0]), size: size}

	// A kernel mapping is page aligned, and every page size in existence is a
	// multiple of 64, so this holds universally. It is asserted anyway: the
	// entire atomic story below depends on it, and a cheap check at startup is
	// preferable to a misaligned CAS in production.
	if uintptr(a.base)&(CacheLineSize-1) != 0 {
		_ = syscall.Munmap(mem)
		return nil, ErrMisalignedArena
	}

	if pin {
		// Wiring the pages down does two things that matter at the tail of the
		// latency distribution: it removes the possibility that a rate-limit
		// decision blocks on a major page fault (a swap-in on the request
		// path is a multi-millisecond stall), and it prefaults every page so
		// the first request for a given tenant is no slower than the millionth.
		if err := wireDown(mem); err != nil {
			if !pinBestEffort {
				_ = syscall.Munmap(mem)
				return nil, &SyscallError{Op: "mlock", Err: err}
			}
			// Best-effort mode: surface the failure without refusing to run.
			// The usual cause is RLIMIT_MEMLOCK, which is 64KiB by default in
			// a lot of container runtimes.
			a.pinErr = err
			// Fall back to touching every page so the mapping is at least
			// resident and the fast path never takes a minor fault either.
			a.prefault()
		} else {
			a.pinned = true
		}
	}
	return a, nil
}

// prefault walks the mapping one page at a time, forcing the kernel to back
// each page with a physical frame now rather than during a request.
func (a *arena) prefault() {
	for off := uintptr(0); off < a.size; off += pageSize {
		p := (*uint64)(unsafe.Add(a.base, off))
		atomicTouch(p)
	}
}

// release unmaps the arena. munmap implicitly unlocks any wired pages.
func (a *arena) release() error {
	if a.mem == nil {
		return nil
	}
	mem := a.mem
	a.mem, a.base, a.size = nil, nil, 0
	if err := syscall.Munmap(mem); err != nil {
		return &SyscallError{Op: "munmap", Err: err}
	}
	return nil
}
