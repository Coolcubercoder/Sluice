//go:build !(darwin || dragonfly || freebsd || linux || netbsd || openbsd)

package sluice

import "unsafe"

// arena on platforms without a POSIX mmap/mlock pair. Sluice's whole premise is
// GC-invisible, wired-down memory; rather than silently degrade to a Go heap
// allocation that the collector would scan and the pacer would account for, the
// package refuses to start.
type arena struct {
	mem    []byte
	base   unsafe.Pointer
	size   uintptr
	pinned bool
	pinErr error
}

var pageSize = uintptr(4096)

func mapArena(size uintptr, pin, pinBestEffort bool) (*arena, error) {
	return nil, ErrUnsupportedPlatform
}

func (a *arena) prefault() {}

func (a *arena) release() error { return nil }
