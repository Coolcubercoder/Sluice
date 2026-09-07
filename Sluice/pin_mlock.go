//go:build darwin || linux

package sluice

import "syscall"

// wireDown pins the mapping into physical memory so that no rate-limit
// decision can ever take a page fault, and so that every page is resident
// before the first request rather than being faulted in during one.
func wireDown(b []byte) error {
	return syscall.Mlock(b)
}
