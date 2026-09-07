//go:build dragonfly || freebsd || netbsd || openbsd

package sluice

// wireDown on the BSDs whose syscall package does not expose mlock. The arena
// still maps and prefaults; it is simply evictable, so PinMemory must be used
// with PinBestEffort here and Engine.PinError will report why.
func wireDown(b []byte) error {
	return ErrPinUnsupported
}
