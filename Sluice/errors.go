package sluice

// Error is Sluice's sentinel error type. It is a string kind rather than a
// struct so that every sentinel below is a compile-time constant: comparing
// against one costs nothing, none of them can be mutated by a caller, and the
// package needs no import of "errors" to declare them.
type Error string

func (e Error) Error() string { return string(e) }

const (
	// ErrInvalidConfig is returned when the requested arena geometry is
	// nonsensical (zero tenants, or more than the address space allows).
	ErrInvalidConfig = Error("sluice: invalid engine configuration")

	// ErrInvalidCapacity is returned when a bucket is registered with a
	// capacity of zero, which could never admit a single request.
	ErrInvalidCapacity = Error("sluice: bucket capacity must be greater than zero")

	// ErrInvalidRate is returned for a negative or NaN refill rate.
	ErrInvalidRate = Error("sluice: refill rate must be a non-negative number")

	// ErrRateOutOfRange is returned when a refill rate exceeds
	// MaxRatePerSecond and therefore cannot be represented in the slot's
	// fixed-width per-nanosecond rate field.
	ErrRateOutOfRange = Error("sluice: refill rate exceeds the representable maximum")

	// ErrInvalidTenant is returned for the two reserved tenant identifiers.
	// Zero is the directory's "empty bucket" sentinel and cannot be a key.
	ErrInvalidTenant = Error("sluice: tenant id 0 is reserved")

	// ErrArenaExhausted is returned when every bucket slot in the arena is
	// allocated. Sluice never grows its mapping at runtime: a fixed arena is
	// what makes the memory footprint a startup-time constant instead of a
	// tail-latency hazard.
	ErrArenaExhausted = Error("sluice: no free bucket slots remain in the arena")

	// ErrRegistryFull is returned when the tenant directory cannot accept a
	// new key. Directory entries are permanent per distinct tenant id, so this
	// signals that the engine was sized for fewer identities than the workload
	// actually presents.
	ErrRegistryFull = Error("sluice: tenant directory is full")

	// ErrAlreadyRegistered is returned by Register when the tenant already
	// owns a live slot. The existing offset is returned alongside it, so a
	// caller racing to register the same tenant twice can simply use it.
	ErrAlreadyRegistered = Error("sluice: tenant is already registered")

	// ErrUnknownTenant is returned when a tenant has no published slot.
	ErrUnknownTenant = Error("sluice: tenant is not registered")

	// ErrBadOffset is returned when a slot offset does not address a properly
	// aligned slot inside the arena.
	ErrBadOffset = Error("sluice: slot offset is outside the arena or misaligned")

	// ErrMisalignedArena is returned if the kernel hands back a mapping that
	// is not cache-line aligned, which would make the engine's 64-bit atomics
	// unsound. It should be unreachable on any real system.
	ErrMisalignedArena = Error("sluice: kernel returned a misaligned mapping")

	// ErrClosed is returned by operations on an engine that has been closed.
	ErrClosed = Error("sluice: engine is closed")

	// ErrPinUnsupported is reported when the platform's syscall package does
	// not expose mlock. The arena is mapped and prefaulted, but evictable.
	ErrPinUnsupported = Error("sluice: memory pinning is unavailable on this platform")

	// ErrUnsupportedPlatform is returned by New on platforms lacking the
	// POSIX mmap/mlock pair Sluice's off-heap arena requires.
	ErrUnsupportedPlatform = Error("sluice: off-heap arena requires a POSIX mmap platform")
)

// SyscallError wraps a failing kernel call with the operation that failed.
type SyscallError struct {
	Op  string
	Err error
}

func (e *SyscallError) Error() string { return "sluice: " + e.Op + ": " + e.Err.Error() }

func (e *SyscallError) Unwrap() error { return e.Err }
