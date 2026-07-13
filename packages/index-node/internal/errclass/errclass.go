// Package errclass centralizes task error classification and retry timing.
//
// Pipeline boundaries should add a class with Wrap and preserve the original
// cause. Classify also recognizes a small set of standard-library errors, but
// deliberately treats unknown errors as transient: retrying is safer than
// silently losing an index update.
package errclass

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"runtime"
	"syscall"
	"time"
)

// Class determines whether a task can be retried or requires wider recovery.
// Class implements error so each value can also be used as an errors.Is/errors.As
// sentinel.
type Class string

const (
	Transient Class = "transient"
	Permanent Class = "permanent"
	Poison    Class = "poison"
	Fatal     Class = "fatal"

	// ErrTransient through ErrFatal are error-style aliases for callers that
	// prefer conventional sentinel names. They are constants, not mutable
	// package state.
	ErrTransient Class = Transient
	ErrPermanent Class = Permanent
	ErrPoison    Class = Poison
	ErrFatal     Class = Fatal
)

// Error implements error, allowing a Class to be wrapped as a sentinel.
func (class Class) Error() string {
	return string(class)
}

// String returns the stable lower-case representation stored in dead letters.
func (class Class) String() string {
	return string(class)
}

// Valid reports whether class is one of the four specified classes.
func (class Class) Valid() bool {
	switch class {
	case Transient, Permanent, Poison, Fatal:
		return true
	default:
		return false
	}
}

// ErrorClass lets Class satisfy Classified.
func (class Class) ErrorClass() Class {
	return class
}

// Class returns the receiver. It is kept alongside ErrorClass so simple typed
// errors exposing either common classifier method can be recognized.
func (class Class) Class() Class {
	return class
}

// Parse converts the stable dead-letter representation to a Class.
func Parse(value string) (Class, error) {
	class := Class(value)
	if !class.Valid() {
		return Transient, fmt.Errorf("errclass: invalid class %q", value)
	}
	return class, nil
}

// Classified is implemented by typed errors that carry an explicit class.
// Explicit classification always takes precedence over inference from a cause.
type Classified interface {
	error
	ErrorClass() Class
}

// Error attaches an explicit Class to a cause while preserving the cause for
// errors.Is and errors.As. Kind is exported so errors.As callers can inspect it.
type Error struct {
	Kind Class
	Err  error
}

// New constructs a classified error with a textual cause.
func New(class Class, message string) *Error {
	return &Error{Kind: normalize(class), Err: errors.New(message)}
}

// Wrap attaches class to err. As with fmt.Errorf wrapping, a nil cause returns
// nil. An invalid class is conservatively normalized to Transient.
func Wrap(class Class, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Kind: normalize(class), Err: err}
}

// WithClass is an argument-order convenience for code that starts with err.
func WithClass(err error, class Class) error {
	return Wrap(class, err)
}

func (err *Error) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.Err == nil {
		return normalize(err.Kind).String()
	}
	return err.Err.Error()
}

// Unwrap returns the original cause.
func (err *Error) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

// Is makes each Class value a category sentinel without hiding the cause.
func (err *Error) Is(target error) bool {
	if err == nil {
		return false
	}
	targetClass, ok := target.(Class)
	return ok && normalize(err.Kind) == targetClass
}

// ErrorClass returns the explicit class.
func (err *Error) ErrorClass() Class {
	if err == nil {
		return Transient
	}
	return normalize(err.Kind)
}

// Class is an alias for ErrorClass for classifier interoperability.
func (err *Error) Class() Class {
	return err.ErrorClass()
}

// Classify returns the explicit or inferred class of err. Unknown errors and
// nil default to Transient because the Class type intentionally has no
// fifth "no error" value.
func Classify(err error) Class {
	if err == nil {
		return Transient
	}

	var classified Classified
	if errors.As(err, &classified) {
		if class := classified.ErrorClass(); class.Valid() {
			return class
		}
	}

	// Support typed errors that expose Class rather than ErrorClass.
	var classProvider interface{ Class() Class }
	if errors.As(err, &classProvider) {
		if class := classProvider.Class(); class.Valid() {
			return class
		}
	}

	// Disk exhaustion is a node-wide condition, not a task failure.
	if isDiskFull(err) {
		return Fatal
	}

	// Permissions, read-only media, invalid inputs, and unsupported operations
	// are deterministic for the current task and should go directly to dead
	// letter handling.
	if errors.Is(err, os.ErrPermission) ||
		errors.Is(err, fs.ErrPermission) ||
		errors.Is(err, errors.ErrUnsupported) ||
		errors.Is(err, fs.ErrInvalid) ||
		errors.Is(err, syscall.EROFS) ||
		errors.Is(err, syscall.EFBIG) ||
		errors.Is(err, syscall.ENAMETOOLONG) ||
		errors.Is(err, syscall.ENOTDIR) ||
		errors.Is(err, syscall.EISDIR) ||
		errors.Is(err, syscall.ENOEXEC) ||
		errors.Is(err, syscall.ENOSYS) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.EOPNOTSUPP) {
		return Permanent
	}

	// Timeouts, cancellation, and network failures are retryable. This check is
	// explicit even though Transient is the fallback, documenting the intended
	// handling of compute-node failures and wrapped net.OpError values.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Transient
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return Transient
	}

	return Transient
}

func normalize(class Class) Class {
	if class.Valid() {
		return class
	}
	return Transient
}

func isDiskFull(err error) bool {
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
		return true
	}

	// Windows reports Win32 error codes through syscall.Errno rather than the
	// invented POSIX-compatible ENOSPC value in package syscall.
	if runtime.GOOS == "windows" {
		var errno syscall.Errno
		if errors.As(err, &errno) {
			const (
				windowsErrorHandleDiskFull syscall.Errno = 39
				windowsErrorDiskFull       syscall.Errno = 112
			)
			return errno == windowsErrorHandleDiskFull || errno == windowsErrorDiskFull
		}
	}
	return false
}

const (
	DefaultRetryBase            = 5 * time.Second
	DefaultRetryCap             = 30 * time.Minute
	DefaultMaxTransientAttempts = 8
)

// Policy is an immutable exponential-backoff policy. The injected random
// function makes jitter deterministic in tests; nil selects the concurrency-
// safe standard-library source.
type Policy struct {
	base                 time.Duration
	cap                  time.Duration
	maxTransientAttempts int
	random               func() float64
}

// NewPolicy validates and constructs a retry policy.
func NewPolicy(base, cap time.Duration, maxTransientAttempts int, random func() float64) (Policy, error) {
	switch {
	case base <= 0:
		return Policy{}, errors.New("errclass: retry base must be greater than zero")
	case cap <= 0:
		return Policy{}, errors.New("errclass: retry cap must be greater than zero")
	case cap < base:
		return Policy{}, errors.New("errclass: retry cap must be greater than or equal to base")
	case maxTransientAttempts < 0:
		return Policy{}, errors.New("errclass: maximum transient attempts must not be negative")
	}
	if random == nil {
		random = rand.Float64
	}
	return Policy{
		base:                 base,
		cap:                  cap,
		maxTransientAttempts: maxTransientAttempts,
		random:               random,
	}, nil
}

// DefaultPolicy returns the specification's 5s/30m/eight-attempt policy.
func DefaultPolicy(random func() float64) Policy {
	policy, _ := NewPolicy(
		DefaultRetryBase,
		DefaultRetryCap,
		DefaultMaxTransientAttempts,
		random,
	)
	return policy
}

// Base returns the configured exponential base duration.
func (policy Policy) Base() time.Duration {
	return policy.base
}

// Cap returns the configured pre-jitter cap.
func (policy Policy) Cap() time.Duration {
	return policy.cap
}

// MaxAttempts returns the attempt limit for class. Only Transient task errors
// are retryable; Permanent, Poison, Fatal, and invalid classes return zero.
func (policy Policy) MaxAttempts(class Class) int {
	if class == Transient {
		return policy.maxTransientAttempts
	}
	return 0
}

// ShouldRetry reports whether an already-consumed attempt remains below the
// class limit. For a limit of eight, attempts 0 through 7 are retryable and
// attempt 8 is terminal.
func (policy Policy) ShouldRetry(class Class, attempts int) bool {
	return class == Transient && attempts >= 0 && attempts < policy.maxTransientAttempts
}

// ShouldRetryError classifies err and applies ShouldRetry.
func (policy Policy) ShouldRetryError(err error, attempts int) bool {
	return policy.ShouldRetry(Classify(err), attempts)
}

// Delay computes min(base*2^attempt, cap)*jitter, where jitter is in [0.8,1.2].
// attempt is the exponent from the specification and is zero-based. Negative
// attempts are treated as zero. Invalid random-source values are clamped; NaN
// uses the neutral 1.0 multiplier. Arithmetic saturates instead of overflowing.
func (policy Policy) Delay(attempt int) time.Duration {
	if policy.base <= 0 || policy.cap < policy.base {
		return 0
	}
	if attempt < 0 {
		attempt = 0
	}

	delay := policy.base
	for exponent := 0; exponent < attempt && delay < policy.cap; exponent++ {
		if delay > policy.cap/2 {
			delay = policy.cap
			break
		}
		delay *= 2
		if delay > policy.cap {
			delay = policy.cap
		}
	}

	random := policy.random
	if random == nil {
		random = rand.Float64
	}
	value := random()
	switch {
	case math.IsNaN(value):
		value = 0.5
	case value < 0:
		value = 0
	case value > 1:
		value = 1
	}

	scaled := float64(delay) * (0.8 + 0.4*value)
	if math.IsInf(scaled, 1) || scaled >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	result := time.Duration(math.Round(scaled))
	if result < time.Nanosecond {
		return time.Nanosecond
	}
	return result
}

// NextAttempt adds Delay(attempt) to now.
func (policy Policy) NextAttempt(now time.Time, attempt int) time.Time {
	return now.Add(policy.Delay(attempt))
}
