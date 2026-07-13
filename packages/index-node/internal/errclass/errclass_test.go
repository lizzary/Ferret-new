package errclass

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"net"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func TestClassSentinelsAndParse(t *testing.T) {
	t.Parallel()

	classes := []Class{Transient, Permanent, Poison, Fatal}
	for _, class := range classes {
		class := class
		t.Run(class.String(), func(t *testing.T) {
			t.Parallel()
			if !class.Valid() {
				t.Fatalf("%q is not valid", class)
			}
			if class.Error() != class.String() {
				t.Fatalf("Error() = %q, String() = %q", class.Error(), class.String())
			}
			if class.ErrorClass() != class || class.Class() != class {
				t.Fatalf("classifier methods did not return %q", class)
			}

			wrapped := fmt.Errorf("operation failed: %w", class)
			if !errors.Is(wrapped, class) {
				t.Fatalf("errors.Is(%v, %q) = false", wrapped, class)
			}
			var asClass Class
			if !errors.As(wrapped, &asClass) || asClass != class {
				t.Fatalf("errors.As() = %q, want %q", asClass, class)
			}
			if got := Classify(wrapped); got != class {
				t.Fatalf("Classify() = %q, want %q", got, class)
			}
			parsed, err := Parse(class.String())
			if err != nil || parsed != class {
				t.Fatalf("Parse(%q) = %q, %v", class, parsed, err)
			}
		})
	}

	if ErrTransient != Transient || ErrPermanent != Permanent || ErrPoison != Poison || ErrFatal != Fatal {
		t.Fatal("error-style aliases do not match their classes")
	}
	if Class("unknown").Valid() {
		t.Fatal("unknown class is valid")
	}
	if got, err := Parse("TRANSIENT"); err == nil || got != Transient {
		t.Fatalf("Parse(invalid) = %q, %v", got, err)
	}
}

func TestClassifiedErrorPreservesClassAndCause(t *testing.T) {
	t.Parallel()

	cause := errors.New("decoder rejected input")
	wrapped := fmt.Errorf("extract file: %w", Wrap(Permanent, cause))
	if !errors.Is(wrapped, Permanent) {
		t.Fatal("classified error does not match Permanent sentinel")
	}
	if !errors.Is(wrapped, cause) {
		t.Fatal("classified error does not preserve its cause")
	}
	if got := Classify(wrapped); got != Permanent {
		t.Fatalf("Classify() = %q, want permanent", got)
	}

	var typed *Error
	if !errors.As(wrapped, &typed) {
		t.Fatal("errors.As() did not find *Error")
	}
	if typed.Kind != Permanent || typed.Err != cause || typed.Class() != Permanent {
		t.Fatalf("typed error = %+v", typed)
	}
	var classified Classified
	if !errors.As(wrapped, &classified) || classified.ErrorClass() != Permanent {
		t.Fatalf("errors.As(Classified) = %T, %v", classified, classified)
	}

	if got := Wrap(Permanent, nil); got != nil {
		t.Fatalf("Wrap(class, nil) = %v, want nil", got)
	}
	if got := WithClass(cause, Poison); !errors.Is(got, Poison) || !errors.Is(got, cause) {
		t.Fatalf("WithClass() = %v", got)
	}
	if got := Wrap(Class("bad"), cause); Classify(got) != Transient || !errors.Is(got, Transient) {
		t.Fatalf("Wrap(invalid) = class %q", Classify(got))
	}

	created := New(Fatal, "database corrupt")
	if created.Error() != "database corrupt" || Classify(created) != Fatal {
		t.Fatalf("New() = %v, class %q", created, Classify(created))
	}
	classOnly := &Error{Kind: Poison}
	if classOnly.Error() != Poison.String() || classOnly.Unwrap() != nil || !errors.Is(classOnly, Poison) {
		t.Fatalf("class-only Error = %v", classOnly)
	}

	var nilError *Error
	if nilError.Error() != "<nil>" || nilError.Unwrap() != nil || nilError.Is(Transient) {
		t.Fatal("nil *Error methods are not safe")
	}
	if nilError.ErrorClass() != Transient || nilError.Class() != Transient {
		t.Fatal("nil *Error should conservatively report transient")
	}
}

func TestClassifyStandardAndTypedErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want Class
	}{
		{name: "nil", want: Transient},
		{name: "unknown", err: errors.New("unrecognized failure"), want: Transient},
		{name: "context canceled", err: fmt.Errorf("worker: %w", context.Canceled), want: Transient},
		{name: "context deadline", err: fmt.Errorf("compute: %w", context.DeadlineExceeded), want: Transient},
		{name: "network timeout", err: &testNetError{timeout: true}, want: Transient},
		{name: "network unavailable", err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("unavailable")}, want: Transient},
		{name: "permission", err: &os.PathError{Op: "open", Path: "secret", Err: fs.ErrPermission}, want: Permanent},
		{name: "unsupported", err: fmt.Errorf("extract: %w", errors.ErrUnsupported), want: Permanent},
		{name: "invalid", err: fmt.Errorf("parse: %w", fs.ErrInvalid), want: Permanent},
		{name: "read only filesystem", err: syscall.EROFS, want: Permanent},
		{name: "file too large", err: syscall.EFBIG, want: Permanent},
		{name: "name too long", err: syscall.ENAMETOOLONG, want: Permanent},
		{name: "not directory", err: syscall.ENOTDIR, want: Permanent},
		{name: "is directory", err: syscall.EISDIR, want: Permanent},
		{name: "not executable", err: syscall.ENOEXEC, want: Permanent},
		{name: "not implemented", err: syscall.ENOSYS, want: Permanent},
		{name: "no disk space", err: &os.PathError{Op: "write", Path: "index", Err: syscall.ENOSPC}, want: Fatal},
		{name: "quota exceeded", err: syscall.EDQUOT, want: Fatal},
		{name: "typed poison", err: typedClassError{kind: Poison}, want: Poison},
		{name: "class method fatal", err: classMethodError{kind: Fatal}, want: Fatal},
		{name: "explicit overrides permission inference", err: Wrap(Transient, fs.ErrPermission), want: Transient},
		{name: "explicit overrides disk inference", err: Wrap(Permanent, syscall.ENOSPC), want: Permanent},
		{name: "invalid typed falls back", err: typedClassError{kind: "invalid"}, want: Transient},
	}
	if runtime.GOOS == "windows" {
		tests = append(tests,
			struct {
				name string
				err  error
				want Class
			}{name: "windows disk full", err: &os.PathError{Op: "write", Path: "index", Err: syscall.Errno(112)}, want: Fatal},
		)
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := Classify(test.err); got != test.want {
				t.Fatalf("Classify(%T: %v) = %q, want %q", test.err, test.err, got, test.want)
			}
		})
	}
}

type testNetError struct {
	timeout bool
}

func (err *testNetError) Error() string { return "network error" }
func (err *testNetError) Timeout() bool { return err.timeout }

type typedClassError struct {
	kind Class
}

func (err typedClassError) Error() string     { return "typed class error" }
func (err typedClassError) ErrorClass() Class { return err.kind }

type classMethodError struct {
	kind Class
}

func (err classMethodError) Error() string { return "class method error" }
func (err classMethodError) Class() Class  { return err.kind }

func TestNewPolicyValidationAndDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		base time.Duration
		cap  time.Duration
		max  int
	}{
		{name: "zero base", base: 0, cap: time.Second, max: 1},
		{name: "negative base", base: -1, cap: time.Second, max: 1},
		{name: "zero cap", base: time.Second, cap: 0, max: 1},
		{name: "negative cap", base: time.Second, cap: -1, max: 1},
		{name: "cap below base", base: 2 * time.Second, cap: time.Second, max: 1},
		{name: "negative attempts", base: time.Second, cap: time.Second, max: -1},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewPolicy(test.base, test.cap, test.max, func() float64 { return 0.5 }); err == nil {
				t.Fatal("NewPolicy() error = nil")
			}
		})
	}

	policy, err := NewPolicy(time.Second, 8*time.Second, 3, nil)
	if err != nil {
		t.Fatalf("NewPolicy(valid) error = %v", err)
	}
	if policy.Base() != time.Second || policy.Cap() != 8*time.Second || policy.MaxAttempts(Transient) != 3 {
		t.Fatalf("NewPolicy(valid) = base %v cap %v max %d", policy.Base(), policy.Cap(), policy.MaxAttempts(Transient))
	}
	if policy.MaxAttempts(Permanent) != 0 || policy.MaxAttempts(Poison) != 0 || policy.MaxAttempts(Fatal) != 0 || policy.MaxAttempts("invalid") != 0 {
		t.Fatal("non-transient class has retry attempts")
	}

	defaultPolicy := DefaultPolicy(func() float64 { return 0.5 })
	if defaultPolicy.Base() != DefaultRetryBase || defaultPolicy.Cap() != DefaultRetryCap || defaultPolicy.MaxAttempts(Transient) != DefaultMaxTransientAttempts {
		t.Fatalf("DefaultPolicy() = base %v cap %v max %d", defaultPolicy.Base(), defaultPolicy.Cap(), defaultPolicy.MaxAttempts(Transient))
	}
}

func TestPolicyShouldRetry(t *testing.T) {
	t.Parallel()

	policy, err := NewPolicy(time.Second, time.Minute, 8, func() float64 { return 0.5 })
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		class    Class
		attempts int
		want     bool
	}{
		{class: Transient, attempts: -1, want: false},
		{class: Transient, attempts: 0, want: true},
		{class: Transient, attempts: 7, want: true},
		{class: Transient, attempts: 8, want: false},
		{class: Transient, attempts: 9, want: false},
		{class: Permanent, attempts: 0, want: false},
		{class: Poison, attempts: 0, want: false},
		{class: Fatal, attempts: 0, want: false},
		{class: "invalid", attempts: 0, want: false},
	}
	for _, test := range tests {
		if got := policy.ShouldRetry(test.class, test.attempts); got != test.want {
			t.Errorf("ShouldRetry(%q, %d) = %v, want %v", test.class, test.attempts, got, test.want)
		}
	}
	if !policy.ShouldRetryError(errors.New("unknown"), 7) {
		t.Fatal("unknown error should use the transient retry policy")
	}
	if policy.ShouldRetryError(Wrap(Permanent, errors.New("corrupt")), 0) {
		t.Fatal("permanent error should not retry")
	}

	var zero Policy
	if zero.ShouldRetry(Transient, 0) || zero.Delay(0) != 0 {
		t.Fatal("zero Policy should fail closed without panicking")
	}
}

func TestDefaultPolicyConsumedAttemptBoundaryAndBackoff(t *testing.T) {
	t.Parallel()

	policy := DefaultPolicy(func() float64 { return 0.5 }) // neutral jitter
	tests := []struct {
		attempts    int
		wantRetry   bool
		wantBackoff time.Duration
	}{
		{attempts: 1, wantRetry: true, wantBackoff: 5 * time.Second},
		{attempts: 2, wantRetry: true, wantBackoff: 10 * time.Second},
		{attempts: 7, wantRetry: true, wantBackoff: 5 * time.Second << 6},
		{attempts: 8, wantRetry: false},
	}
	for _, test := range tests {
		if got := policy.ShouldRetry(Transient, test.attempts); got != test.wantRetry {
			t.Errorf("ShouldRetry(transient, consumed attempts=%d) = %t, want %t", test.attempts, got, test.wantRetry)
		}
		if test.wantRetry {
			// Durable attempts is one-based after Claim; Delay's exponent is
			// zero-based, so the worker passes attempts-1.
			if got := policy.Delay(test.attempts - 1); got != test.wantBackoff {
				t.Errorf("Delay(attempts=%d) = %v, want %v", test.attempts, got, test.wantBackoff)
			}
		}
	}
	if policy.ShouldRetry(Permanent, 0) {
		t.Fatal("permanent errors must be terminal without a retry attempt")
	}
}

func TestPolicyDelayExponentialCapAndNextAttempt(t *testing.T) {
	t.Parallel()

	policy, err := NewPolicy(5*time.Second, 30*time.Minute, 8, func() float64 { return 0.5 })
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: -10, want: 5 * time.Second},
		{attempt: 0, want: 5 * time.Second},
		{attempt: 1, want: 10 * time.Second},
		{attempt: 8, want: 1280 * time.Second},
		{attempt: 9, want: 30 * time.Minute},
		{attempt: 1 << 30, want: 30 * time.Minute},
	}
	for _, test := range tests {
		if got := policy.Delay(test.attempt); got != test.want {
			t.Errorf("Delay(%d) = %v, want %v", test.attempt, got, test.want)
		}
	}

	now := time.Date(2026, 7, 13, 8, 0, 0, 0, time.FixedZone("test", 8*60*60))
	if got, want := policy.NextAttempt(now, 2), now.Add(20*time.Second); !got.Equal(want) || got.Location() != now.Location() {
		t.Fatalf("NextAttempt() = %v, want %v", got, want)
	}

	var zero Policy
	if got := zero.NextAttempt(now, 5); !got.Equal(now) {
		t.Fatalf("zero Policy NextAttempt() = %v, want %v", got, now)
	}
}

func TestPolicyDelayClampsJitterAndArithmetic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		random float64
		want   time.Duration
	}{
		{name: "negative infinity", random: math.Inf(-1), want: 4 * time.Second},
		{name: "below zero", random: -3, want: 4 * time.Second},
		{name: "zero", random: 0, want: 4 * time.Second},
		{name: "middle", random: 0.5, want: 5 * time.Second},
		{name: "one", random: 1, want: 6 * time.Second},
		{name: "above one", random: 3, want: 6 * time.Second},
		{name: "positive infinity", random: math.Inf(1), want: 6 * time.Second},
		{name: "nan", random: math.NaN(), want: 5 * time.Second},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			policy, err := NewPolicy(5*time.Second, 30*time.Minute, 8, func() float64 { return test.random })
			if err != nil {
				t.Fatal(err)
			}
			if got := policy.Delay(0); got != test.want {
				t.Fatalf("Delay() = %v, want %v", got, test.want)
			}
		})
	}

	preJitterCap, err := NewPolicy(5*time.Second, 6*time.Second, 1, func() float64 { return 1 })
	if err != nil {
		t.Fatal(err)
	}
	if got := preJitterCap.Delay(100); got != 7200*time.Millisecond {
		t.Fatalf("pre-jitter capped Delay() = %v, want 7.2s", got)
	}

	tiny, err := NewPolicy(time.Nanosecond, time.Nanosecond, 1, func() float64 { return 0 })
	if err != nil {
		t.Fatal(err)
	}
	if got := tiny.Delay(0); got != time.Nanosecond {
		t.Fatalf("tiny Delay() = %v, want 1ns", got)
	}

	overflow, err := NewPolicy(time.Duration(math.MaxInt64), time.Duration(math.MaxInt64), 1, func() float64 { return 1 })
	if err != nil {
		t.Fatal(err)
	}
	if got := overflow.Delay(1 << 30); got != time.Duration(math.MaxInt64) {
		t.Fatalf("overflow Delay() = %v, want MaxInt64", got)
	}

	calls := 0
	counted, err := NewPolicy(time.Second, time.Second, 1, func() float64 {
		calls++
		return 0.5
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = counted.Delay(0)
	if calls != 1 {
		t.Fatalf("random source called %d times, want 1", calls)
	}
}
