package embed

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

type manualTimer struct {
	ch      chan time.Time
	resets  chan time.Duration
	stopped atomic.Bool
}

func (timer *manualTimer) C() <-chan time.Time { return timer.ch }
func (timer *manualTimer) Stop() bool {
	return !timer.stopped.Swap(true)
}
func (timer *manualTimer) Reset(duration time.Duration) bool {
	wasStopped := timer.stopped.Swap(false)
	select {
	case timer.resets <- duration:
	default:
	}
	return !wasStopped
}

type manualClock struct {
	mu      sync.Mutex
	now     time.Time
	timer   *manualTimer
	created chan struct{}
	once    sync.Once
	invalid bool
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{now: now, created: make(chan struct{})}
}

func (clock *manualClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *manualClock) NewTimer(time.Duration) Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if clock.invalid {
		clock.once.Do(func() { close(clock.created) })
		return nil
	}
	clock.timer = &manualTimer{
		ch: make(chan time.Time, 16), resets: make(chan time.Duration, 16),
	}
	clock.once.Do(func() { close(clock.created) })
	return clock.timer
}

func (clock *manualClock) Timer() *manualTimer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.timer
}

func (clock *manualClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

func (clock *manualClock) AdvanceAndFire(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	now := clock.now
	timer := clock.timer
	clock.mu.Unlock()
	if timer != nil {
		timer.ch <- now
	}
}

func TestNewBreakerValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config Config
		valid  bool
	}{
		{name: "missing failures", config: Config{OpenFor: time.Second}},
		{name: "negative failures", config: Config{Failures: -1, OpenFor: time.Second}},
		{name: "missing cooldown", config: Config{Failures: 1}},
		{name: "negative release batch", config: Config{Failures: 1, OpenFor: time.Second, ReleaseBatch: -1}},
		{name: "compute breaker mapping", config: Config{Failures: 5, OpenFor: 30 * time.Second}, valid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			breaker, err := NewBreaker(test.config)
			if test.valid && (err != nil || breaker == nil) {
				t.Fatalf("NewBreaker() = %v, %v", breaker, err)
			}
			if !test.valid && !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewBreaker() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestBreakerConsecutiveFailureTable(t *testing.T) {
	t.Parallel()
	clock := newManualClock(time.Unix(100, 0))
	breaker, err := NewBreaker(Config{Failures: 3, OpenFor: 10 * time.Second, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name         string
		success      bool
		wantState    State
		wantFailures int
	}{
		{name: "first failure", wantState: StateClosed, wantFailures: 1},
		{name: "success resets sequence", success: true, wantState: StateClosed, wantFailures: 0},
		{name: "new sequence one", wantState: StateClosed, wantFailures: 1},
		{name: "new sequence two", wantState: StateClosed, wantFailures: 2},
		{name: "threshold opens", wantState: StateOpen, wantFailures: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			permit, allowErr := breaker.Allow()
			if allowErr != nil {
				t.Fatalf("Allow() error = %v", allowErr)
			}
			if test.success {
				permit.Success()
			} else {
				permit.Failure()
			}
			snapshot := breaker.Snapshot()
			if snapshot.State != test.wantState || snapshot.ConsecutiveFailures != test.wantFailures {
				t.Fatalf("snapshot = %+v, want state=%s failures=%d", snapshot, test.wantState, test.wantFailures)
			}
		})
	}

	if _, err := breaker.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow(open) error = %v, want ErrOpen", err)
	}
	if got := StateOpen.MetricValue(); got != 2 {
		t.Fatalf("StateOpen.MetricValue() = %v", got)
	}
}

func TestBreakerHalfOpenSingleProbeAndFreshCooldown(t *testing.T) {
	t.Parallel()
	clock := newManualClock(time.Unix(200, 0))
	breaker, _ := NewBreaker(Config{Failures: 1, OpenFor: time.Minute, Clock: clock})
	first, _ := breaker.Allow()
	opened := first.Failure()
	if !opened.Changed || opened.To != StateOpen {
		t.Fatalf("opening transition = %+v", opened)
	}

	clock.Advance(time.Minute)
	if state := breaker.State(); state != StateHalfOpen {
		t.Fatalf("state after cooldown = %s", state)
	}
	probe, err := breaker.Allow()
	if err != nil {
		t.Fatalf("Allow(probe) error = %v", err)
	}
	if _, err := breaker.Allow(); !errors.Is(err, ErrOpen) || !errors.Is(err, ErrProbeInFlight) {
		t.Fatalf("Allow(second probe) error = %v", err)
	}

	now := clock.Now()
	reopened := probe.Failure()
	snapshot := breaker.Snapshot()
	if !reopened.Changed || snapshot.State != StateOpen || !snapshot.OpenUntil.Equal(now.Add(time.Minute)) {
		t.Fatalf("reopened = %+v snapshot=%+v", reopened, snapshot)
	}
	clock.Advance(time.Minute - time.Nanosecond)
	if state := breaker.State(); state != StateOpen {
		t.Fatalf("state before fresh deadline = %s", state)
	}
	clock.Advance(time.Nanosecond)
	probe, err = breaker.Allow()
	if err != nil {
		t.Fatalf("Allow(second-cycle probe) error = %v", err)
	}
	closed := probe.Success()
	if !closed.Changed || closed.To != StateClosed || breaker.State() != StateClosed {
		t.Fatalf("closing transition = %+v state=%s", closed, breaker.State())
	}
}

func TestBreakerIgnoresLatePermitsFromOlderEpoch(t *testing.T) {
	t.Parallel()
	clock := newManualClock(time.Unix(300, 0))
	breaker, _ := NewBreaker(Config{Failures: 1, OpenFor: time.Second, Clock: clock})
	openingPermit, _ := breaker.Allow()
	latePermit, _ := breaker.Allow()
	openingPermit.Failure()
	clock.Advance(time.Second)
	probe, _ := breaker.Allow()
	probe.Success()

	transition := latePermit.Failure()
	if transition.Accepted || transition.Changed {
		t.Fatalf("late transition = %+v", transition)
	}
	snapshot := breaker.Snapshot()
	if snapshot.State != StateClosed || snapshot.ConsecutiveFailures != 0 {
		t.Fatalf("late permit changed snapshot: %+v", snapshot)
	}
}

func TestBreakerConcurrentHalfOpenAdmission(t *testing.T) {
	t.Parallel()
	clock := newManualClock(time.Unix(400, 0))
	breaker, _ := NewBreaker(Config{Failures: 1, OpenFor: time.Second, Clock: clock})
	permit, _ := breaker.Allow()
	permit.Failure()
	clock.Advance(time.Second)

	const callers = 128
	start := make(chan struct{})
	permits := make(chan *Permit, callers)
	errorsSeen := make(chan error, callers)
	var group errgroup.Group
	for range callers {
		group.Go(func() error {
			<-start
			candidate, err := breaker.Allow()
			if err != nil {
				errorsSeen <- err
				return nil
			}
			permits <- candidate
			return nil
		})
	}
	close(start)
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	close(permits)
	close(errorsSeen)

	var admitted []*Permit
	for candidate := range permits {
		admitted = append(admitted, candidate)
	}
	if len(admitted) != 1 {
		t.Fatalf("half-open admitted %d probes, want 1", len(admitted))
	}
	for err := range errorsSeen {
		if !errors.Is(err, ErrProbeInFlight) {
			t.Errorf("rejected error = %v", err)
		}
	}
	admitted[0].Success()
}

func TestPermitCompletionIsIdempotentUnderConcurrency(t *testing.T) {
	t.Parallel()
	breaker, _ := NewBreaker(Config{Failures: 1, OpenFor: time.Second})
	permit, _ := breaker.Allow()
	var group errgroup.Group
	for i := range 64 {
		if i%2 == 0 {
			group.Go(func() error { permit.Success(); return nil })
		} else {
			group.Go(func() error { permit.Failure(); return nil })
		}
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	state := breaker.State()
	if state != StateClosed && state != StateOpen {
		t.Fatalf("state after idempotent completion = %s", state)
	}
}
