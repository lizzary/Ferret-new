// Package embed contains dependency-agnostic coordination for remote embedding
// stages. The actual compute transport is deliberately supplied by later
// pipeline wiring; this package does not depend on gRPC.
package embed

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrOpen means that the dependency call was rejected by the breaker.
	ErrOpen = errors.New("embed breaker: dependency is unavailable")
	// ErrProbeInFlight is returned when a half-open probe already owns the only
	// admission slot. The returned error matches both this sentinel and ErrOpen.
	ErrProbeInFlight = errors.New("embed breaker: half-open probe is already in flight")
	// ErrInvalidConfig identifies a breaker/controller configuration that cannot
	// provide the required safety guarantees.
	ErrInvalidConfig = errors.New("embed breaker: invalid configuration")
)

// State is the externally visible circuit-breaker state. The numeric order is
// also the metrics contract: closed=0, half-open=1, open=2.
type State uint8

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (state State) String() string {
	switch state {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half_open"
	case StateOpen:
		return "open"
	default:
		return "unknown"
	}
}

// MetricValue returns the breaker_state gauge contract used by obs.Metrics.
func (state State) MetricValue() float64 { return float64(state) }

// Timer is the subset of time.Timer needed by Controller.Run.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

// Clock makes cooldown transitions deterministic in tests and avoids sleeping
// in state-machine tests.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTimer(duration time.Duration) Timer {
	return systemTimer{Timer: time.NewTimer(duration)}
}

type systemTimer struct{ *time.Timer }

func (timer systemTimer) C() <-chan time.Time { return timer.Timer.C }

// Config maps directly from compute.breaker.failures/open_for. ReleaseBatch
// tunes waiting_dep coordination; its zero value selects a conservative
// default. IsFailure lets a future transport exclude application errors that
// prove the dependency is reachable.
type Config struct {
	Failures     int
	OpenFor      time.Duration
	ReleaseBatch int
	Clock        Clock
	IsFailure    func(error) bool
}

const defaultReleaseBatch = 1000

func normalizeConfig(config Config) (Config, error) {
	if config.Failures <= 0 {
		return Config{}, fmt.Errorf("%w: failures must be positive", ErrInvalidConfig)
	}
	if config.OpenFor <= 0 {
		return Config{}, fmt.Errorf("%w: open_for must be positive", ErrInvalidConfig)
	}
	if config.ReleaseBatch == 0 {
		config.ReleaseBatch = defaultReleaseBatch
	}
	if config.ReleaseBatch < 0 {
		return Config{}, fmt.Errorf("%w: release batch must not be negative", ErrInvalidConfig)
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	if config.IsFailure == nil {
		config.IsFailure = func(err error) bool { return err != nil }
	}
	return config, nil
}

// Snapshot is an atomic view of the breaker. Epoch changes whenever a state
// transition invalidates permits issued by the preceding state.
type Snapshot struct {
	State               State
	ConsecutiveFailures int
	OpenUntil           time.Time
	ProbeInFlight       bool
	Epoch               uint64
}

// Transition describes the state-machine effect of completing a Permit.
// Accepted is false for a late completion from an older epoch.
type Transition struct {
	From     State
	To       State
	Changed  bool
	Accepted bool
}

// Breaker is a concurrency-safe consecutive-failure circuit breaker. Calls
// admitted while closed may execute concurrently. After OpenFor, the first
// caller alone receives a half-open probe permit.
type Breaker struct {
	mu sync.Mutex

	failures int
	openFor  time.Duration
	clock    Clock

	state               State
	consecutiveFailures int
	openUntil           time.Time
	probeInFlight       bool
	epoch               uint64
}

// NewBreaker constructs a closed breaker. Only Failures, OpenFor and Clock are
// used; accepting Config here keeps lifecycle mapping to compute.breaker
// identical to NewController.
func NewBreaker(config Config) (*Breaker, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	return &Breaker{
		failures: normalized.Failures,
		openFor:  normalized.OpenFor,
		clock:    normalized.Clock,
		state:    StateClosed,
	}, nil
}

// Allow admits a normal call or the unique half-open probe. Every returned
// permit must be completed with Success, Failure, or Abort. Prefer
// Controller.Execute, which guarantees completion even when the call panics.
func (breaker *Breaker) Allow() (*Permit, error) {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()

	breaker.advanceLocked(breaker.clock.Now())
	switch breaker.state {
	case StateClosed:
		return &Permit{breaker: breaker, epoch: breaker.epoch}, nil
	case StateOpen:
		return nil, ErrOpen
	case StateHalfOpen:
		if breaker.probeInFlight {
			return nil, probeInFlightError{}
		}
		breaker.probeInFlight = true
		return &Permit{breaker: breaker, epoch: breaker.epoch, probe: true}, nil
	default:
		return nil, ErrOpen
	}
}

// Snapshot returns a coherent state view. Reaching the cooldown deadline is a
// lazy Open -> HalfOpen transition, so no private timer goroutine is required.
func (breaker *Breaker) Snapshot() Snapshot {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	breaker.advanceLocked(breaker.clock.Now())
	return Snapshot{
		State: breaker.state, ConsecutiveFailures: breaker.consecutiveFailures,
		OpenUntil: breaker.openUntil, ProbeInFlight: breaker.probeInFlight,
		Epoch: breaker.epoch,
	}
}

// State returns the current state, applying a due cooldown transition.
func (breaker *Breaker) State() State { return breaker.Snapshot().State }

// withHalfOpenIdle linearizes durable probe-candidate release with Allow. The
// callback runs while the breaker lock is held, so either the candidate is
// released first or an already in-flight probe prevents the release. The
// callback must not call back into the breaker.
func (breaker *Breaker) withHalfOpenIdle(epoch uint64, release func() (int64, error)) (int64, bool, error) {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	breaker.advanceLocked(breaker.clock.Now())
	if breaker.state != StateHalfOpen || breaker.epoch != epoch || breaker.probeInFlight {
		return 0, false, nil
	}
	released, err := release()
	return released, true, err
}

func (breaker *Breaker) advanceLocked(now time.Time) {
	if breaker.state != StateOpen || now.Before(breaker.openUntil) {
		return
	}
	breaker.state = StateHalfOpen
	breaker.probeInFlight = false
	breaker.epoch++
}

type completion uint8

const (
	completionSuccess completion = iota
	completionFailure
	completionAbort
)

func (breaker *Breaker) complete(epoch uint64, probe bool, result completion) Transition {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()

	from := breaker.state
	transition := Transition{From: from, To: from}
	if epoch != breaker.epoch {
		return transition
	}

	switch breaker.state {
	case StateClosed:
		if probe {
			return transition
		}
		transition.Accepted = true
		switch result {
		case completionSuccess:
			breaker.consecutiveFailures = 0
		case completionFailure:
			breaker.consecutiveFailures++
			if breaker.consecutiveFailures >= breaker.failures {
				breaker.openLocked(breaker.clock.Now())
			}
		case completionAbort:
			// Caller cancellation is not dependency health evidence and does
			// not break a consecutive sequence established by other calls.
		}
	case StateHalfOpen:
		if !probe || !breaker.probeInFlight {
			return transition
		}
		transition.Accepted = true
		switch result {
		case completionSuccess:
			breaker.state = StateClosed
			breaker.consecutiveFailures = 0
			breaker.openUntil = time.Time{}
			breaker.probeInFlight = false
			breaker.epoch++
		case completionFailure, completionAbort:
			// An abandoned half-open probe cannot retain the only slot forever.
			// Reopening is conservative and starts a fresh full cooldown.
			breaker.openLocked(breaker.clock.Now())
		}
	}
	transition.To = breaker.state
	transition.Changed = transition.From != transition.To
	return transition
}

func (breaker *Breaker) openLocked(now time.Time) {
	breaker.state = StateOpen
	breaker.openUntil = now.Add(breaker.openFor)
	breaker.probeInFlight = false
	breaker.epoch++
}

// Permit represents one admitted dependency call. Completion is idempotent and
// safe even if defensive cleanup races normal completion.
type Permit struct {
	breaker *Breaker
	epoch   uint64
	probe   bool

	once       sync.Once
	transition Transition
}

// Success reports that the dependency was reachable and healthy.
func (permit *Permit) Success() Transition {
	return permit.finish(completionSuccess)
}

// Failure reports a dependency-health failure.
func (permit *Permit) Failure() Transition {
	return permit.finish(completionFailure)
}

// Abort releases the permit without treating caller cancellation as a normal
// dependency failure. A half-open abort reopens the breaker so another probe
// can be attempted after a fresh cooldown.
func (permit *Permit) Abort() Transition {
	return permit.finish(completionAbort)
}

func (permit *Permit) finish(result completion) Transition {
	if permit == nil || permit.breaker == nil {
		return Transition{}
	}
	permit.once.Do(func() {
		permit.transition = permit.breaker.complete(permit.epoch, permit.probe, result)
	})
	return permit.transition
}

type probeInFlightError struct{}

func (probeInFlightError) Error() string { return ErrProbeInFlight.Error() }
func (probeInFlightError) Is(target error) bool {
	return target == ErrOpen || target == ErrProbeInFlight
}
