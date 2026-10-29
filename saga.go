// Package saga runs business transactions that span several services. Every step has a
// compensating action; when a step fails for good, the steps are undone in reverse order.
// Progress goes to a Log after every step, so a saga cut short by a crash or a deploy picks up
// where it stopped when it is run again with the same ID.
package saga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Step[T any] struct {
	Name string
	// Do performs the step. key is the same for this saga and step across retries and restarts,
	// so the service on the other end can use it to deduplicate.
	Do func(ctx context.Context, data *T, key string) error
	// Compensate undoes Do. It also runs for the step that failed, because a call that timed out
	// may still have taken effect, so it must be a no-op when there is nothing to undo.
	// nil means the step has nothing to undo.
	Compensate func(ctx context.Context, data *T, key string) error
}

type Saga[T any] struct {
	Steps []Step[T]
	Log   Log // where progress is recorded; a MemoryLog if nil
	Retry Retry
}

// Retry applies to every Do and Compensate call.
type Retry struct {
	Attempts   int           // tries per call, including the first; default 3
	Backoff    time.Duration // first wait, doubling after each try; default 100ms
	MaxBackoff time.Duration // default 5s
}

type Status string

const (
	Completed   Status = "completed"
	Compensated Status = "compensated" // a step failed and everything was undone
	Stuck       Status = "stuck"       // a compensation kept failing; a human has to look
)

type Result struct {
	Status     Status
	FailedStep string   // the step whose failure started the compensation
	Err        error    // why that step failed
	StuckSteps []string // steps whose compensation failed for good
}

// Permanent marks an error that retrying can't fix, like a declined card, so the saga
// starts compensating at once.
func Permanent(err error) error { return permanentError{err} }

type permanentError struct{ error }

func (p permanentError) Unwrap() error { return p.error }

// Run executes the saga with the given ID, or resumes it from its log. Running a finished saga
// again just returns its result. If ctx ends mid-step, Run returns ctx's error without recording
// a failure; run it again with the same ID to continue.
func (s *Saga[T]) Run(ctx context.Context, id string, data *T) (Result, error) {
	if s.Log == nil {
		s.Log = NewMemoryLog()
	}
	events, err := s.Log.Events(ctx, id)
	if err != nil {
		return Result{}, fmt.Errorf("saga %s: reading log: %w", id, err)
	}

	r := run[T]{saga: s, ctx: ctx, id: id, data: data, next: 0, failed: -1, undone: map[int]bool{}}
	for _, e := range events {
		if len(e.Data) > 0 {
			if err := json.Unmarshal(e.Data, data); err != nil {
				return Result{}, fmt.Errorf("saga %s: restoring data: %w", id, err)
			}
		}
		switch e.Kind {
		case StepDone:
			r.next = e.Step + 1
		case StepFailed:
			r.failed, r.failErr = e.Step, errors.New(e.Error)
		case CompensationDone:
			r.undone[e.Step] = true
		case CompensationFailed:
			r.undone[e.Step] = true
			r.stuck = append(r.stuck, s.Steps[e.Step].Name)
		case Finished:
			return r.result(e.Status), nil
		}
	}

	if r.failed < 0 {
		for i := r.next; i < len(s.Steps); i++ {
			err := r.retry(func() error { return s.Steps[i].Do(ctx, data, r.key(i)) })
			if err != nil && ctx.Err() != nil { // interrupted, not failed: leave it for the next Run
				return Result{}, ctx.Err()
			}
			if err != nil {
				r.failed, r.failErr = i, err
				if err := r.record(StepFailed, i, err); err != nil {
					return Result{}, err
				}
				break
			}
			if err := r.record(StepDone, i, nil); err != nil {
				return Result{}, err
			}
		}
		if r.failed < 0 {
			return r.finish(Completed)
		}
	}

	for i := r.failed; i >= 0; i-- {
		undo := s.Steps[i].Compensate
		if undo == nil || r.undone[i] {
			continue
		}
		err := r.retry(func() error { return undo(ctx, data, r.key(i)) })
		if err != nil && ctx.Err() != nil {
			return Result{}, ctx.Err()
		}
		kind := CompensationDone
		if err != nil {
			kind = CompensationFailed
			r.stuck = append(r.stuck, s.Steps[i].Name)
		}
		if err := r.record(kind, i, err); err != nil {
			return Result{}, err
		}
	}
	if len(r.stuck) > 0 {
		return r.finish(Stuck)
	}
	return r.finish(Compensated)
}

// run is the state of one Run call, rebuilt from the log on resume.
type run[T any] struct {
	saga    *Saga[T]
	ctx     context.Context
	id      string
	data    *T
	next    int          // first step not yet done
	failed  int          // step that failed, -1 while going forward
	failErr error        // its error
	undone  map[int]bool // compensations already attempted
	stuck   []string
}

func (r *run[T]) key(step int) string { return r.id + "/" + r.saga.Steps[step].Name }

func (r *run[T]) record(kind EventKind, step int, cause error) error {
	data, err := json.Marshal(r.data)
	if err != nil {
		return fmt.Errorf("saga %s: encoding data: %w", r.id, err)
	}
	e := Event{Kind: kind, Step: step, Data: data, At: time.Now().UTC()}
	if cause != nil {
		e.Error = cause.Error()
	}
	if err := r.saga.Log.Append(r.ctx, r.id, e); err != nil {
		return fmt.Errorf("saga %s: writing log: %w", r.id, err)
	}
	return nil
}

func (r *run[T]) finish(status Status) (Result, error) {
	e := Event{Kind: Finished, Step: -1, Status: status, At: time.Now().UTC()}
	if err := r.saga.Log.Append(r.ctx, r.id, e); err != nil {
		return Result{}, fmt.Errorf("saga %s: writing log: %w", r.id, err)
	}
	return r.result(status), nil
}

func (r *run[T]) result(status Status) Result {
	res := Result{Status: status, Err: r.failErr, StuckSteps: r.stuck}
	if r.failed >= 0 {
		res.FailedStep = r.saga.Steps[r.failed].Name
	}
	return res
}

// retry calls f until it succeeds, returns a Permanent error, runs out of attempts, or ctx ends.
func (r *run[T]) retry(f func() error) error {
	p := r.saga.Retry
	attempts, wait, ceiling := p.Attempts, p.Backoff, p.MaxBackoff
	if attempts <= 0 {
		attempts = 3
	}
	if wait <= 0 {
		wait = 100 * time.Millisecond
	}
	if ceiling <= 0 {
		ceiling = 5 * time.Second
	}
	var err error
	for try := 1; ; try++ {
		if err = f(); err == nil || try == attempts || errors.As(err, new(permanentError)) {
			return err
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(wait):
		}
		wait = min(wait*2, ceiling)
	}
}
