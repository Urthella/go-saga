package saga

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type order struct {
	ID       string
	Reserved bool
	ChargeID string
	Shipped  bool
}

// shop records every call and lets a test choose how each step behaves.
type shop struct {
	mu    sync.Mutex
	calls []string
	keys  map[string][]string
	fail  map[string]func(attempt int) error // per call name, by attempt number
}

func newShop() *shop { return &shop{keys: map[string][]string{}, fail: map[string]func(int) error{}} }

func (s *shop) call(name, key string) error {
	s.mu.Lock()
	s.calls = append(s.calls, name)
	s.keys[name] = append(s.keys[name], key)
	attempt := len(s.keys[name])
	f := s.fail[name]
	s.mu.Unlock()
	if f != nil {
		return f(attempt)
	}
	return nil
}

func (s *shop) saga(log Log) *Saga[order] {
	return &Saga[order]{
		Log:   log,
		Retry: Retry{Attempts: 3, Backoff: time.Millisecond},
		Steps: []Step[order]{
			{
				Name:       "reserve",
				Do:         func(_ context.Context, o *order, k string) error { o.Reserved = true; return s.call("reserve", k) },
				Compensate: func(_ context.Context, o *order, k string) error { o.Reserved = false; return s.call("release", k) },
			},
			{
				Name: "charge",
				Do: func(_ context.Context, o *order, k string) error {
					o.ChargeID = "ch_" + o.ID
					return s.call("charge", k)
				},
				Compensate: func(_ context.Context, o *order, k string) error { o.ChargeID = ""; return s.call("refund", k) },
			},
			{
				Name: "ship",
				Do:   func(_ context.Context, o *order, k string) error { o.Shipped = true; return s.call("ship", k) },
			},
		},
	}
}

func always(err error) func(int) error { return func(int) error { return err } }

func TestCompletes(t *testing.T) {
	s := newShop()
	o := &order{ID: "o1"}
	res, err := s.saga(nil).Run(context.Background(), "o1", o)
	if err != nil || res.Status != Completed || res.FailedStep != "" {
		t.Fatalf("%+v %v", res, err)
	}
	if want := []string{"reserve", "charge", "ship"}; !reflect.DeepEqual(s.calls, want) {
		t.Errorf("calls %v, want %v", s.calls, want)
	}
	if !o.Reserved || o.ChargeID != "ch_o1" || !o.Shipped {
		t.Errorf("data %+v", o)
	}
}

func TestCompensatesInReverseIncludingTheFailedStep(t *testing.T) {
	s := newShop()
	s.fail["charge"] = always(Permanent(errors.New("card declined")))
	o := &order{ID: "o2"}
	res, err := s.saga(nil).Run(context.Background(), "o2", o)
	if err != nil || res.Status != Compensated || res.FailedStep != "charge" || res.Err.Error() != "card declined" {
		t.Fatalf("%+v %v", res, err)
	}
	// The refund runs too: a charge that timed out may still have gone through.
	if want := []string{"reserve", "charge", "refund", "release"}; !reflect.DeepEqual(s.calls, want) {
		t.Errorf("calls %v, want %v", s.calls, want)
	}
	if o.Reserved || o.ChargeID != "" || o.Shipped {
		t.Errorf("not undone: %+v", o)
	}
}

func TestRetriesWithTheSameIdempotencyKey(t *testing.T) {
	s := newShop()
	s.fail["ship"] = func(attempt int) error {
		if attempt < 3 {
			return errors.New("carrier timeout")
		}
		return nil
	}
	res, err := s.saga(nil).Run(context.Background(), "o3", &order{ID: "o3"})
	if err != nil || res.Status != Completed {
		t.Fatalf("%+v %v", res, err)
	}
	if want := []string{"o3/ship", "o3/ship", "o3/ship"}; !reflect.DeepEqual(s.keys["ship"], want) {
		t.Errorf("ship keys %v", s.keys["ship"])
	}
}

func TestPermanentErrorsAreNotRetried(t *testing.T) {
	s := newShop()
	s.fail["charge"] = always(Permanent(errors.New("card declined")))
	if _, err := s.saga(nil).Run(context.Background(), "o4", &order{}); err != nil {
		t.Fatal(err)
	}
	if n := len(s.keys["charge"]); n != 1 {
		t.Errorf("charge tried %d times", n)
	}
}

func TestStuckWhenACompensationKeepsFailing(t *testing.T) {
	s := newShop()
	s.fail["ship"] = always(Permanent(errors.New("address invalid")))
	s.fail["release"] = always(errors.New("inventory service down"))
	res, err := s.saga(nil).Run(context.Background(), "o5", &order{})
	if err != nil || res.Status != Stuck || !reflect.DeepEqual(res.StuckSteps, []string{"reserve"}) {
		t.Fatalf("%+v %v", res, err)
	}
	if len(s.keys["refund"]) != 1 || len(s.keys["release"]) != 3 {
		t.Errorf("refund %d, release %d; every other compensation must still run", len(s.keys["refund"]), len(s.keys["release"]))
	}
}

// blockOnce makes the first call to a step hang until the test cancels the saga, like a crash.
func blockOnce(cancel context.CancelFunc) func(int) error {
	return func(attempt int) error {
		if attempt == 1 {
			cancel()
			return context.Canceled
		}
		return nil
	}
}

func TestResumesForwardAfterAnInterruption(t *testing.T) {
	s, log := newShop(), NewMemoryLog()
	ctx, cancel := context.WithCancel(context.Background())
	s.fail["charge"] = blockOnce(cancel)
	if _, err := s.saga(log).Run(ctx, "o6", &order{ID: "o6"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted run: %v", err)
	}

	// A new process: fresh data, same log.
	o := &order{}
	res, err := s.saga(log).Run(context.Background(), "o6", o)
	if err != nil || res.Status != Completed {
		t.Fatalf("%+v %v", res, err)
	}
	if want := []string{"reserve", "charge", "charge", "ship"}; !reflect.DeepEqual(s.calls, want) {
		t.Errorf("calls %v: reserve must not run twice", s.calls)
	}
	if !o.Reserved || o.ChargeID != "ch_o6" || o.ID != "o6" {
		t.Errorf("data not restored from the log: %+v", o)
	}
}

func TestResumesMidCompensation(t *testing.T) {
	s, log := newShop(), NewMemoryLog()
	ctx, cancel := context.WithCancel(context.Background())
	s.fail["ship"] = always(Permanent(errors.New("address invalid")))
	s.fail["release"] = blockOnce(cancel)
	if _, err := s.saga(log).Run(ctx, "o7", &order{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted run: %v", err)
	}
	res, err := s.saga(log).Run(context.Background(), "o7", &order{})
	if err != nil || res.Status != Compensated || res.FailedStep != "ship" || res.Err.Error() != "address invalid" {
		t.Fatalf("%+v %v", res, err)
	}
	if want := []string{"reserve", "charge", "ship", "refund", "release", "release"}; !reflect.DeepEqual(s.calls, want) {
		t.Errorf("calls %v: no Do may rerun, and the refund must not repeat", s.calls)
	}
}

func TestRunningAFinishedSagaReturnsItsResult(t *testing.T) {
	s, log := newShop(), NewMemoryLog()
	s.fail["charge"] = always(Permanent(errors.New("card declined")))
	first, _ := s.saga(log).Run(context.Background(), "o8", &order{})
	calls := len(s.calls)
	again, err := s.saga(log).Run(context.Background(), "o8", &order{})
	if err != nil || again.Status != first.Status || again.FailedStep != "charge" || len(s.calls) != calls {
		t.Fatalf("second run %+v, %d new calls", again, len(s.calls)-calls)
	}
}
