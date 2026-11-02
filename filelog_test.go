package saga

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestFileLogResumesAfterARestart(t *testing.T) {
	dir := t.TempDir()
	s := newShop()
	ctx, cancel := context.WithCancel(context.Background())
	s.fail["charge"] = blockOnce(cancel)
	log1, err := NewFileLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.saga(log1).Run(ctx, "orders/2026:42", &order{ID: "42"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted run: %v", err)
	}

	// A new process: a fresh FileLog on the same directory, fresh data.
	log2, _ := NewFileLog(dir)
	o := &order{}
	res, err := s.saga(log2).Run(context.Background(), "orders/2026:42", o)
	if err != nil || res.Status != Completed {
		t.Fatalf("%+v %v", res, err)
	}
	if want := []string{"reserve", "charge", "charge", "ship"}; !reflect.DeepEqual(s.calls, want) {
		t.Errorf("calls %v", s.calls)
	}
	if o.ID != "42" || !o.Reserved {
		t.Errorf("data not restored: %+v", o)
	}
}

func TestFileLogDropsATornLastLineButNotCorruption(t *testing.T) {
	l, _ := NewFileLog(t.TempDir())
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := l.Append(ctx, "s", Event{Kind: StepDone, Step: i}); err != nil {
			t.Fatal(err)
		}
	}
	f, _ := os.OpenFile(l.path("s"), os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.WriteString(`{"kind":"step_do`) // the process died mid-write
	_ = f.Close()
	events, err := l.Events(ctx, "s")
	if err != nil || len(events) != 2 || events[1].Step != 1 {
		t.Fatalf("torn line: %d events, %v", len(events), err)
	}

	data, _ := os.ReadFile(l.path("s"))
	_ = os.WriteFile(l.path("s"), []byte("garbage\n"+string(data)), 0o644)
	if _, err := l.Events(ctx, "s"); err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("corruption must be an error, got %v", err)
	}
}

func TestFileLogNamesAreSafeAndDistinct(t *testing.T) {
	l, _ := NewFileLog(t.TempDir())
	seen := map[string]string{}
	for _, id := range []string{"a/b", "a%2Fb", "a:b", "../etc/passwd", ".hidden", "plain-id_1.2"} {
		p := l.path(id)
		if filepath.Dir(p) != l.dir {
			t.Errorf("%q escapes the log directory: %s", id, p)
		}
		if other, dup := seen[p]; dup {
			t.Errorf("%q and %q share %s", id, other, p)
		}
		seen[p] = id
	}
	if got := filepath.Base(l.path("plain-id_1.2")); got != "plain-id_1.2.jsonl" {
		t.Errorf("readable IDs should stay readable: %s", got)
	}
	if events, err := l.Events(context.Background(), "never-ran"); err != nil || events != nil {
		t.Errorf("unknown saga: %v %v", events, err)
	}
}
