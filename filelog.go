package saga

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// FileLog keeps one JSON-lines file per saga in a directory, synced to disk on every append,
// so sagas resume after a process restart. Run a given saga ID from one process at a time.
type FileLog struct {
	dir string
	mu  sync.Mutex
}

func NewFileLog(dir string) (*FileLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileLog{dir: dir}, nil
}

func (l *FileLog) Append(_ context.Context, id string, e Event) error {
	line, err := json.Marshal(e)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	path := l.path(id)
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil { // durable before Append returns, as Log requires
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if errors.Is(statErr, os.ErrNotExist) {
		syncDir(l.dir) // make the new file's directory entry durable too
	}
	return nil
}

func (l *FileLog) Events(_ context.Context, id string) ([]Event, error) {
	data, err := os.ReadFile(l.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// A crash mid-append can leave a last line without its newline. That event was never
	// acknowledged, so it's dropped; a broken line anywhere else is real corruption.
	if i := bytes.LastIndexByte(data, '\n'); i < len(data)-1 {
		data = data[:i+1]
	}
	var events []Event
	for n, line := range bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("%s line %d: %w", l.path(id), n+1, err)
		}
		events = append(events, e)
	}
	return events, nil
}

// path maps any saga ID to a safe, unique file name: letters, digits, '.', '-' and '_' stay,
// every other byte becomes %XX, so "orders/2026:42" can't escape the directory or collide.
func (l *FileLog) path(id string) string {
	var b strings.Builder
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || (c == '.' && i > 0) {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return filepath.Join(l.dir, b.String()+".jsonl")
}

// syncDir is best effort: Windows can't fsync a directory, and the file itself is already synced.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
