package kv

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	caphealth "github.com/nucleuskit/nucleus/cap/health"
)

var (
	ErrNotConfigured    = errors.New("kv not configured")
	ErrNotFound         = errors.New("kv key not found")
	ErrCASConflict      = errors.New("kv cas conflict")
	ErrBatchFailed      = errors.New("kv batch failed")
	ErrInvalidOperation = errors.New("kv invalid operation")
)

type OperationKind string

const (
	OperationGet    OperationKind = "get"
	OperationPut    OperationKind = "put"
	OperationDelete OperationKind = "delete"
)

type Entry struct {
	Key       string
	Value     []byte
	Version   uint64
	ExpiresAt int64
}

type Operation struct {
	Kind    OperationKind
	Key     string
	Value   []byte
	Options WriteOptions
}

type Result struct {
	Operation Operation
	Entry     Entry
	Deleted   bool
	Err       error
}

type WriteOptions struct {
	TTL             time.Duration
	ExpectedVersion uint64
	MatchVersion    bool
}

type WriteOption func(*WriteOptions)

type CASConflictError struct {
	Key      string
	Expected uint64
	Actual   uint64
}

func (e CASConflictError) Error() string {
	return fmt.Sprintf("%s: key %q expected version %d got %d", ErrCASConflict, e.Key, e.Expected, e.Actual)
}

func (e CASConflictError) Is(target error) bool {
	return target == ErrCASConflict
}

type BatchError struct {
	Results []Result
}

func (e BatchError) Error() string {
	for _, result := range e.Results {
		if result.Err != nil {
			return fmt.Sprintf("%s: %s %s: %v", ErrBatchFailed, result.Operation.Kind, result.Operation.Key, result.Err)
		}
	}
	return ErrBatchFailed.Error()
}

func (e BatchError) Is(target error) bool {
	return target == ErrBatchFailed
}

func (e BatchError) Unwrap() error {
	for _, result := range e.Results {
		if result.Err != nil {
			return result.Err
		}
	}
	return nil
}

func NewWriteOptions(options ...WriteOption) WriteOptions {
	var values WriteOptions
	for _, option := range options {
		if option != nil {
			option(&values)
		}
	}
	return values
}

func WithTTL(ttl time.Duration) WriteOption {
	return func(options *WriteOptions) {
		options.TTL = ttl
	}
}

func WithExpectedVersion(version uint64) WriteOption {
	return func(options *WriteOptions) {
		options.ExpectedVersion = version
		options.MatchVersion = true
	}
}

func NewGet(key string) Operation {
	return Operation{Kind: OperationGet, Key: key}
}

func NewPut(key string, value []byte, options ...WriteOption) Operation {
	return Operation{Kind: OperationPut, Key: key, Value: append([]byte(nil), value...), Options: NewWriteOptions(options...)}
}

func NewDelete(key string, options ...WriteOption) Operation {
	return Operation{Kind: OperationDelete, Key: key, Options: NewWriteOptions(options...)}
}

func (e Entry) Expired(now time.Time) bool {
	return e.ExpiresAt > 0 && now.UnixNano() >= e.ExpiresAt
}

func (e Entry) Clone() Entry {
	e.Value = append([]byte(nil), e.Value...)
	return e
}

func (op Operation) Clone() Operation {
	op.Value = append([]byte(nil), op.Value...)
	return op
}

func (r Result) Clone() Result {
	r.Operation = r.Operation.Clone()
	r.Entry = r.Entry.Clone()
	return r
}

type Config struct {
	Namespace string
	Now       func() time.Time
}

type Store struct {
	namespace string
	mu        sync.Mutex
	now       func() time.Time
	values    map[string]Entry
	version   uint64
	closed    bool
}

func New(cfg Config) (*Store, error) {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Store{namespace: strings.TrimSpace(cfg.Namespace), now: now, values: map[string]Entry{}}, nil
}

func (s *Store) Get(ctx context.Context, key string) (Entry, error) {
	if err := contextErr(ctx); err != nil {
		return Entry{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpenLocked(); err != nil {
		return Entry{}, err
	}
	return s.getLocked(key, s.now())
}

func (s *Store) Put(ctx context.Context, key string, value []byte, options ...WriteOption) (Entry, error) {
	if err := contextErr(ctx); err != nil {
		return Entry{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpenLocked(); err != nil {
		return Entry{}, err
	}
	return s.putLocked(key, value, NewWriteOptions(options...), s.now())
}

func (s *Store) Delete(ctx context.Context, key string, options ...WriteOption) error {
	if err := contextErr(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpenLocked(); err != nil {
		return err
	}
	return s.deleteLocked(key, NewWriteOptions(options...), s.now())
}

func (s *Store) Batch(ctx context.Context, operations ...Operation) ([]Result, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpenLocked(); err != nil {
		return nil, err
	}

	now := s.now()
	results := make([]Result, len(operations))
	for i, operation := range operations {
		result := Result{Operation: operation.Clone()}
		switch operation.Kind {
		case OperationGet:
			entry, err := s.getLocked(operation.Key, now)
			result.Entry = entry
			result.Err = err
		case OperationPut:
			entry, err := s.putLocked(operation.Key, operation.Value, operation.Options, now)
			result.Entry = entry
			result.Err = err
		case OperationDelete:
			result.Err = s.deleteLocked(operation.Key, operation.Options, now)
			result.Deleted = result.Err == nil
		default:
			result.Err = ErrInvalidOperation
		}
		results[i] = result
		if result.Err != nil {
			return cloneResults(results), BatchError{Results: cloneResults(results)}
		}
	}
	return cloneResults(results), nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *Store) getLocked(key string, now time.Time) (Entry, error) {
	storageKey := s.storageKey(key)
	entry, ok := s.values[storageKey]
	if !ok {
		return Entry{}, ErrNotFound
	}
	if entry.Expired(now) {
		delete(s.values, storageKey)
		return Entry{}, ErrNotFound
	}
	return s.publicEntry(entry), nil
}

func (s *Store) putLocked(key string, value []byte, options WriteOptions, now time.Time) (Entry, error) {
	storageKey := s.storageKey(key)
	current, exists := s.values[storageKey]
	if exists && current.Expired(now) {
		delete(s.values, storageKey)
		current = Entry{}
		exists = false
	}
	if options.MatchVersion {
		var actual uint64
		if exists {
			actual = current.Version
		}
		if actual != options.ExpectedVersion {
			return Entry{}, CASConflictError{Key: key, Expected: options.ExpectedVersion, Actual: actual}
		}
	}

	s.version++
	entry := Entry{Key: storageKey, Value: append([]byte(nil), value...), Version: s.version}
	if options.TTL > 0 {
		entry.ExpiresAt = now.Add(options.TTL).UnixNano()
	}
	s.values[storageKey] = entry
	return s.publicEntry(entry), nil
}

func (s *Store) deleteLocked(key string, options WriteOptions, now time.Time) error {
	storageKey := s.storageKey(key)
	current, exists := s.values[storageKey]
	if exists && current.Expired(now) {
		delete(s.values, storageKey)
		current = Entry{}
		exists = false
	}
	if options.MatchVersion {
		var actual uint64
		if exists {
			actual = current.Version
		}
		if actual != options.ExpectedVersion {
			return CASConflictError{Key: key, Expected: options.ExpectedVersion, Actual: actual}
		}
	}
	delete(s.values, storageKey)
	return nil
}

func (s *Store) ReportHealth(context.Context) (caphealth.Report, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := caphealth.StatusReady
	message := "kv provider ready"
	if s.closed {
		status = caphealth.StatusDown
		message = "kv provider is closed"
	}
	return caphealth.Report{
		Capability: "kv",
		Status:     status,
		Message:    message,
		Metadata: map[string]string{
			"provider":  "kv",
			"namespace": s.namespace,
			"keys":      strconv.Itoa(len(s.values)),
		},
	}, nil
}

func (s *Store) ensureOpenLocked() error {
	if s.closed {
		return ErrNotConfigured
	}
	return nil
}

func (s *Store) storageKey(key string) string {
	if s.namespace == "" {
		return key
	}
	return s.namespace + ":" + key
}

func (s *Store) publicEntry(entry Entry) Entry {
	clone := entry.Clone()
	if s.namespace != "" {
		clone.Key = strings.TrimPrefix(clone.Key, s.namespace+":")
	}
	return clone
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func cloneResults(results []Result) []Result {
	clones := make([]Result, len(results))
	for i, result := range results {
		clones[i] = result.Clone()
	}
	return clones
}
