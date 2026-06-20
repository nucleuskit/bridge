package mongo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	caphealth "github.com/nucleuskit/nucleus/cap/health"
)

var (
	ErrInvalidCollection = errors.New("mongo collection is required")
	ErrInvalidID         = errors.New("mongo document id is required")
	ErrNotFound          = errors.New("mongo document not found")
	ErrConflict          = errors.New("mongo document version conflict")
)

type Document struct {
	ID        string
	Fields    map[string]any
	Version   int64
	ExpiresAt int64
	Metadata  map[string]string
}

type Filter map[string]any

type Query struct {
	Collection string
	Filter     Filter
	Limit      int
	Offset     int
}

type Patch struct {
	Set   map[string]any
	Unset []string
}

type WriteOptions struct {
	ExpectedVersion int64
	Upsert          bool
	TTL             time.Duration
}

type WriteOption func(*WriteOptions)

func WithExpectedVersion(version int64) WriteOption {
	return func(options *WriteOptions) {
		options.ExpectedVersion = version
	}
}

func WithUpsert(enabled bool) WriteOption {
	return func(options *WriteOptions) {
		options.Upsert = enabled
	}
}

func WithTTL(ttl time.Duration) WriteOption {
	return func(options *WriteOptions) {
		options.TTL = ttl
	}
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

func (d Document) Expired(now time.Time) bool {
	return d.ExpiresAt > 0 && now.UnixNano() >= d.ExpiresAt
}

func (d Document) Clone() Document {
	clone := d
	clone.Fields = cloneFields(d.Fields)
	if d.Metadata != nil {
		clone.Metadata = make(map[string]string, len(d.Metadata))
		for key, value := range d.Metadata {
			clone.Metadata[key] = value
		}
	}
	return clone
}

func (p Patch) Clone() Patch {
	return Patch{
		Set:   cloneFields(p.Set),
		Unset: append([]string(nil), p.Unset...),
	}
}

func Match(doc Document, filter Filter) bool {
	for key, want := range filter {
		var got any
		switch key {
		case "_id", "id":
			got = doc.ID
		case "_version", "version":
			got = doc.Version
		default:
			got = doc.Fields[key]
		}
		if !reflect.DeepEqual(got, want) {
			return false
		}
	}
	return true
}

func cloneFields(fields map[string]any) map[string]any {
	if fields == nil {
		return nil
	}
	clone := make(map[string]any, len(fields))
	for key, value := range fields {
		clone[key] = cloneValue(value)
	}
	return clone
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return append([]byte(nil), typed...)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneValue(item)
		}
		return out
	case map[string]any:
		return cloneFields(typed)
	case map[string]string:
		out := make(map[string]string, len(typed))
		for key, item := range typed {
			out[key] = item
		}
		return out
	default:
		return value
	}
}

type Config struct {
	Name      string
	Database  string
	Namespace string
	Clock     func() time.Time
}

type Store struct {
	name      string
	database  string
	namespace string
	clock     func() time.Time

	mu          sync.Mutex
	collections map[string]map[string]Document
	nextID      int64
	closed      bool
}

func New(cfg Config) (*Store, error) {
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	database := strings.TrimSpace(cfg.Database)
	if database == "" {
		database = "default"
	}
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		name = database
	}
	return &Store{
		name:        name,
		database:    database,
		namespace:   strings.TrimSpace(cfg.Namespace),
		clock:       clock,
		collections: map[string]map[string]Document{},
	}, nil
}

func (s *Store) Insert(ctx context.Context, collection string, doc Document, options ...WriteOption) (Document, error) {
	if err := ctxErr(ctx); err != nil {
		return Document{}, err
	}
	key, err := s.collectionKey(collection)
	if err != nil {
		return Document{}, err
	}
	values := NewWriteOptions(options...)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpenLocked(); err != nil {
		return Document{}, err
	}
	s.pruneLocked(s.clock())
	collectionValues := s.ensureCollectionLocked(key)
	doc = doc.Clone()
	if strings.TrimSpace(doc.ID) == "" {
		s.nextID++
		doc.ID = strconv.FormatInt(s.nextID, 10)
	}
	if _, exists := collectionValues[doc.ID]; exists {
		return Document{}, fmt.Errorf("%w: %s", ErrConflict, doc.ID)
	}
	doc.Version = 1
	applyTTL(&doc, values, s.clock())
	collectionValues[doc.ID] = doc.Clone()
	return doc.Clone(), nil
}

func (s *Store) Get(ctx context.Context, collection string, id string) (Document, error) {
	if err := ctxErr(ctx); err != nil {
		return Document{}, err
	}
	key, err := s.collectionKey(collection)
	if err != nil {
		return Document{}, err
	}
	if strings.TrimSpace(id) == "" {
		return Document{}, ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpenLocked(); err != nil {
		return Document{}, err
	}
	now := s.clock()
	s.pruneLocked(now)
	doc, ok := s.collections[key][id]
	if !ok {
		return Document{}, ErrNotFound
	}
	if doc.Expired(now) {
		delete(s.collections[key], id)
		return Document{}, ErrNotFound
	}
	return doc.Clone(), nil
}

func (s *Store) Find(ctx context.Context, query Query) ([]Document, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	key, err := s.collectionKey(query.Collection)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpenLocked(); err != nil {
		return nil, err
	}
	now := s.clock()
	s.pruneLocked(now)
	values := s.collections[key]
	result := make([]Document, 0, len(values))
	skipped := 0
	for id, doc := range values {
		if doc.Expired(now) {
			delete(values, id)
			continue
		}
		if !Match(doc, query.Filter) {
			continue
		}
		if query.Offset > 0 && skipped < query.Offset {
			skipped++
			continue
		}
		result = append(result, doc.Clone())
		if query.Limit > 0 && len(result) >= query.Limit {
			break
		}
	}
	return result, nil
}

func (s *Store) Replace(ctx context.Context, collection string, doc Document, options ...WriteOption) (Document, error) {
	if err := ctxErr(ctx); err != nil {
		return Document{}, err
	}
	key, err := s.collectionKey(collection)
	if err != nil {
		return Document{}, err
	}
	if strings.TrimSpace(doc.ID) == "" {
		return Document{}, ErrInvalidID
	}
	values := NewWriteOptions(options...)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpenLocked(); err != nil {
		return Document{}, err
	}
	s.pruneLocked(s.clock())
	collectionValues := s.ensureCollectionLocked(key)
	current, exists := collectionValues[doc.ID]
	if !exists && !values.Upsert {
		return Document{}, ErrNotFound
	}
	if exists && values.ExpectedVersion > 0 && current.Version != values.ExpectedVersion {
		return Document{}, fmt.Errorf("%w: expected %d got %d", ErrConflict, values.ExpectedVersion, current.Version)
	}
	doc = doc.Clone()
	if exists {
		doc.Version = current.Version + 1
	} else {
		doc.Version = 1
	}
	applyTTL(&doc, values, s.clock())
	collectionValues[doc.ID] = doc.Clone()
	return doc.Clone(), nil
}

func (s *Store) Update(ctx context.Context, collection string, id string, patch Patch, options ...WriteOption) (Document, error) {
	if err := ctxErr(ctx); err != nil {
		return Document{}, err
	}
	key, err := s.collectionKey(collection)
	if err != nil {
		return Document{}, err
	}
	if strings.TrimSpace(id) == "" {
		return Document{}, ErrInvalidID
	}
	values := NewWriteOptions(options...)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpenLocked(); err != nil {
		return Document{}, err
	}
	s.pruneLocked(s.clock())
	collectionValues := s.ensureCollectionLocked(key)
	doc, exists := collectionValues[id]
	if !exists {
		if !values.Upsert {
			return Document{}, ErrNotFound
		}
		doc = Document{ID: id, Fields: map[string]any{}, Version: 0}
	}
	if exists && values.ExpectedVersion > 0 && doc.Version != values.ExpectedVersion {
		return Document{}, fmt.Errorf("%w: expected %d got %d", ErrConflict, values.ExpectedVersion, doc.Version)
	}
	next := doc.Clone()
	if next.Fields == nil {
		next.Fields = map[string]any{}
	}
	patch = patch.Clone()
	for key, value := range patch.Set {
		next.Fields[key] = value
	}
	for _, key := range patch.Unset {
		delete(next.Fields, key)
	}
	next.Version = doc.Version + 1
	if next.Version == 0 {
		next.Version = 1
	}
	applyTTL(&next, values, s.clock())
	collectionValues[id] = next.Clone()
	return next.Clone(), nil
}

func (s *Store) Delete(ctx context.Context, collection string, id string) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	key, err := s.collectionKey(collection)
	if err != nil {
		return err
	}
	if strings.TrimSpace(id) == "" {
		return ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpenLocked(); err != nil {
		return err
	}
	delete(s.collections[key], id)
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *Store) ReportHealth(context.Context) (caphealth.Report, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := caphealth.StatusReady
	message := "mongo document provider ready"
	if s.closed {
		status = caphealth.StatusDown
		message = "mongo document provider is closed"
	}
	return caphealth.Report{
		Capability: "mongo",
		Status:     status,
		Message:    message,
		Metadata: map[string]string{
			"provider":    "mongo",
			"name":        s.name,
			"database":    s.database,
			"namespace":   s.namespace,
			"collections": strconv.Itoa(len(s.collections)),
		},
	}, nil
}

func (s *Store) collectionKey(collection string) (string, error) {
	collection = strings.TrimSpace(collection)
	if collection == "" {
		return "", ErrInvalidCollection
	}
	parts := []string{s.database, collection}
	if s.namespace != "" {
		parts = []string{s.namespace, s.database, collection}
	}
	return strings.Join(parts, "/"), nil
}

func (s *Store) ensureCollectionLocked(key string) map[string]Document {
	values := s.collections[key]
	if values == nil {
		values = map[string]Document{}
		s.collections[key] = values
	}
	return values
}

func (s *Store) ensureOpenLocked() error {
	if s.closed {
		return errors.New("mongo document provider is closed")
	}
	return nil
}

func (s *Store) pruneLocked(now time.Time) {
	for collection, values := range s.collections {
		for id, doc := range values {
			if doc.Expired(now) {
				delete(values, id)
			}
		}
		if len(values) == 0 {
			delete(s.collections, collection)
		}
	}
}

func applyTTL(doc *Document, options WriteOptions, now time.Time) {
	if options.TTL > 0 {
		doc.ExpiresAt = now.Add(options.TTL).UnixNano()
	}
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
