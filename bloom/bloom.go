package bloom

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sync"
)

const DefaultFalsePositiveRate = 0.01

var ErrInvalidOptions = errors.New("invalid bloom options")

type Config struct {
	Namespace         string
	Capacity          uint64
	FalsePositiveRate float64
	Bits              uint64
	Hashes            uint64
	Seed              uint64
}

type Stats struct {
	Namespace         string
	Capacity          uint64
	FalsePositiveRate float64
	Bits              uint64
	Hashes            uint64
	Added             uint64
}

type Filter struct {
	namespace         string
	capacity          uint64
	falsePositiveRate float64
	bits              uint64
	hashes            uint64
	seed              uint64

	mu    sync.RWMutex
	words []uint64
	added uint64
}

func New(cfg Config) (*Filter, error) {
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	bits, hashes := cfg.Bits, cfg.Hashes
	if bits == 0 && hashes == 0 {
		bits, hashes = size(cfg.Capacity, cfg.FalsePositiveRate)
	}
	if bits == 0 || hashes == 0 {
		return nil, fmt.Errorf("%w: calculated bits and hashes must be greater than zero", ErrInvalidOptions)
	}
	return &Filter{
		namespace:         cfg.Namespace,
		capacity:          cfg.Capacity,
		falsePositiveRate: cfg.FalsePositiveRate,
		bits:              bits,
		hashes:            hashes,
		seed:              cfg.Seed,
		words:             make([]uint64, (bits+63)/64),
	}, nil
}

func (f *Filter) Add(ctx context.Context, key string) error {
	if err := checkContextAndKey(ctx, key); err != nil {
		return err
	}
	positions := f.positions(key)
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, position := range positions {
		f.setBit(position)
	}
	f.added++
	return nil
}

func (f *Filter) Contains(ctx context.Context, key string) (bool, error) {
	if err := checkContextAndKey(ctx, key); err != nil {
		return false, err
	}
	positions := f.positions(key)
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, position := range positions {
		if !f.hasBit(position) {
			return false, nil
		}
	}
	return true, nil
}

func (f *Filter) TestAndAdd(ctx context.Context, key string) (bool, error) {
	if err := checkContextAndKey(ctx, key); err != nil {
		return false, err
	}
	positions := f.positions(key)
	f.mu.Lock()
	defer f.mu.Unlock()
	existed := true
	for _, position := range positions {
		if !f.hasBit(position) {
			existed = false
		}
	}
	for _, position := range positions {
		f.setBit(position)
	}
	f.added++
	return existed, nil
}

func (f *Filter) Reset(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.words {
		f.words[i] = 0
	}
	f.added = 0
	return nil
}

func (f *Filter) Stats(ctx context.Context) (Stats, error) {
	if err := ctx.Err(); err != nil {
		return Stats{}, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return Stats{
		Namespace:         f.namespace,
		Capacity:          f.capacity,
		FalsePositiveRate: f.falsePositiveRate,
		Bits:              f.bits,
		Hashes:            f.hashes,
		Added:             f.added,
	}, nil
}

func (f *Filter) positions(key string) []uint64 {
	positions := make([]uint64, f.hashes)
	first := hash(f.seed, f.namespace, key, 0)
	second := hash(f.seed, f.namespace, key, 1) | 1
	for i := uint64(0); i < f.hashes; i++ {
		positions[i] = (first + i*second) % f.bits
	}
	return positions
}

func (f *Filter) setBit(position uint64) {
	f.words[position/64] |= 1 << (position % 64)
}

func (f *Filter) hasBit(position uint64) bool {
	return f.words[position/64]&(1<<(position%64)) != 0
}

func size(capacity uint64, falsePositiveRate float64) (uint64, uint64) {
	n := float64(capacity)
	m := math.Ceil(-(n * math.Log(falsePositiveRate)) / (math.Ln2 * math.Ln2))
	k := math.Ceil((m / n) * math.Ln2)
	return uint64(m), uint64(k)
}

func hash(seed uint64, namespace, key string, salt uint64) uint64 {
	hasher := fnv.New64a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], seed)
	_, _ = hasher.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], salt)
	_, _ = hasher.Write(buf[:])
	_, _ = hasher.Write([]byte(namespace))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write([]byte(key))
	return hasher.Sum64()
}

func checkContextAndKey(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("%w: key must not be empty", ErrInvalidOptions)
	}
	return nil
}

func (o *Config) ApplyDefaults() {
	if o.FalsePositiveRate == 0 {
		o.FalsePositiveRate = DefaultFalsePositiveRate
	}
}

func (o Config) Validate() error {
	if o.Capacity == 0 {
		return fmt.Errorf("%w: capacity must be greater than zero", ErrInvalidOptions)
	}
	if o.FalsePositiveRate <= 0 || o.FalsePositiveRate >= 1 {
		return fmt.Errorf("%w: false positive rate must be between zero and one", ErrInvalidOptions)
	}
	if (o.Bits == 0) != (o.Hashes == 0) {
		return fmt.Errorf("%w: bits and hashes must be configured together", ErrInvalidOptions)
	}
	return nil
}
