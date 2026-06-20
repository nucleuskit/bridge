package bloom

import (
	"context"
	"reflect"
	"testing"
)

func TestFilterImplementsBloomFilter(t *testing.T) {
	filter, err := New(Config{Capacity: 1000, FalsePositiveRate: 0.01, Seed: 7})
	if err != nil {
		t.Fatal(err)
	}

	if err := filter.Add(context.Background(), "user:1"); err != nil {
		t.Fatal(err)
	}

	ok, err := filter.Contains(context.Background(), "user:1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected added key to be present")
	}
}

func TestFilterTestAndAdd(t *testing.T) {
	filter, err := New(Config{Capacity: 1000, FalsePositiveRate: 0.01})
	if err != nil {
		t.Fatal(err)
	}

	existed, err := filter.TestAndAdd(context.Background(), "missing")
	if err != nil {
		t.Fatal(err)
	}
	if existed {
		t.Fatal("expected first TestAndAdd to report missing key")
	}

	existed, err = filter.TestAndAdd(context.Background(), "missing")
	if err != nil {
		t.Fatal(err)
	}
	if !existed {
		t.Fatal("expected second TestAndAdd to report existing key")
	}
}

func TestFilterNamespaceAndSeedAreRepeatable(t *testing.T) {
	alpha, err := New(Config{Namespace: "alpha", Capacity: 100, FalsePositiveRate: 0.01, Seed: 123})
	if err != nil {
		t.Fatal(err)
	}
	alphaAgain, err := New(Config{Namespace: "alpha", Capacity: 100, FalsePositiveRate: 0.01, Seed: 123})
	if err != nil {
		t.Fatal(err)
	}
	beta, err := New(Config{Namespace: "beta", Capacity: 100, FalsePositiveRate: 0.01, Seed: 123})
	if err != nil {
		t.Fatal(err)
	}

	if err := alpha.Add(context.Background(), "same-key"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(alpha.positions("same-key"), alphaAgain.positions("same-key")) {
		t.Fatal("expected same namespace and seed to produce repeatable bit positions")
	}
	if reflect.DeepEqual(alpha.positions("same-key"), beta.positions("same-key")) {
		t.Fatal("expected different namespace to produce different bit positions")
	}
}

func TestFilterDoesNotReturnFalseNegative(t *testing.T) {
	filter, err := New(Config{Capacity: 10000, FalsePositiveRate: 0.001, Seed: 99})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 500; i++ {
		key := string(rune('a'+i%26)) + ":" + string(rune(i))
		if err := filter.Add(context.Background(), key); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 500; i++ {
		key := string(rune('a'+i%26)) + ":" + string(rune(i))
		ok, err := filter.Contains(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("expected no false negative for %q", key)
		}
	}
}

func TestFilterResetAndStats(t *testing.T) {
	filter, err := New(Config{Capacity: 100, FalsePositiveRate: 0.01})
	if err != nil {
		t.Fatal(err)
	}
	if err := filter.Add(context.Background(), "key"); err != nil {
		t.Fatal(err)
	}

	stats, err := filter.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Added != 1 || stats.Bits == 0 || stats.Hashes == 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	if err := filter.Reset(context.Background()); err != nil {
		t.Fatal(err)
	}
	ok, err := filter.Contains(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected reset to clear filter")
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	if _, err := New(Config{Capacity: 0, FalsePositiveRate: 0.01}); err == nil {
		t.Fatal("expected invalid config error")
	}
}
