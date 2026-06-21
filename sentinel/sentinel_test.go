package sentinel

import (
	"context"
	"testing"

	capsentinel "github.com/nucleuskit/cap/sentinel"
)

func TestGuardImplementsSentinelCapabilityInMemory(t *testing.T) {
	guard, err := New(Config{MaxInFlight: 1, FailClosed: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = guard.Close() }()

	var breaker capsentinel.Breaker = guard
	var limiter capsentinel.Limiter = guard
	first, err := breaker.Allow(context.Background(), capsentinel.Resource{Name: "checkout"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := limiter.Acquire(context.Background(), capsentinel.Resource{Name: "checkout"}); err == nil {
		t.Fatal("expected in-flight request to be rejected")
	}
	first.Done(nil)
}

func TestGuardCanFailOpenPerPolicy(t *testing.T) {
	guard, err := New(Config{
		MaxInFlight: 1,
		FailClosed:  true,
		Policies: []capsentinel.Policy{{
			Resource:    "search",
			MaxInFlight: 1,
			FailClosed:  false,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = guard.Close() }()

	first, err := guard.Allow(context.Background(), capsentinel.Resource{Name: "search"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := guard.Allow(context.Background(), capsentinel.Resource{Name: "search"})
	if err != nil {
		t.Fatalf("expected fail-open policy to allow, got %v", err)
	}
	second.Done(nil)
	first.Done(nil)
}
