package sentinel

import (
	"context"
	"sync"

	capsentinel "github.com/nucleuskit/cap/sentinel"
)

type Config struct {
	MaxInFlight int
	FailClosed  bool
	Policies    []capsentinel.Policy
}

type Guard struct {
	maxInFlight int
	failClosed  bool
	policies    map[string]capsentinel.Policy

	mu       sync.Mutex
	inFlight map[string]int
}

type guard struct {
	release func()
}

type permit struct {
	release func()
}

func New(cfg Config) (*Guard, error) {
	maxInFlight := cfg.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = 1
	}
	policies := make(map[string]capsentinel.Policy, len(cfg.Policies))
	for _, policy := range cfg.Policies {
		if policy.Resource == "" {
			continue
		}
		policies[policy.Resource] = policy
	}
	return &Guard{maxInFlight: maxInFlight, failClosed: cfg.FailClosed, policies: policies, inFlight: map[string]int{}}, nil
}

func (g *Guard) Allow(ctx context.Context, resource capsentinel.Resource) (capsentinel.Guard, error) {
	release, err := g.acquire(resource.Name)
	if err != nil {
		return nil, err
	}
	return guard{release: release}, nil
}

func (g *Guard) Acquire(ctx context.Context, resource capsentinel.Resource) (capsentinel.Permit, error) {
	release, err := g.acquire(resource.Name)
	if err != nil {
		return nil, err
	}
	return permit{release: release}, nil
}

func (g *Guard) Close() error {
	return nil
}

func (g *Guard) acquire(resource string) (func(), error) {
	limit, failClosed := g.policy(resource)
	g.mu.Lock()
	if g.inFlight[resource] >= limit {
		g.mu.Unlock()
		if failClosed {
			return nil, capsentinel.ErrRejected
		}
		return func() {}, nil
	}
	g.inFlight[resource]++
	g.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			if g.inFlight[resource] > 0 {
				g.inFlight[resource]--
			}
		})
	}, nil
}

func (g *Guard) policy(resource string) (int, bool) {
	policy, ok := g.policies[resource]
	if !ok {
		return g.maxInFlight, g.failClosed
	}
	limit := policy.MaxInFlight
	if limit <= 0 {
		limit = g.maxInFlight
	}
	return limit, policy.FailClosed
}

func (g guard) Done(error) {
	g.release()
}

func (p permit) Release() {
	p.release()
}
