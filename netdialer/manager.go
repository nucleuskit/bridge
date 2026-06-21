package netdialer

import (
	"context"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"

	captransport "github.com/nucleuskit/cap/transport"
)

type ManagerConfig struct {
	Config Config
	Policy ConnectionPolicy
	Dialer captransport.Dialer
}

type ConnectionState string

const (
	ConnectionStateIdle       ConnectionState = "idle"
	ConnectionStateConnecting ConnectionState = "connecting"
	ConnectionStateOpen       ConnectionState = "open"
	ConnectionStateRetrying   ConnectionState = "retrying"
	ConnectionStateClosed     ConnectionState = "closed"
	ConnectionStateFailed     ConnectionState = "failed"
)

type ConnectionPolicy struct {
	MaxAttempts int
	Backoff     BackoffPolicy
	Hooks       []ManagerHook
}

type BackoffPolicy struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
	Jitter     float64
}

type ConnectionStats struct {
	Attempts    int64
	Successes   int64
	Failures    int64
	LastState   ConnectionState
	LastNetwork string
	LastAddress string
	LastAttempt int
	LastError   string
	UpdatedAt   time.Time
}

type ConnectionEvent struct {
	State   ConnectionState
	Attempt int
	Target  captransport.Target
	Err     error
	At      time.Time
}

type ManagerHook interface {
	HandleConnectionEvent(ConnectionEvent)
}

type ManagerHookFuncs struct {
	OnEvent func(ConnectionEvent)
}

func (h ManagerHookFuncs) HandleConnectionEvent(event ConnectionEvent) {
	if h.OnEvent != nil {
		h.OnEvent(event.Clone())
	}
}

type Manager struct {
	dialer captransport.Dialer
	cfg    captransport.Config
	policy ConnectionPolicy

	mu      sync.Mutex
	state   ConnectionState
	current net.Conn
	stats   ConnectionStats
	closed  bool
}

func NewManager(cfg ManagerConfig) (*Manager, error) {
	config := normalizeConfig(cfg.Config)
	dialer := cfg.Dialer
	if dialer == nil {
		var err error
		dialer, err = New(Config{Config: config})
		if err != nil {
			return nil, err
		}
	}
	policy := cfg.Policy.Clone()
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}
	return &Manager{
		dialer: dialer,
		cfg:    config,
		policy: policy,
		state:  ConnectionStateIdle,
		stats:  ConnectionStats{LastState: ConnectionStateIdle},
	}, nil
}

func (m *Manager) Connect(ctx context.Context, target captransport.Target) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	target = m.normalizeTarget(target)
	if m.isClosed() {
		return nil, captransport.ErrClosed
	}

	var lastErr error
	for attempt := 1; attempt <= m.policy.MaxAttempts; attempt++ {
		m.setState(ConnectionStateConnecting, attempt, target, nil)
		conn, err := m.dialer.DialContext(ctx, target)
		if err == nil {
			m.setCurrent(conn)
			m.recordSuccess(attempt, target)
			return conn, nil
		}
		lastErr = err
		m.recordFailure(ConnectionStateFailed, attempt, target, err)
		if attempt == m.policy.MaxAttempts {
			break
		}
		m.setState(ConnectionStateRetrying, attempt, target, err)
		if err := sleepBackoff(ctx, m.policy.Backoff, attempt); err != nil {
			m.recordFailure(ConnectionStateFailed, attempt, target, err)
			return nil, err
		}
		if m.isClosed() {
			return nil, captransport.ErrClosed
		}
	}
	return nil, lastErr
}

func (m *Manager) State() ConnectionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

func (m *Manager) Stats() ConnectionStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats.Clone()
}

func (m *Manager) Current() net.Conn {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}

func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.state = ConnectionStateClosed
	m.stats.LastState = ConnectionStateClosed
	m.stats.UpdatedAt = time.Now()
	current := m.current
	m.current = nil
	m.mu.Unlock()

	var err error
	if current != nil {
		err = current.Close()
	}
	if closer, ok := m.dialer.(captransport.Closer); ok {
		if closeErr := closer.Close(); err == nil {
			err = closeErr
		}
	}
	m.emit(ConnectionEvent{State: ConnectionStateClosed, Target: captransport.Target{}, At: time.Now()})
	return err
}

func (m *Manager) normalizeTarget(target captransport.Target) captransport.Target {
	target = target.Clone()
	if target.Network == "" {
		target.Network = m.cfg.Network
	}
	target.Network = captransport.DefaultNetwork(target.Network)
	if target.Address == "" {
		target.Address = m.cfg.Address
	}
	if target.ServerName == "" {
		target.ServerName = m.cfg.ServerName
	}
	target.Metadata = captransport.MergeMetadata(m.cfg.Metadata, target.Metadata)
	if target.TLS == nil && !isZeroTLS(m.cfg.TLS) {
		tls := m.cfg.TLS.Clone()
		target.TLS = &tls
	}
	return target
}

func (m *Manager) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *Manager) setCurrent(conn net.Conn) {
	m.mu.Lock()
	previous := m.current
	m.current = conn
	m.mu.Unlock()
	if previous != nil && previous != conn {
		_ = previous.Close()
	}
}

func (m *Manager) setState(state ConnectionState, attempt int, target captransport.Target, err error) {
	now := time.Now()
	m.mu.Lock()
	m.state = state
	m.stats.LastState = state
	m.stats.LastNetwork = target.Network
	m.stats.LastAddress = target.Address
	m.stats.LastAttempt = attempt
	m.stats.UpdatedAt = now
	if err != nil {
		m.stats.LastError = err.Error()
	}
	m.mu.Unlock()
	m.emit(ConnectionEvent{State: state, Attempt: attempt, Target: target, Err: err, At: now})
}

func (m *Manager) recordSuccess(attempt int, target captransport.Target) {
	now := time.Now()
	m.mu.Lock()
	m.state = ConnectionStateOpen
	m.stats.Attempts++
	m.stats.Successes++
	m.stats.LastState = ConnectionStateOpen
	m.stats.LastNetwork = target.Network
	m.stats.LastAddress = target.Address
	m.stats.LastAttempt = attempt
	m.stats.LastError = ""
	m.stats.UpdatedAt = now
	m.mu.Unlock()
	m.emit(ConnectionEvent{State: ConnectionStateOpen, Attempt: attempt, Target: target, At: now})
}

func (m *Manager) recordFailure(state ConnectionState, attempt int, target captransport.Target, err error) {
	now := time.Now()
	m.mu.Lock()
	m.state = state
	m.stats.Attempts++
	m.stats.Failures++
	m.stats.LastState = state
	m.stats.LastNetwork = target.Network
	m.stats.LastAddress = target.Address
	m.stats.LastAttempt = attempt
	m.stats.UpdatedAt = now
	if err != nil {
		m.stats.LastError = err.Error()
	}
	m.mu.Unlock()
	m.emit(ConnectionEvent{State: state, Attempt: attempt, Target: target, Err: err, At: now})
}

func (m *Manager) emit(event ConnectionEvent) {
	for _, hook := range m.policy.Hooks {
		if hook == nil {
			continue
		}
		hook.HandleConnectionEvent(event)
	}
}

func (p ConnectionPolicy) Clone() ConnectionPolicy {
	p.Hooks = append([]ManagerHook(nil), p.Hooks...)
	return p
}

func (s ConnectionStats) Clone() ConnectionStats {
	return s
}

func (e ConnectionEvent) Clone() ConnectionEvent {
	e.Target = e.Target.Clone()
	return e
}

func (p BackoffPolicy) Duration(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	initial := p.Initial
	if initial <= 0 {
		initial = 100 * time.Millisecond
	}
	multiplier := p.Multiplier
	if multiplier <= 0 {
		multiplier = 2
	}
	value := float64(initial)
	if attempt > 1 {
		value *= math.Pow(multiplier, float64(attempt-1))
	}
	duration := time.Duration(value)
	if p.Max > 0 && duration > p.Max {
		duration = p.Max
	}
	if p.Jitter > 0 {
		duration = withJitter(duration, p.Jitter)
	}
	return duration
}

func sleepBackoff(ctx context.Context, policy BackoffPolicy, attempt int) error {
	delay := policy.Duration(attempt)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func withJitter(duration time.Duration, ratio float64) time.Duration {
	if ratio <= 0 {
		return duration
	}
	if ratio > 1 {
		ratio = 1
	}
	spread := float64(duration) * ratio
	delta := (rand.Float64()*2 - 1) * spread
	next := time.Duration(float64(duration) + delta)
	if next < 0 {
		return 0
	}
	return next
}
