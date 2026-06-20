package netdialer

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	captransport "github.com/nucleuskit/nucleus/cap/transport"
)

type Config struct {
	Network    string
	Address    string
	ServerName string
	Timeout    captransport.TimeoutConfig
	TLS        captransport.TLSConfig
	Proxy      captransport.ProxyConfig
	Metadata   captransport.Metadata
	Hooks      []captransport.DialHook
	Config     captransport.Config
}

type Dialer struct {
	cfg captransport.Config

	mu     sync.Mutex
	closed bool
	stats  captransport.Stats
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func New(cfg Config) (*Dialer, error) {
	config := normalizeConfig(cfg)
	return &Dialer{cfg: config}, nil
}

func (d *Dialer) DialContext(ctx context.Context, target captransport.Target) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if d.isClosed() {
		return nil, captransport.ErrClosed
	}
	target = d.normalizeTarget(target)
	event := d.newEvent(target)
	ctx, event = d.before(ctx, event)

	started := time.Now()
	conn, proxy, err := d.dial(ctx, target)
	event.Duration = time.Since(started)
	event.Proxy = proxy
	event.Err = err
	d.record(event, err)
	d.after(ctx, event)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (d *Dialer) Stats() captransport.Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stats.Clone()
}

func (d *Dialer) Close() error {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	return nil
}

func (d *Dialer) dial(ctx context.Context, target captransport.Target) (net.Conn, string, error) {
	if strings.TrimSpace(target.Address) == "" {
		return nil, "", captransport.ErrMissingTarget
	}
	base := net.Dialer{
		Timeout:   d.cfg.Timeout.Dial,
		KeepAlive: d.cfg.Timeout.KeepAlive,
	}

	var (
		conn  net.Conn
		proxy string
		err   error
	)
	if proxyURL := d.proxyURL(ctx, target); proxyURL != nil {
		proxy = proxyURL.String()
		conn, err = d.dialHTTPProxy(ctx, base, proxyURL, target)
	} else {
		conn, err = base.DialContext(ctx, captransport.DefaultNetwork(target.Network), target.Address)
	}
	if err != nil {
		return nil, proxy, err
	}

	tlsConfig, enabled, err := d.tlsConfig(target)
	if err != nil {
		_ = conn.Close()
		return nil, proxy, err
	}
	if !enabled {
		return conn, proxy, nil
	}

	tlsConn := tls.Client(conn, tlsConfig)
	handshakeCtx := ctx
	cancel := func() {}
	if d.cfg.Timeout.TLSHandshake > 0 {
		handshakeCtx, cancel = context.WithTimeout(ctx, d.cfg.Timeout.TLSHandshake)
	}
	defer cancel()
	if err := tlsConn.HandshakeContext(handshakeCtx); err != nil {
		_ = conn.Close()
		return nil, proxy, err
	}
	return tlsConn, proxy, nil
}

func (d *Dialer) dialHTTPProxy(ctx context.Context, base net.Dialer, proxyURL *url.URL, target captransport.Target) (net.Conn, error) {
	proxyAddress := hostPort(proxyURL)
	if proxyAddress == "" {
		return nil, errors.New("transport proxy address is empty")
	}
	conn, err := base.DialContext(ctx, "tcp", proxyAddress)
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(proxyURL.Scheme, "https") {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: stripPort(proxyAddress)})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, err
		}
		conn = tlsConn
	}

	if err := writeConnect(conn, proxyURL, target.Address, d.cfg.Proxy.Headers); err != nil {
		_ = conn.Close()
		return nil, err
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = conn.Close()
		return nil, fmt.Errorf("transport proxy CONNECT failed: %s", response.Status)
	}
	if reader.Buffered() > 0 {
		return &bufferedConn{Conn: conn, reader: reader}, nil
	}
	return conn, nil
}

func (d *Dialer) proxyURL(ctx context.Context, target captransport.Target) *url.URL {
	if d.cfg.Proxy.URL != "" {
		parsed, err := url.Parse(d.cfg.Proxy.URL)
		if err == nil && parsed.Host != "" {
			return parsed
		}
		return nil
	}
	if !d.cfg.Proxy.FromEnvironment {
		return nil
	}
	scheme := "http"
	if tlsPolicy := d.effectiveTLS(target); tlsPolicy.Enabled {
		scheme = "https"
	}
	req := (&http.Request{URL: &url.URL{Scheme: scheme, Host: target.Address}}).WithContext(ctx)
	proxyURL, err := http.ProxyFromEnvironment(req)
	if err != nil {
		return nil
	}
	return proxyURL
}

func (d *Dialer) tlsConfig(target captransport.Target) (*tls.Config, bool, error) {
	policy := d.effectiveTLS(target)
	if !policy.Enabled {
		return nil, false, nil
	}
	minVersion, err := tlsVersion(policy.MinVersion)
	if err != nil {
		return nil, false, err
	}
	maxVersion, err := tlsVersion(policy.MaxVersion)
	if err != nil {
		return nil, false, err
	}
	serverName := policy.ServerName
	if serverName == "" {
		serverName = target.ServerName
	}
	if serverName == "" {
		serverName = d.cfg.ServerName
	}
	if serverName == "" {
		serverName = stripPort(target.Address)
	}
	return &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: policy.InsecureSkipVerify,
		MinVersion:         minVersion,
		MaxVersion:         maxVersion,
		NextProtos:         append([]string(nil), policy.NextProtos...),
	}, true, nil
}

func (d *Dialer) effectiveTLS(target captransport.Target) captransport.TLSConfig {
	if target.TLS != nil {
		return target.TLS.Clone()
	}
	return d.cfg.TLS.Clone()
}

func (d *Dialer) normalizeTarget(target captransport.Target) captransport.Target {
	target = target.Clone()
	if target.Network == "" {
		target.Network = d.cfg.Network
	}
	target.Network = captransport.DefaultNetwork(target.Network)
	if target.Address == "" {
		target.Address = d.cfg.Address
	}
	if target.ServerName == "" {
		target.ServerName = d.cfg.ServerName
	}
	target.Metadata = captransport.MergeMetadata(d.cfg.Metadata, target.Metadata)
	return target
}

func (d *Dialer) newEvent(target captransport.Target) captransport.DialEvent {
	tlsPolicy := d.effectiveTLS(target)
	return captransport.DialEvent{
		Network:   target.Network,
		Address:   target.Address,
		TLS:       tlsPolicy.Enabled,
		Metadata:  captransport.CloneMetadata(target.Metadata),
		StartedAt: time.Now(),
	}
}

func (d *Dialer) before(ctx context.Context, event captransport.DialEvent) (context.Context, captransport.DialEvent) {
	for _, hook := range d.cfg.Hooks {
		if hook == nil {
			continue
		}
		next := hook.BeforeDial(ctx, event)
		if next != nil {
			ctx = next
		}
	}
	return ctx, event
}

func (d *Dialer) after(ctx context.Context, event captransport.DialEvent) {
	for _, hook := range d.cfg.Hooks {
		if hook == nil {
			continue
		}
		hook.AfterDial(ctx, event)
	}
}

func (d *Dialer) record(event captransport.DialEvent, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats.Dials++
	d.stats.LastNetwork = event.Network
	d.stats.LastAddress = event.Address
	d.stats.LastProxy = event.Proxy
	d.stats.LastDuration = event.Duration
	d.stats.LastError = ""
	if event.TLS && err == nil {
		d.stats.TLSHandshakes++
	}
	if event.Proxy != "" {
		d.stats.ProxyDials++
	}
	if err != nil {
		d.stats.Errors++
		d.stats.LastError = err.Error()
		return
	}
	d.stats.Successes++
}

func (d *Dialer) isClosed() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closed
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	if c.reader != nil && c.reader.Buffered() > 0 {
		return c.reader.Read(p)
	}
	return c.Conn.Read(p)
}

func normalizeConfig(cfg Config) captransport.Config {
	config := cfg.Config.Clone()
	if cfg.Network != "" {
		config.Network = cfg.Network
	}
	if cfg.Address != "" {
		config.Address = cfg.Address
	}
	if cfg.ServerName != "" {
		config.ServerName = cfg.ServerName
	}
	if cfg.Timeout != (captransport.TimeoutConfig{}) {
		config.Timeout = cfg.Timeout
	}
	if !isZeroTLS(cfg.TLS) {
		config.TLS = cfg.TLS.Clone()
	}
	if cfg.Proxy.URL != "" || cfg.Proxy.FromEnvironment || len(cfg.Proxy.Headers) > 0 {
		config.Proxy = cfg.Proxy.Clone()
	}
	config.Metadata = captransport.MergeMetadata(config.Metadata, cfg.Metadata)
	config.Hooks = append(config.Hooks, cfg.Hooks...)
	config.Network = captransport.DefaultNetwork(config.Network)
	return config.Clone()
}

func tlsVersion(version string) (uint16, error) {
	switch strings.ToLower(strings.TrimSpace(version)) {
	case "":
		return 0, nil
	case "1.0", "tls1.0", "tls10":
		return tls.VersionTLS10, nil
	case "1.1", "tls1.1", "tls11":
		return tls.VersionTLS11, nil
	case "1.2", "tls1.2", "tls12":
		return tls.VersionTLS12, nil
	case "1.3", "tls1.3", "tls13":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("unsupported TLS version %q", version)
	}
}

func isZeroTLS(cfg captransport.TLSConfig) bool {
	return !cfg.Enabled &&
		cfg.ServerName == "" &&
		!cfg.InsecureSkipVerify &&
		cfg.MinVersion == "" &&
		cfg.MaxVersion == "" &&
		len(cfg.NextProtos) == 0
}

func writeConnect(conn net.Conn, proxyURL *url.URL, targetAddress string, headers map[string]string) error {
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n", targetAddress, targetAddress); err != nil {
		return err
	}
	if proxyURL.User != nil {
		if _, err := fmt.Fprintf(conn, "Proxy-Authorization: Basic %s\r\n", proxyAuth(proxyURL)); err != nil {
			return err
		}
	}
	for key, value := range headers {
		if strings.ContainsAny(key, "\r\n:") || strings.ContainsAny(value, "\r\n") {
			continue
		}
		if _, err := fmt.Fprintf(conn, "%s: %s\r\n", key, value); err != nil {
			return err
		}
	}
	_, err := fmt.Fprint(conn, "\r\n")
	return err
}

func proxyAuth(proxyURL *url.URL) string {
	username := proxyURL.User.Username()
	password, _ := proxyURL.User.Password()
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
}

func hostPort(parsed *url.URL) string {
	host := parsed.Host
	if host == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https":
		return net.JoinHostPort(host, "443")
	default:
		return net.JoinHostPort(host, "80")
	}
}

func stripPort(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err == nil {
		return host
	}
	return address
}
