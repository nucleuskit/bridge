package security

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	capauth "github.com/nucleuskit/nucleus/cap/auth"
)

const (
	HeaderSignature = "X-Nucleus-Signature"
	HeaderTimestamp = "X-Nucleus-Timestamp"
	HeaderKeyID     = "X-Nucleus-Key-Id"
	SchemeSignature = "Signature"
)

var ErrUnauthenticated = errors.New("unauthenticated")

type Signature struct {
	KeyID     string
	Timestamp string
	Value     string
	Algorithm string
}

type Config struct {
	Secrets map[string]string
	Skew    time.Duration
	Now     func() time.Time
}

type HMACAuthenticator struct {
	secrets map[string]string
	skew    time.Duration
	now     func() time.Time
}

func NewHMACAuthenticator(cfg Config) *HMACAuthenticator {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	skew := cfg.Skew
	if skew <= 0 {
		skew = 5 * time.Minute
	}
	secrets := make(map[string]string, len(cfg.Secrets))
	for key, value := range cfg.Secrets {
		secrets[key] = value
	}
	return &HMACAuthenticator{secrets: secrets, skew: skew, now: now}
}

func (a *HMACAuthenticator) Authenticate(ctx context.Context, credentials capauth.Credentials) (capauth.Principal, error) {
	if err := ctx.Err(); err != nil {
		return capauth.Principal{}, err
	}
	signature := SignatureFromHeaders(credentials.Header)
	secret := a.secrets[signature.KeyID]
	if secret == "" || signature.Value == "" {
		return capauth.Principal{}, ErrUnauthenticated
	}
	timestamp, err := strconv.ParseInt(signature.Timestamp, 10, 64)
	if err != nil {
		return capauth.Principal{}, ErrUnauthenticated
	}
	at := time.Unix(timestamp, 0)
	if delta := a.now().Sub(at); delta > a.skew || delta < -a.skew {
		return capauth.Principal{}, ErrUnauthenticated
	}
	expected := signValue(signature.KeyID, secret, signature.Timestamp, canonicalMessage(credentials))
	if !hmac.Equal([]byte(expected), []byte(signature.Value)) {
		return capauth.Principal{}, ErrUnauthenticated
	}
	principal := capauth.PrincipalFromClaims(credentials.Claims)
	if principal.Claims == nil {
		principal.Claims = map[string]any{}
	}
	principal.Claims["key_id"] = signature.KeyID
	return principal, nil
}

func SignCredentials(credentials capauth.Credentials, keyID string, secret string, at time.Time) capauth.Credentials {
	if credentials.Header == nil {
		credentials.Header = map[string][]string{}
	}
	timestamp := strconv.FormatInt(at.Unix(), 10)
	credentials.Header[HeaderKeyID] = []string{keyID}
	credentials.Header[HeaderTimestamp] = []string{timestamp}
	credentials.Header[HeaderSignature] = []string{signValue(keyID, secret, timestamp, canonicalMessage(credentials))}
	return credentials
}

func SignatureFromHeaders(headers map[string][]string) Signature {
	value := headerValue(headers, HeaderSignature)
	algorithm := SchemeSignature
	if prefix, rest, ok := strings.Cut(value, " "); ok {
		algorithm = strings.TrimSpace(prefix)
		value = strings.TrimSpace(rest)
	}
	return Signature{
		KeyID:     headerValue(headers, HeaderKeyID),
		Timestamp: headerValue(headers, HeaderTimestamp),
		Value:     value,
		Algorithm: algorithm,
	}
}

func signValue(keyID string, secret string, timestamp string, message string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(keyID))
	_, _ = mac.Write([]byte("\n"))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("\n"))
	_, _ = mac.Write([]byte(message))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func canonicalMessage(credentials capauth.Credentials) string {
	method := claimString(credentials.Claims, "method")
	path := claimString(credentials.Claims, "path")
	bodyHash := claimString(credentials.Claims, "body_sha256")
	return strings.ToUpper(method) + "\n" + path + "\n" + bodyHash
}

func claimString(claims map[string]any, key string) string {
	value, ok := claims[key]
	if !ok {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func headerValue(headers map[string][]string, key string) string {
	if len(headers) == 0 || strings.TrimSpace(key) == "" {
		return ""
	}
	if values, ok := headers[key]; ok {
		if value := firstHeader(values); value != "" {
			return value
		}
	}
	for candidate, values := range headers {
		if strings.EqualFold(candidate, key) {
			return firstHeader(values)
		}
	}
	return ""
}

func firstHeader(values []string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
