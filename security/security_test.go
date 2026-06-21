package security

import (
	"context"
	"errors"
	"testing"
	"time"

	capauth "github.com/nucleuskit/cap/auth"
)

func TestHMACAuthenticatorAcceptsSignedCredentials(t *testing.T) {
	authenticator := NewHMACAuthenticator(Config{
		Secrets: map[string]string{"app": "secret"},
		Now:     func() time.Time { return time.Unix(100, 0) },
		Skew:    time.Minute,
	})
	credentials := SignCredentials(capauth.Credentials{
		Claims: map[string]any{
			capauth.ClaimSubject: "client-1",
			"method":             "POST",
			"path":               "/orders",
			"body_sha256":        "abc",
		},
	}, "app", "secret", time.Unix(100, 0))

	principal, err := authenticator.Authenticate(context.Background(), credentials)
	if err != nil {
		t.Fatal(err)
	}
	if principal.Subject != "client-1" || principal.Claims["key_id"] != "app" {
		t.Fatalf("unexpected principal: %#v", principal)
	}
}

func TestHMACAuthenticatorRejectsBadSignature(t *testing.T) {
	authenticator := NewHMACAuthenticator(Config{Secrets: map[string]string{"app": "secret"}})
	credentials := capauth.Credentials{Header: map[string][]string{
		HeaderSignature: {"bad"},
		HeaderTimestamp: {"100"},
		HeaderKeyID:     {"app"},
	}}
	_, err := authenticator.Authenticate(context.Background(), credentials)
	if !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("expected ErrUnauthenticated, got %v", err)
	}
}
