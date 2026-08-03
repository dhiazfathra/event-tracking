package handler

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/lestrrat-go/jwx/v3/jwk"
)

// JWKSSource is the control-plane store's public-key export. Defined here,
// not imported from pkg/controlplane, so this package's dependency graph
// stays services -> pkg — never the other way — while still typing against
// the real return type.
type JWKSSource interface {
	PublicJWKS(ctx context.Context) (jwk.Set, error)
}

// NewJWKS serves GET /.well-known/jwks.json from the given source.
//
// Both main.go and the e2e test wire this same handler against the control
// plane, so a token minted by that process always verifies against the JWKS
// it publishes — an ephemeral signing key with a separately-implemented JWKS
// endpoint is exactly the bug this shared handler forecloses. Cached briefly:
// the verifier has its own TTL, and rotation overlaps keys, so a few seconds
// of staleness is harmless.
func NewJWKS(store JWKSSource) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		set, err := store.PublicJWKS(r.Context())
		if err != nil {
			http.Error(w, "jwks unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/jwk-set+json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		_ = json.NewEncoder(w).Encode(set)
	})
}
