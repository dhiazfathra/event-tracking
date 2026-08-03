package main

import (
	"encoding/json"
	"net/http"

	"github.com/dhiazfathra/event-tracking/pkg/controlplane"
)

// newJWKSHandler publishes the public half of every non-retired signing key.
//
// Serving this from the same process that mints is what makes an exchanged
// token verifiable at /v1/batch. Cached briefly: the verifier has its own TTL,
// and rotation overlaps keys, so a few seconds of staleness is harmless.
func newJWKSHandler(store *controlplane.Store) http.Handler {
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
