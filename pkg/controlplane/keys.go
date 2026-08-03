package controlplane

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/lestrrat-go/jwx/v3/jwk"
)

type SigningKey struct {
	KID     string
	Private ed25519.PrivateKey
}

// ActiveSigningKey returns the key the minter signs with. Exactly one row is
// active at a time; rotation flips `active` and leaves the old row in place so
// its public half stays published and in-flight tokens keep verifying.
func (s *Store) ActiveSigningKey(ctx context.Context) (SigningKey, error) {
	var kid string
	var priv []byte
	err := s.pool.QueryRow(ctx,
		`SELECT kid, private_key FROM signing_keys WHERE active AND retired_at IS NULL LIMIT 1`).
		Scan(&kid, &priv)
	if errors.Is(err, pgx.ErrNoRows) {
		return SigningKey{}, ErrNotFound
	}
	if err != nil {
		return SigningKey{}, err
	}
	return SigningKey{KID: kid, Private: ed25519.PrivateKey(priv)}, nil
}

// PublicJWKS returns every non-retired public key, each marked use=sig, which
// is what the verifier's key-shape check requires.
func (s *Store) PublicJWKS(ctx context.Context) (jwk.Set, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT kid, public_key FROM signing_keys WHERE retired_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	set := jwk.NewSet()
	for rows.Next() {
		var kid string
		var pub []byte
		if err := rows.Scan(&kid, &pub); err != nil {
			return nil, err
		}
		key, err := jwk.Import(ed25519.PublicKey(pub))
		if err != nil {
			return nil, fmt.Errorf("import %s: %w", kid, err)
		}
		_ = key.Set(jwk.KeyIDKey, kid)
		_ = key.Set(jwk.KeyUsageKey, "sig")
		if err := set.AddKey(key); err != nil {
			return nil, err
		}
	}
	return set, rows.Err()
}

// EnsureSigningKey generates and activates a key when none exists. Called at
// startup so a fresh environment is usable without a manual provisioning step.
//
// The `WHERE NOT EXISTS` guard alone is a check-then-insert race: two
// replicas starting cold can both pass the check before either commits, and
// both insert an active key. The `signing_keys_one_active` partial unique
// index makes the second insert fail instead, and ON CONFLICT DO NOTHING
// turns that failure into "someone else already bootstrapped it" rather than
// an error the caller has to handle.
//
// The `kid` PRIMARY KEY is a second conflict target the partial-index clause
// alone does not cover: once rotation retires the row for a fixed default kid
// (e.g. SIGNING_KID's "kid-1"), that row still exists with active=false, so a
// process starting cold and trying to ensure that same kid hits the PK, not
// the partial index — an unhandled unique-violation, not a benign no-op. A
// kid that already exists, active or not, needs nothing done for it. An
// unqualified `ON CONFLICT DO NOTHING` (no target list) applies to *any*
// unique or exclusion constraint the insert could hit — both the PK and the
// partial index — so both races collapse to the same no-op instead of one of
// them surfacing as an error.
func (s *Store) EnsureSigningKey(ctx context.Context, kid string) error {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO signing_keys (kid, public_key, private_key, active)
		SELECT $1, $2, $3, true
		WHERE NOT EXISTS (SELECT 1 FROM signing_keys WHERE active AND retired_at IS NULL)
		ON CONFLICT DO NOTHING`,
		kid, []byte(pub), []byte(priv))
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	// No row inserted: either another active key already exists (the
	// intended, benign race we're guarding against), or this same kid was
	// already used by a retired row, which the WHERE NOT EXISTS clause did
	// not anticipate — that case leaves signing_keys with no active row at
	// all. Distinguish them instead of silently treating both as success.
	if _, err := s.ActiveSigningKey(ctx); err != nil {
		return fmt.Errorf("ensure signing key %q: insert conflicted and no active key exists: %w", kid, err)
	}
	return nil
}
