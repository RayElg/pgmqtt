//! Postgres-role-backed MQTT password authentication.
//!
//! Reads the SCRAM-SHA-256 verifier stored in `pg_authid.rolpassword` for the
//! claimed role, then runs the SCRAM client-side derivation on the plaintext
//! password from the CONNECT packet and compares `StoredKey` in constant time.
//!
//! Verifier format (per Postgres docs):
//!   `SCRAM-SHA-256$<iter>:<b64salt>$<b64StoredKey>:<b64ServerKey>`
//!
//! Derivation (RFC 5802):
//!   SaltedPassword = PBKDF2-HMAC-SHA256(password, salt, iter, 32)
//!   ClientKey      = HMAC-SHA-256(SaltedPassword, "Client Key")
//!   StoredKey      = SHA-256(ClientKey)
//!
//! `md5`-prefixed verifiers are explicitly rejected — admins must use SCRAM.
//!
//! Passwords are compared as raw bytes. Postgres applies SASLprep (RFC 4013)
//! before hashing; for ASCII-only passwords this is a no-op. Non-ASCII
//! passwords are not portable across this path.

use base64::Engine;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::spi::Spi;
use ring::{constant_time, digest, hmac, pbkdf2};
use std::collections::HashMap;
use std::num::NonZeroU32;

// Dummy verifier params used on no-role / bad-verifier paths so that every
// call to `verify` pays the same PBKDF2 cost regardless of whether the role
// exists or has a valid SCRAM verifier. 4096 matches Postgres' default
// `scram_iterations`. The dummy StoredKey is all-zeros — a real PBKDF2 output
// has 1-in-2^256 odds of matching, so the compare reliably fails on this path.
const DUMMY_SCRAM_SALT: &[u8] = b"pgmqtt-dummy-scram-salt----0000\0";
const DUMMY_SCRAM_ITERATIONS: u32 = 4096;
const DUMMY_SCRAM_STORED_KEY: [u8; 32] = [0u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthOutcome {
    /// Username and password match the SCRAM verifier in `pg_authid`.
    Ok,
    /// Role does not exist, has `rolcanlogin = false`, or `rolvaliduntil` has passed.
    BadRole,
    /// Verifier is missing, malformed, or uses an unsupported scheme (e.g. md5).
    BadVerifier,
    /// Verifier parsed cleanly but the supplied password does not match.
    BadPassword,
    /// Username failed the `password_auth_role_filter` GUC pattern.
    FilteredOut,
    /// SPI lookup failed; treat as an authentication failure but log loudly.
    LookupError,
}

/// Verify the supplied (username, password) against `pg_authid`. Honors the
/// optional `password_auth_role_filter` LIKE pattern.
///
/// Timing: every code path runs exactly one `lookup_auth` SPI roundtrip and
/// one `compute_stored_key` (PBKDF2 + HMAC + SHA-256) against either the real
/// verifier or a fixed dummy, then a constant-time compare. This prevents a
/// remote attacker from distinguishing "no such role", "filtered out", or
/// "wrong password" by measuring response latency.
pub fn verify(username: &str, password: &[u8]) -> AuthOutcome {
    let filter = crate::get_password_auth_role_filter_guc();
    let lookup = lookup_auth(username, &filter);

    // Pick a verifier: the real one if the role is valid and has a parseable
    // SCRAM-SHA-256 entry, otherwise the dummy. The dummy ensures we always
    // do one PBKDF2 derivation below.
    let real_verifier: Option<ScramVerifier> = match &lookup {
        Ok(l) => l.role.as_ref().and_then(|r| {
            let role_usable = r.can_login
                && r.valid_until_unix
                    .map(|t| crate::license::now_secs() <= t)
                    .unwrap_or(true);
            if role_usable {
                r.rolpassword.as_deref().and_then(parse_scram_verifier)
            } else {
                None
            }
        }),
        Err(()) => None,
    };

    let (salt, iterations, expected_stored_key): (&[u8], NonZeroU32, &[u8]) = match &real_verifier {
        Some(v) => (&v.salt, v.iterations, &v.stored_key),
        None => (
            DUMMY_SCRAM_SALT,
            NonZeroU32::new(DUMMY_SCRAM_ITERATIONS).expect("nonzero"),
            &DUMMY_SCRAM_STORED_KEY,
        ),
    };

    let computed = compute_stored_key(password, salt, iterations);
    let matches = constant_time::verify_slices_are_equal(&computed, expected_stored_key).is_ok();

    match lookup {
        Err(()) => AuthOutcome::LookupError,
        Ok(l) => {
            // Filter rejection takes priority over role state — preserves
            // prior semantics where FilteredOut surfaces as NOT_AUTHORIZED
            // (0x87) so operators can distinguish it from BadPassword.
            if !l.passes_filter {
                return AuthOutcome::FilteredOut;
            }
            let r = match l.role {
                Some(r) => r,
                None => return AuthOutcome::BadRole,
            };
            if !r.can_login {
                return AuthOutcome::BadRole;
            }
            if let Some(t) = r.valid_until_unix {
                if crate::license::now_secs() > t {
                    return AuthOutcome::BadRole;
                }
            }
            if real_verifier.is_none() {
                return AuthOutcome::BadVerifier;
            }
            if matches {
                AuthOutcome::Ok
            } else {
                AuthOutcome::BadPassword
            }
        }
    }
}

struct AuthRow {
    can_login: bool,
    valid_until_unix: Option<i64>,
    rolpassword: Option<String>,
}

struct AuthLookup {
    /// True when the GUC pattern is empty or `username LIKE pattern`.
    passes_filter: bool,
    /// Role row from pg_authid; None when the role does not exist.
    role: Option<AuthRow>,
}
fn lookup_auth(username: &str, filter: &str) -> Result<AuthLookup, ()> {
    // BGW runs as the bootstrap superuser, so `pg_authid` is readable.
    // DatumWithOid args MUST be constructed inside the Spi::connect closure —
    // they reference SPI memory contexts that don't exist outside it.
    // BackgroundWorker::transaction provides the outer transaction; Spi::connect
    // alone is not safe at the BGW main-loop scope.
    let result: Result<AuthLookup, pgrx::spi::Error> =
        BackgroundWorker::transaction(|| Spi::connect(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> =
                vec![username.into(), filter.into()];
            let table = client.select(
                "SELECT \
                     ($2::text = '' OR $1::text LIKE $2::text) AS passes_filter, \
                     a.rolcanlogin, \
                     EXTRACT(EPOCH FROM a.rolvaliduntil)::bigint AS valid_until, \
                     a.rolpassword \
                 FROM (SELECT 1) q \
                 LEFT JOIN pg_catalog.pg_authid a ON a.rolname = $1::text",
                Some(1),
                &args,
            )?;
            for row in table {
                let passes_filter: bool = row.get::<bool>(1)?.unwrap_or(false);
                let rolcanlogin: Option<bool> = row.get::<bool>(2)?;
                let valid_until_unix: Option<i64> = row.get::<i64>(3)?;
                let rolpassword: Option<String> = row.get::<String>(4)?;
                let role = rolcanlogin.map(|can_login| AuthRow {
                    can_login,
                    valid_until_unix,
                    rolpassword,
                });
                return Ok(AuthLookup { passes_filter, role });
            }
            // LEFT JOIN against a one-row source always yields a row; this
            // branch is unreachable but kept explicit for type completeness.
            Ok(AuthLookup {
                passes_filter: false,
                role: None,
            })
        }));
    result.map_err(|e| {
        pgrx::log!("pgmqtt password_auth: auth lookup failed: {}", e);
    })
}

struct ScramVerifier {
    iterations: NonZeroU32,
    salt: Vec<u8>,
    stored_key: Vec<u8>,
}

fn parse_scram_verifier(s: &str) -> Option<ScramVerifier> {
    // SCRAM-SHA-256$<iter>:<b64salt>$<b64StoredKey>:<b64ServerKey>
    let rest = s.strip_prefix("SCRAM-SHA-256$")?;
    let (iter_salt, keys) = rest.split_once('$')?;
    let (iter_str, salt_b64) = iter_salt.split_once(':')?;
    let (stored_b64, _server_b64) = keys.split_once(':')?;

    let iterations = iter_str.parse::<u32>().ok().and_then(NonZeroU32::new)?;
    let engine = base64::engine::general_purpose::STANDARD;
    let salt = engine.decode(salt_b64).ok()?;
    let stored_key = engine.decode(stored_b64).ok()?;
    if stored_key.len() != 32 {
        return None;
    }
    Some(ScramVerifier {
        iterations,
        salt,
        stored_key,
    })
}

fn compute_stored_key(password: &[u8], salt: &[u8], iterations: NonZeroU32) -> Vec<u8> {
    let mut salted = [0u8; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iterations,
        salt,
        password,
        &mut salted,
    );
    let key = hmac::Key::new(hmac::HMAC_SHA256, &salted);
    let client_key = hmac::sign(&key, b"Client Key");
    digest::digest(&digest::SHA256, client_key.as_ref())
        .as_ref()
        .to_vec()
}

/// Loaded ACL rule set for a role.
#[derive(Debug, Default, Clone)]
pub struct AclRules {
    pub sub: Vec<String>,
    pub pub_: Vec<String>,
}

/// Load the per-topic ACL rows for `role_name` from `pgmqtt_acls`. Empty
/// vectors mean "unrestricted" under the existing claims-enforcement code.
///
/// Returns empty rules when the license does not include the `acl` feature —
/// Community-tier password auth grants full topic access.
pub fn load_acls_for_role(role_name: &str) -> AclRules {
    if !crate::license::has_feature(crate::license::Feature::Acl) {
        return AclRules::default();
    }
    let result: Result<AclRules, pgrx::spi::Error> = BackgroundWorker::transaction(|| Spi::connect(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![role_name.into()];
        let mut rules = AclRules::default();
        let table = client.select(
            "SELECT topic_filter, can_publish, can_subscribe \
             FROM pgmqtt_acls WHERE role_name = $1::name",
            None,
            &args,
        )?;
        for row in table {
            let filter: String = match row.get::<String>(1)? {
                Some(s) => s,
                None => continue,
            };
            let can_pub: bool = row.get::<bool>(2)?.unwrap_or(false);
            let can_sub: bool = row.get::<bool>(3)?.unwrap_or(false);
            if can_pub {
                rules.pub_.push(filter.clone());
            }
            if can_sub {
                rules.sub.push(filter);
            }
        }
        // Empty sub/pub_ vectors fall through to "unrestricted" under the
        // existing claims-enforcement code (see mqtt.rs:claim_covers_*). If
        // an operator wants "deny everything", they should leave the role
        // out of pgmqtt_acls and rely on jwt or role-filter instead.
        Ok(rules)
    }));
    match result {
        Ok(rules) => rules,
        Err(e) => {
            pgrx::log!("pgmqtt acls: load failed for role {}: {}", role_name, e);
            AclRules::default()
        }
    }
}

/// Returns a map from role name → rules. Roles with no matching rows are
/// present in the map with an empty `AclRules` (unrestricted under the
/// existing claims-enforcement code). Caller can pass duplicates; they're
/// deduped before the query.
///
/// Returns an empty map when the license does not include the `acl` feature.
pub fn load_acls_for_roles(role_names: &[String]) -> HashMap<String, AclRules> {
    if !crate::license::has_feature(crate::license::Feature::Acl) {
        return HashMap::new();
    }
    // Dedup the caller's roles so every requested role appears in the
    // result map, even if pgmqtt_acls has no rows for it (empty rules ==
    // unrestricted under the claims-enforcement code).
    let unique: Vec<String> = {
        let mut seen: HashMap<String, ()> = HashMap::new();
        for r in role_names {
            seen.entry(r.clone()).or_default();
        }
        seen.into_keys().collect()
    };
    if unique.is_empty() {
        return HashMap::new();
    }

    let result: Result<HashMap<String, AclRules>, pgrx::spi::Error> =
        BackgroundWorker::transaction(|| {
            Spi::connect(|client| {
                let mut out: HashMap<String, AclRules> = HashMap::new();
                for r in &unique {
                    out.entry(r.clone()).or_default();
                }
                let args: Vec<pgrx::datum::DatumWithOid> = vec![unique.clone().into()];
                let table = client.select(
                    "SELECT role_name::text, topic_filter, can_publish, can_subscribe \
                     FROM pgmqtt_acls WHERE role_name = ANY($1::name[])",
                    None,
                    &args,
                )?;
                for row in table {
                    let role: String = match row.get::<String>(1)? {
                        Some(s) => s,
                        None => continue,
                    };
                    let filter: String = match row.get::<String>(2)? {
                        Some(s) => s,
                        None => continue,
                    };
                    let can_pub: bool = row.get::<bool>(3)?.unwrap_or(false);
                    let can_sub: bool = row.get::<bool>(4)?.unwrap_or(false);
                    let rules = out.entry(role).or_default();
                    if can_pub {
                        rules.pub_.push(filter.clone());
                    }
                    if can_sub {
                        rules.sub.push(filter);
                    }
                }
                Ok(out)
            })
        });
    match result {
        Ok(m) => m,
        Err(e) => {
            // Return empty map so caller leaves existing in-memory ACLs
            // untouched on transient SPI failure rather than wiping them.
            pgrx::log!("pgmqtt acls: batched load failed: {}", e);
            HashMap::new()
        }
    }
}

/// Heuristic: does the bytes-in-password-field "look like" a JWT?
///
/// JWTs are three base64url segments separated by '.'. A plaintext password
/// will rarely contain exactly two dots and pass base64url decoding of all
/// three segments. Used to keep backward compatibility with deployments that
/// put a JWT in the password field with no username.
pub fn looks_like_jwt(password: &[u8]) -> bool {
    let Ok(s) = std::str::from_utf8(password) else {
        return false;
    };
    let s = s.trim();
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 3 {
        return false;
    }
    parts
        .iter()
        .all(|p| !p.is_empty() && crate::license::base64_url_decode(p).is_ok())
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use super::*;

    #[test]
    fn scram_verifier_parses_real_pg_output() {
        // Example verifier shape; iteration count and base64 values are illustrative.
        let v = "SCRAM-SHA-256$4096:c2FsdHNhbHRzYWx0c2FsdA==$abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRST==:dummy=";
        let parsed = parse_scram_verifier(v);
        assert!(parsed.is_some());
        let p = parsed.unwrap();
        assert_eq!(p.iterations.get(), 4096);
        assert_eq!(p.salt.len(), 12);
        assert_eq!(p.stored_key.len(), 32);
    }

    #[test]
    fn scram_verifier_rejects_md5() {
        assert!(parse_scram_verifier("md5abcdef0123456789abcdef01234567").is_none());
    }

    #[test]
    fn jwt_sniff_rejects_password() {
        assert!(!looks_like_jwt(b"hunter2"));
        assert!(!looks_like_jwt(b"a.b")); // wrong segment count
        assert!(!looks_like_jwt(b"a.b.c.d"));
    }

    #[test]
    fn jwt_sniff_accepts_three_b64url_parts() {
        // Minimal shape: each part is valid b64url and non-empty.
        assert!(looks_like_jwt(b"aGVhZGVy.cGF5bG9hZA.c2ln"));
    }

    #[test]
    fn scram_known_answer() {
        // RFC 5802 §4 doesn't give a SCRAM-SHA-256 test vector directly, but we
        // can verify the derivation is internally consistent by round-tripping:
        // compute StoredKey, then verify a fresh derivation against the same
        // password+salt+iterations produces the same bytes.
        let pw = b"correcthorse";
        let salt = b"NaCl"; // four bytes is enough for a smoke test
        let iters = NonZeroU32::new(4096).unwrap();
        let a = compute_stored_key(pw, salt, iters);
        let b = compute_stored_key(pw, salt, iters);
        assert_eq!(a, b);
        assert_eq!(a.len(), 32);
        // Wrong password must produce a different StoredKey.
        let c = compute_stored_key(b"wrong", salt, iters);
        assert_ne!(a, c);
    }
}
