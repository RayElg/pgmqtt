//! License validation for pgmqtt enterprise features.
//!
//! License token format: `base64url(JSON_payload).base64url(Ed25519_signature_over_JSON_bytes)`

use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::sync::{Mutex, OnceLock};

// ---------------------------------------------------------------------------
// Public keys
// ---------------------------------------------------------------------------

/// Ed25519 public key used to verify license tokens.
const PRODUCTION_PUBLIC_KEY: [u8; 32] = [
    0x02, 0xcc, 0x0d, 0x7f, 0x6b, 0x37, 0xd2, 0x7e, 0xfa, 0x8e, 0x3b, 0x33, 0x72, 0xf0, 0x02, 0x52,
    0xa5, 0x75, 0x85, 0x26, 0x6b, 0x9b, 0xbc, 0xef, 0x19, 0x61, 0xbb, 0xd6, 0xdc, 0x78, 0x4e, 0xa8,
];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Feature {
    Tls,
    Jwt,
    Metrics,
}

impl Feature {
    fn as_str(&self) -> &'static str {
        match self {
            Feature::Tls => "tls",
            Feature::Jwt => "jwt",
            Feature::Metrics => "metrics",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LicensePayload {
    pub customer: String,
    pub expires_at: i64,
    pub grace_expires_at: i64,
    pub features: Vec<String>,
    pub max_connections: usize,
}

#[derive(Debug, Clone)]
pub enum LicenseStatus {
    Community,
    Active(LicensePayload),
    Grace(LicensePayload),
    Expired,
    /// License key was provided but is invalid (bad format, bad signature, etc.).
    Invalid(String),
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

static LICENSE_CACHE: OnceLock<Mutex<(String, LicenseStatus)>> = OnceLock::new();

fn cache() -> &'static Mutex<(String, LicenseStatus)> {
    LICENSE_CACHE.get_or_init(|| Mutex::new((String::new(), LicenseStatus::Community)))
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parse and validate a license token string.
pub fn validate_license(token: &str) -> LicenseStatus {
    validate_license_with_key(token, active_public_key())
}

fn active_public_key() -> &'static [u8; 32] {
    &PRODUCTION_PUBLIC_KEY
}

/// Known feature names for validation.
const KNOWN_FEATURES: &[&str] = &["tls", "jwt", "metrics", "multi_node"];

fn validate_license_with_key(token: &str, pubkey_bytes: &[u8; 32]) -> LicenseStatus {
    let token = token.trim();
    if token.is_empty() {
        return LicenseStatus::Community;
    }

    let parts: Vec<&str> = token.splitn(2, '.').collect();
    if parts.len() != 2 {
        return LicenseStatus::Invalid("license token must be payload.signature".into());
    }

    let payload_b64 = parts[0];
    let sig_b64 = parts[1];

    // Decode payload bytes
    let payload_bytes = match base64_url_decode(payload_b64) {
        Ok(b) => b,
        Err(_) => return LicenseStatus::Invalid("bad base64url in payload".into()),
    };

    // Decode signature
    let sig_bytes = match base64_url_decode(sig_b64) {
        Ok(b) => b,
        Err(_) => return LicenseStatus::Invalid("bad base64url in signature".into()),
    };

    if sig_bytes.len() != 64 {
        return LicenseStatus::Invalid(format!(
            "signature must be 64 bytes, got {}",
            sig_bytes.len()
        ));
    }

    // Verify signature
    let verifying_key = match VerifyingKey::from_bytes(pubkey_bytes) {
        Ok(k) => k,
        Err(_) => return LicenseStatus::Invalid("invalid Ed25519 public key".into()),
    };

    let sig_arr: [u8; 64] = match sig_bytes.try_into() {
        Ok(a) => a,
        Err(_) => return LicenseStatus::Invalid("signature conversion failed".into()),
    };
    let signature = Signature::from_bytes(&sig_arr);

    use ed25519_dalek::Verifier;
    if verifying_key.verify(&payload_bytes, &signature).is_err() {
        return LicenseStatus::Invalid("signature verification failed".into());
    }

    // Parse JSON payload
    let payload: LicensePayload = match serde_json::from_slice(&payload_bytes) {
        Ok(p) => p,
        Err(e) => return LicenseStatus::Invalid(format!("bad payload JSON: {}", e)),
    };

    // Warn about unrecognized feature names
    for f in &payload.features {
        if !KNOWN_FEATURES.contains(&f.as_str()) {
            #[cfg(feature = "pg_test")]
            eprintln!("pgmqtt WARNING: unrecognized license feature: '{}'", f);
            // In production builds this uses pgrx::warning! (when available)
        }
    }

    // Check expiry
    let now = now_secs();

    if now > payload.grace_expires_at {
        LicenseStatus::Expired
    } else if now > payload.expires_at {
        LicenseStatus::Grace(payload)
    } else {
        LicenseStatus::Active(payload)
    }
}

/// Returns the current license status, using a cache keyed on the GUC value.
/// Logs a warning when a license key is set but invalid.
pub fn current_status() -> LicenseStatus {
    let key = crate::get_license_key_guc();
    let mut guard = cache().lock().unwrap_or_else(|e| e.into_inner());
    if guard.0 != key {
        let status = validate_license(&key);
        if let LicenseStatus::Invalid(ref reason) = status {
            // Use eprintln as fallback; in bgworker context pgrx::log! is used
            eprintln!("pgmqtt WARNING: invalid license key: {}", reason);
        }
        *guard = (key, status.clone());
        status
    } else {
        guard.1.clone()
    }
}

/// Returns true if the license is Active or Grace.
pub fn is_enterprise() -> bool {
    matches!(
        current_status(),
        LicenseStatus::Active(_) | LicenseStatus::Grace(_)
    )
}

/// Returns true if the enterprise license includes the given feature.
pub fn has_feature(f: Feature) -> bool {
    let feature_str = f.as_str();
    match current_status() {
        LicenseStatus::Active(p) | LicenseStatus::Grace(p) => {
            p.features.iter().any(|s| s == feature_str)
        }
        _ => false,
    }
}

/// Returns the max_connections limit: 1000 for Community, payload value for enterprise.
pub fn max_connections() -> usize {
    match current_status() {
        LicenseStatus::Active(p) | LicenseStatus::Grace(p) => p.max_connections,
        _ => 1000,
    }
}

// ---------------------------------------------------------------------------
// Shared utilities
// ---------------------------------------------------------------------------

/// Current Unix timestamp in seconds.
pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Base64url helpers (no padding)
// ---------------------------------------------------------------------------

pub fn base64_url_decode(s: &str) -> Result<Vec<u8>, ()> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .map_err(|_| ())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

