//! Credentials, sessions and identity providers.

use argon2::password_hash::{
    rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString,
};
use argon2::Argon2;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};

pub const SESSION_COOKIE: &str = "flux_session";
pub const OAUTH_COOKIE: &str = "flux_oauth";

/// Random URL-safe token of `bytes` entropy.
pub fn random_token(bytes: usize) -> String {
    let mut buffer = vec![0u8; bytes];
    OsRng.fill_bytes(&mut buffer);
    URL_SAFE_NO_PAD.encode(buffer)
}

/// Lowercase hex identifier of `bytes` entropy.
pub fn random_hex(bytes: usize) -> String {
    let mut buffer = vec![0u8; bytes];
    OsRng.fill_bytes(&mut buffer);
    buffer.iter().map(|b| format!("{b:02x}")).collect()
}

/// Lowercase base-32 identifier, used for ids that become part of engine
/// database names (no padding, no ambiguous characters, always alphanumeric).
pub fn random_id(length: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijkmnpqrstuvwxyz23456789";
    let mut buffer = vec![0u8; length];
    OsRng.fill_bytes(&mut buffer);
    buffer
        .iter()
        .map(|b| ALPHABET[(*b as usize) % ALPHABET.len()] as char)
        .collect()
}

pub fn sha256_hex(input: &str) -> String {
    let digest = Sha256::digest(input.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// Constant-time equality for secrets of equal expected length.
pub fn secrets_match(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        let left = a.get(i).copied().unwrap_or(0);
        let right = b.get(i).copied().unwrap_or(0);
        diff |= (left ^ right) as usize;
    }
    diff == 0
}

/// Argon2id hash for a user-chosen password.
pub fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| format!("Password could not be hashed: {e}"))
}

pub fn verify_password(password: &str, encoded: &str) -> bool {
    PasswordHash::new(encoded)
        .map(|parsed| {
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .is_ok()
        })
        .unwrap_or(false)
}

type HmacSha256 = Hmac<Sha256>;

/// Server-held secrets and provider configuration.
pub struct Secrets {
    session_secret: Vec<u8>,
    pub github: Option<GithubApp>,
    pub public_base_url: Option<String>,
}

pub struct GithubApp {
    pub client_id: String,
    pub client_secret: String,
}

impl Secrets {
    /// Load from the environment. A missing `FLUXDB_SESSION_SECRET` generates an
    /// ephemeral one, which keeps a first local run working while making the
    /// consequence explicit: sessions do not survive a restart.
    pub fn from_env() -> (Self, Vec<String>) {
        let mut notes = Vec::new();
        let session_secret = match std::env::var("FLUXDB_SESSION_SECRET") {
            Ok(value) if value.len() >= 32 => value.into_bytes(),
            Ok(_) => {
                notes.push(
                    "FLUXDB_SESSION_SECRET is shorter than 32 characters; generated an ephemeral secret. Signed cookies will not survive a restart.".into(),
                );
                random_token(32).into_bytes()
            }
            Err(_) => {
                notes.push(
                    "FLUXDB_SESSION_SECRET is not set; generated an ephemeral secret. Signed cookies will not survive a restart.".into(),
                );
                random_token(32).into_bytes()
            }
        };
        let github = match (
            std::env::var("GITHUB_CLIENT_ID")
                .ok()
                .filter(|v| !v.is_empty()),
            std::env::var("GITHUB_CLIENT_SECRET")
                .ok()
                .filter(|v| !v.is_empty()),
        ) {
            (Some(client_id), Some(client_secret)) => Some(GithubApp {
                client_id,
                client_secret,
            }),
            (Some(_), None) | (None, Some(_)) => {
                notes.push(
                    "GitHub sign-in needs both GITHUB_CLIENT_ID and GITHUB_CLIENT_SECRET; the provider stays disabled.".into(),
                );
                None
            }
            (None, None) => None,
        };
        let public_base_url = std::env::var("PUBLIC_BASE_URL")
            .ok()
            .map(|value| value.trim_end_matches('/').to_string())
            .filter(|value| value.starts_with("http://") || value.starts_with("https://"));
        (
            Self {
                session_secret,
                github,
                public_base_url,
            },
            notes,
        )
    }

    #[cfg(test)]
    pub fn for_tests() -> Self {
        Self {
            session_secret: b"test-session-secret-at-least-32-bytes".to_vec(),
            github: None,
            public_base_url: None,
        }
    }

    fn tag(&self, payload: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.session_secret)
            .expect("HMAC accepts keys of any length");
        mac.update(payload.as_bytes());
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    }

    /// `payload.tag`, for values that travel through the browser and must not be
    /// modified (OAuth state and its return path).
    pub fn sign(&self, payload: &str) -> String {
        format!("{payload}.{}", self.tag(payload))
    }

    pub fn verify(&self, signed: &str) -> Option<String> {
        let (payload, tag) = signed.rsplit_once('.')?;
        secrets_match(tag, &self.tag(payload)).then(|| payload.to_string())
    }
}

/// `Set-Cookie` value for a session or OAuth-state cookie. `Secure` is applied
/// whenever the deployment is reached over HTTPS; a plain-HTTP local run must
/// not set it or the browser drops the cookie.
pub fn cookie(name: &str, value: &str, max_age: i64, secure: bool) -> String {
    let mut parts = vec![
        format!("{name}={value}"),
        "Path=/".to_string(),
        "HttpOnly".to_string(),
        "SameSite=Lax".to_string(),
        format!("Max-Age={max_age}"),
    ];
    if max_age == 0 {
        parts.push("Expires=Thu, 01 Jan 1970 00:00:00 GMT".to_string());
    }
    if secure {
        parts.push("Secure".to_string());
    }
    parts.join("; ")
}

/// Read one cookie out of a `Cookie` header.
pub fn read_cookie(header: Option<&str>, name: &str) -> Option<String> {
    header?.split(';').find_map(|pair| {
        let (key, value) = pair.trim().split_once('=')?;
        (key == name).then(|| value.to_string())
    })
}

/// Parsed API key token. Tokens are `fdbk_{id}_{secret}`, which keeps the
/// public key id greppable in logs while the secret half stays opaque.
pub struct ParsedKey {
    pub id: String,
    pub secret: String,
}

pub fn format_key_token(id: &str, secret: &str) -> String {
    format!("fdbk_{id}_{secret}")
}

pub fn parse_key_token(token: &str) -> Option<ParsedKey> {
    let rest = token.trim().strip_prefix("fdbk_")?;
    let (id, secret) = rest.split_once('_')?;
    let valid_id = id.len() == 16 && id.bytes().all(|b| b.is_ascii_hexdigit());
    (valid_id && secret.len() >= 32).then(|| ParsedKey {
        id: id.to_string(),
        secret: secret.to_string(),
    })
}

/// Authorization URL for the GitHub authorization-code flow.
pub fn github_authorize_url(app: &GithubApp, redirect_uri: &str, state: &str) -> String {
    format!(
        "https://github.com/login/oauth/authorize?client_id={}&redirect_uri={}&scope={}&state={}&allow_signup=true",
        urlencoding::encode(&app.client_id),
        urlencoding::encode(redirect_uri),
        urlencoding::encode("read:user user:email"),
        urlencoding::encode(state),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_hashes_verify_only_against_the_original() {
        let hash = hash_password("correct horse battery staple").unwrap();
        assert!(hash.starts_with("$argon2"));
        assert!(verify_password("correct horse battery staple", &hash));
        assert!(!verify_password("Correct horse battery staple", &hash));
        assert!(!verify_password("", &hash));
        // A stored value that is not a valid encoded hash must fail closed.
        assert!(!verify_password("anything", "not-a-hash"));
    }

    #[test]
    fn signatures_reject_tampered_payloads() {
        let secrets = Secrets::for_tests();
        let signed = secrets.sign("state-value|/app/projects");
        assert_eq!(
            secrets.verify(&signed).as_deref(),
            Some("state-value|/app/projects")
        );
        let (payload, tag) = signed.rsplit_once('.').unwrap();
        assert!(secrets.verify(&format!("{payload}x.{tag}")).is_none());
        assert!(secrets.verify("no-separator").is_none());
    }

    #[test]
    fn cookies_carry_the_expected_attributes() {
        let set = cookie(SESSION_COOKIE, "abc", 3600, true);
        assert!(set.contains("flux_session=abc"));
        assert!(set.contains("HttpOnly") && set.contains("SameSite=Lax") && set.contains("Secure"));
        let insecure = cookie(SESSION_COOKIE, "abc", 0, false);
        assert!(!insecure.contains("Secure"));
        assert!(insecure.contains("Max-Age=0") && insecure.contains("Expires="));
    }

    #[test]
    fn cookie_parsing_picks_the_named_value() {
        let header = Some("other=1; flux_session=token-value; trailing=2");
        assert_eq!(
            read_cookie(header, SESSION_COOKIE).as_deref(),
            Some("token-value")
        );
        assert!(read_cookie(header, "absent").is_none());
        assert!(read_cookie(None, SESSION_COOKIE).is_none());
    }

    #[test]
    fn api_key_tokens_round_trip_and_reject_malformed_input() {
        let id = random_hex(8);
        let secret = random_token(32);
        let token = format_key_token(&id, &secret);
        let parsed = parse_key_token(&token).expect("round trip");
        assert_eq!(parsed.id, id);
        assert_eq!(parsed.secret, secret);
        for bad in [
            "",
            "fdbk_short_secret",
            "wrongprefix_0123456789abcdef_secret",
            &format!("fdbk_{id}_tooshort"),
            &format!("fdbk_ZZZZZZZZZZZZZZZZ_{secret}"),
        ] {
            assert!(parse_key_token(bad).is_none(), "{bad} should be rejected");
        }
    }

    #[test]
    fn generated_identifiers_fit_engine_database_names() {
        let id = random_id(12);
        assert_eq!(id.len(), 12);
        assert!(id.bytes().all(|b| b.is_ascii_alphanumeric()));
        assert_ne!(random_id(12), random_id(12));
        assert_eq!(sha256_hex("").len(), 64);
        assert!(secrets_match("abc", "abc") && !secrets_match("abc", "abd"));
        assert!(!secrets_match("abc", "abcd"));
    }
}
