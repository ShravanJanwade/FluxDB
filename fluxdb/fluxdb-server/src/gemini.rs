//! Gemini transport, shared by the single-tenant assistant and the cloud agent.
//!
//! Everything provider-facing lives here: the HTTP client, bounded retries for
//! transient failures, response size limits, model and key validation, and the
//! mapping from a provider status to a message a user can act on. Callers get
//! a parsed response or a typed [`Error`]; neither layer talks to Google
//! directly, so a fix to a provider quirk lands in one place.

use serde_json::{json, Value};
use std::time::Duration;

/// Model used when neither the caller nor `GEMINI_MODEL` names one.
pub const DEFAULT_MODEL: &str = "gemini-2.5-flash";

/// A response larger than this is refused rather than buffered.
const MAX_RESPONSE_BYTES: usize = 2_000_000;

#[derive(Debug)]
pub enum Error {
    /// The provider could not be reached at all.
    Unreachable(String),
    /// The provider answered, but not with a usable result.
    Provider { status: u16, message: String },
    /// The provider's answer was too large, truncated or not JSON.
    Malformed(String),
    /// The caller's key or model is unusable; never reached the provider.
    Invalid(String),
    /// No key is configured anywhere.
    NoKey(String),
}

impl Error {
    /// HTTP status a caller should surface for this failure.
    pub fn status(&self) -> u16 {
        match self {
            Error::Invalid(_) => 400,
            Error::NoKey(_) => 503,
            Error::Provider { status: 429, .. } => 429,
            Error::Unreachable(_) | Error::Provider { .. } | Error::Malformed(_) => 502,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Error::Unreachable(m) | Error::Malformed(m) | Error::Invalid(m) | Error::NoKey(m) => m,
            Error::Provider { message, .. } => message,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

/// `GEMINI_MODEL`, or the built-in default.
pub fn default_model() -> String {
    std::env::var("GEMINI_MODEL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_MODEL.into())
}

/// The shared server key, when one is configured.
pub fn server_key() -> Option<String> {
    std::env::var("GEMINI_API_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().to_owned())
}

/// Accept a key only if it could plausibly be sent as a header value. This is a
/// format check, not an authenticity check — only the provider can judge that.
pub fn validate_key(key: &str) -> Result<String, Error> {
    let key = key.trim();
    if key.is_empty() {
        return Err(Error::Invalid("An API key is required".into()));
    }
    if key.len() > 256 || key.chars().any(char::is_control) {
        return Err(Error::Invalid("Invalid Gemini key format".into()));
    }
    Ok(key.to_owned())
}

/// Accept a Gemini model id. The id becomes part of a URL path, so the
/// character set is restricted rather than escaped.
pub fn validate_model(model: &str) -> Result<String, Error> {
    let model = model.trim();
    if model.is_empty() {
        return Ok(default_model());
    }
    if !model.starts_with("gemini-")
        || model.len() > 100
        || !model
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
    {
        return Err(Error::Invalid(
            "Enter a Gemini model ID, for example gemini-2.5-flash".into(),
        ));
    }
    Ok(model.to_owned())
}

pub struct Client {
    http: reqwest::Client,
    endpoint: String,
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Client {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(45))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("TLS HTTP client"),
            endpoint: "https://generativelanguage.googleapis.com/v1beta/models".into(),
        }
    }

    /// Point the client at a stand-in provider. Tests use this to drive the
    /// whole tool loop without reaching Google.
    #[doc(hidden)]
    pub fn with_endpoint(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Self::new()
        }
    }

    /// One `generateContent` call, with bounded retries for transient provider
    /// failures. Returns the parsed body.
    pub async fn generate(&self, key: &str, model: &str, body: &Value) -> Result<Value, Error> {
        for attempt in 0..3u32 {
            let response = self
                .http
                .post(format!("{}/{}:generateContent", self.endpoint, model))
                .header("x-goog-api-key", key)
                .json(body)
                .send()
                .await
                .map_err(|_| {
                    Error::Unreachable(
                        "Cannot reach Gemini. Check server internet access or try a faster model."
                            .into(),
                    )
                })?;
            let status = response.status().as_u16();
            let transient = matches!(status, 502..=504);
            if !transient || attempt == 2 {
                return self.read(response).await;
            }
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            // Honour a long Retry-After by surfacing it rather than ignoring it.
            if retry_after.is_some_and(|seconds| seconds > 8) {
                return self.read(response).await;
            }
            let delay = Duration::from_secs(retry_after.unwrap_or(1 << attempt).max(1));
            tracing::warn!(status, attempt = attempt + 1, model, "retrying Gemini");
            drop(response);
            tokio::time::sleep(delay).await;
        }
        unreachable!("bounded retry loop always returns")
    }

    async fn read(&self, response: reqwest::Response) -> Result<Value, Error> {
        let status = response.status();
        if !status.is_success() {
            let body = response.json::<Value>().await.unwrap_or(Value::Null);
            return Err(Error::Provider {
                status: status.as_u16(),
                message: explain(status.as_u16(), &body),
            });
        }
        if response.content_length().unwrap_or(0) as usize > MAX_RESPONSE_BYTES {
            return Err(Error::Malformed(
                "Gemini response exceeded the size limit".into(),
            ));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| Error::Malformed("Incomplete Gemini response".into()))?;
        if bytes.len() > MAX_RESPONSE_BYTES {
            return Err(Error::Malformed(
                "Gemini response exceeded the size limit".into(),
            ));
        }
        serde_json::from_slice(&bytes)
            .map_err(|_| Error::Malformed("Invalid Gemini response".into()))
    }

    /// Model ids available to this key that support function calling.
    pub async fn models(&self, key: &str) -> Result<Vec<Value>, Error> {
        let response = self
            .http
            .get(&self.endpoint)
            .query(&[("pageSize", "1000")])
            .header("x-goog-api-key", key)
            .send()
            .await
            .map_err(|_| {
                Error::Unreachable(
                    "Cannot reach Gemini. Check the server internet connection.".into(),
                )
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::Provider {
                status: status.as_u16(),
                message: format!(
                    "Gemini model lookup failed (Provider HTTP {}). Check your AI Studio key, API permissions, and quota.",
                    status.as_u16()
                ),
            });
        }
        let body: Value = response
            .json()
            .await
            .map_err(|_| Error::Malformed("Invalid Gemini model response".into()))?;
        Ok(body["models"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|model| {
                model["supportedGenerationMethods"]
                    .as_array()
                    .is_some_and(|methods| methods.iter().any(|method| method == "generateContent"))
            })
            .filter_map(|model| {
                model["name"]
                    .as_str()
                    .and_then(|name| name.strip_prefix("models/"))
                    .filter(|name| name.starts_with("gemini-"))
                    .map(|id| json!({"id":id,"name":model["displayName"]}))
            })
            .take(100)
            .collect())
    }
}

/// Turn a provider rejection into something the reader can act on. The
/// structured `reason` is preferred over the status, because an invalid key and
/// an exhausted quota both arrive as 400-family errors.
fn explain(status: u16, body: &Value) -> String {
    let reason = body["error"]["details"]
        .as_array()
        .and_then(|details| details.iter().find_map(|detail| detail["reason"].as_str()))
        .unwrap_or("");
    let message = if [
        "API_KEY_INVALID",
        "API_KEY_EXPIRED",
        "API_KEY_SERVICE_BLOCKED",
    ]
    .contains(&reason)
    {
        "Gemini rejected the API key. Replace it with an active Gemini API key from Google AI Studio."
    } else {
        match status {
            401 | 403 => "Gemini rejected the key or project permissions. Check the API key in Google AI Studio.",
            429 => "Gemini quota or rate limit reached. Check your API project billing and quota, then retry.",
            400 | 404 => "Gemini rejected the model or request. Check that this model ID is available and supports generateContent function calling.",
            502..=504 => "The selected Gemini model remains unavailable after bounded retry handling. Select another model or retry later. No proposed changes were executed.",
            _ => "Gemini is temporarily unavailable. Retry later.",
        }
    };
    format!("{message} (Provider HTTP {status})")
}

/// Extract the text and function calls from a `generateContent` response.
pub struct Turn {
    pub text: String,
    pub calls: Vec<(String, Value)>,
    /// The raw model content, to be echoed back as history on the next round.
    pub content: Value,
}

pub fn parse_turn(response: &Value) -> Result<Turn, Error> {
    let content = response["candidates"][0]["content"].clone();
    let parts = content["parts"].as_array().ok_or_else(|| {
        Error::Malformed(
            "Gemini returned no answer. The response may have been blocked; rephrase the request."
                .into(),
        )
    })?;
    let text = parts
        .iter()
        .filter(|part| part["thought"] != true)
        .filter_map(|part| part["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let calls = parts
        .iter()
        .filter_map(|part| {
            let call = part.get("functionCall")?;
            Some((
                call["name"].as_str()?.to_owned(),
                call.get("args").cloned().unwrap_or(json!({})),
            ))
        })
        .collect();
    Ok(Turn {
        text,
        calls,
        content,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_model_id_must_be_url_safe() {
        assert_eq!(validate_model("gemini-2.5-pro").unwrap(), "gemini-2.5-pro");
        assert_eq!(validate_model("").unwrap(), default_model());
        // Path traversal and query injection through the model segment.
        for bad in [
            "gemini-../../models",
            "gemini-a?key=x",
            "gpt-4",
            "gemini-a/b",
        ] {
            assert!(validate_model(bad).is_err(), "{bad} must be rejected");
        }
    }

    #[test]
    fn a_key_must_be_header_safe() {
        assert_eq!(validate_key("  abc  ").unwrap(), "abc");
        assert!(validate_key("").is_err());
        assert!(validate_key("bad\nvalue").is_err(), "no header splitting");
        assert!(validate_key(&"x".repeat(257)).is_err());
    }

    #[test]
    fn an_invalid_key_reason_beats_the_status() {
        let body = json!({"error":{"details":[{"reason":"API_KEY_INVALID"}]}});
        assert!(explain(400, &body).contains("rejected the API key"));
        // Without a reason, the status decides.
        assert!(explain(429, &Value::Null).contains("quota"));
    }

    #[test]
    fn a_turn_separates_text_from_calls_and_drops_thoughts() {
        let response = json!({"candidates":[{"content":{"parts":[
            {"text":"internal","thought":true},
            {"text":"visible"},
            {"functionCall":{"name":"run_query","args":{"sql":"SELECT 1"}}}
        ]}}]});
        let turn = parse_turn(&response).unwrap();
        assert_eq!(turn.text, "visible");
        assert_eq!(turn.calls.len(), 1);
        assert_eq!(turn.calls[0].0, "run_query");
        assert_eq!(turn.calls[0].1["sql"], "SELECT 1");
    }

    #[test]
    fn a_blocked_response_is_an_error_not_an_empty_answer() {
        assert!(parse_turn(&json!({"candidates":[]})).is_err());
    }
}
