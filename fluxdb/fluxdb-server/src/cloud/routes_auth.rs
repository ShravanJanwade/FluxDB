//! Sign-up, sign-in, guest access and GitHub identity linking.

use super::model::{limits, Account, AccountKind, Invite, Member, Org, Project, Role};
use super::store::Entity;
use super::{
    auth, bad_request, client_key, conflict, forbidden, internal, now_ms, seed, throttled,
    unauthorized, Actor, Cloud, CloudState, Fail, Result,
};
use axum::{
    extract::{Query, State},
    http::{header::SET_COOKIE, HeaderMap, StatusCode},
    response::{IntoResponse, Redirect, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

pub fn routes() -> Router<CloudState> {
    Router::new()
        .route("/api/cloud/config", get(config))
        .route("/api/cloud/auth/signup", post(signup))
        .route("/api/cloud/auth/login", post(login))
        .route("/api/cloud/auth/logout", post(logout))
        .route("/api/cloud/auth/guest", post(guest))
        .route("/api/cloud/auth/session", get(session))
        .route("/api/cloud/auth/password", post(change_password))
        .route("/api/cloud/auth/account", delete(delete_account))
        .route("/api/cloud/auth/github/start", get(github_start))
        .route("/api/cloud/auth/github/callback", get(github_callback))
}

async fn config(State(cloud): State<CloudState>) -> Json<Value> {
    Json(super::public_config(&cloud).await)
}

/// Public view of an account. Built explicitly so a credential hash can never
/// reach a response by being added to the stored struct later.
fn account_json(account: &Account) -> Value {
    json!({
        "id": account.id,
        "email": account.email,
        "name": account.name,
        "avatar_url": account.avatar_url,
        "kind": account.kind,
        "has_password": account.password_hash.is_some(),
        "created_at": account.created_at,
        "expires_at": account.expires_at,
    })
}

/// Everything the browser needs to render the shell after authentication: the
/// account, its organizations with the caller's role in each, and the projects
/// it can open.
pub(super) async fn session_payload(cloud: &Cloud, actor: &Actor) -> Result<Value> {
    let memberships = cloud
        .store
        .list_by_owner::<Member>(&actor.account.id)
        .await?;
    let mut organizations = Vec::new();
    for membership in &memberships {
        let Some(org) = cloud.store.get::<Org>(&membership.org_id).await? else {
            continue;
        };
        let projects = cloud.store.list_by_parent::<Project>(&org.id).await?;
        organizations.push(json!({
            "id": org.id,
            "name": org.name,
            "slug": org.slug,
            "plan": org.plan,
            "role": membership.role,
            "expires_at": org.expires_at,
            "is_demo": org.id == super::DEMO_ORG_ID,
            "projects": projects.iter().map(|project| json!({
                "id": project.id,
                "name": project.name,
                "slug": project.slug,
                "description": project.description,
                "demo": project.demo,
                "created_at": project.created_at,
            })).collect::<Vec<_>>(),
        }));
    }
    // The visitor's own workspaces first, the shared showcase last.
    organizations.sort_by_key(|org| org["is_demo"].as_bool().unwrap_or(false));
    Ok(json!({
        "account": account_json(&actor.account),
        "organizations": organizations,
    }))
}

fn set_session_cookie(
    cloud: &Cloud,
    headers: &HeaderMap,
    token: &str,
    lifetime_seconds: i64,
) -> (axum::http::HeaderName, String) {
    (
        SET_COOKIE,
        auth::cookie(
            auth::SESSION_COOKIE,
            token,
            lifetime_seconds,
            cloud.secure_cookies(headers),
        ),
    )
}

#[derive(Deserialize)]
struct SignupRequest {
    email: String,
    password: String,
    name: Option<String>,
}

async fn signup(
    State(cloud): State<CloudState>,
    headers: HeaderMap,
    Json(request): Json<SignupRequest>,
) -> Result<Response> {
    let ip = client_key(&headers);
    if !cloud.throttle.lock().map_err(internal)?.allow(
        &format!("signup:{ip}"),
        10,
        60 * 60 * 1000,
        now_ms(),
    ) {
        return Err(throttled(
            "Too many sign-up attempts from this address. Try again later.",
        ));
    }
    let email = super::model::validate_email(&request.email).map_err(bad_request)?;
    super::model::validate_password(&request.password).map_err(bad_request)?;
    let name = match request
        .name
        .as_deref()
        .map(str::trim)
        .filter(|n| !n.is_empty())
    {
        Some(name) => super::model::validate_display_name(name, "name").map_err(bad_request)?,
        None => email.split('@').next().unwrap_or("Engineer").to_string(),
    };

    let account = Account {
        id: format!("acc_{}", auth::random_id(16)),
        email: email.clone(),
        name,
        password_hash: Some(auth::hash_password(&request.password).map_err(internal)?),
        avatar_url: None,
        kind: AccountKind::Standard,
        created_at: now_ms(),
        last_seen_at: now_ms(),
        expires_at: None,
    };
    // The store's unique lookup index on the email address is what makes this
    // race-free; two concurrent sign-ups cannot both win.
    if let Err(error) = cloud.store.create(&account).await {
        return match error {
            super::store::StoreError::Conflict(_) => Err(conflict(
                "An account already uses that email address. Sign in instead.",
            )),
            other => Err(internal(other)),
        };
    }
    provision_workspace(&cloud, &account, false).await?;
    accept_pending_invites(&cloud, &account).await?;

    let (token, lifetime) = cloud.start_session(&account, user_agent(&headers)).await?;
    let actor = Actor {
        account,
        session_id: auth::sha256_hex(&token),
    };
    let payload = session_payload(&cloud, &actor).await?;
    Ok((
        StatusCode::CREATED,
        [set_session_cookie(&cloud, &headers, &token, lifetime)],
        Json(payload),
    )
        .into_response())
}

#[derive(Deserialize)]
struct LoginRequest {
    email: String,
    password: String,
}

async fn login(
    State(cloud): State<CloudState>,
    headers: HeaderMap,
    Json(request): Json<LoginRequest>,
) -> Result<Response> {
    let email = request.email.trim().to_ascii_lowercase();
    let ip = client_key(&headers);
    {
        let mut throttle = cloud.throttle.lock().map_err(internal)?;
        let window = 15 * 60 * 1000;
        // Two limiters: one per account so a single target cannot be ground
        // down, one per address so a spray across accounts is also bounded.
        if !throttle.allow(&format!("login:{email}"), 8, window, now_ms())
            || !throttle.allow(&format!("loginip:{ip}"), 40, window, now_ms())
        {
            return Err(throttled(
                "Too many sign-in attempts. Wait a few minutes and try again.",
            ));
        }
    }
    let account = cloud.store.find::<Account>(&email).await?;
    let Some(account) = account else {
        // Still spend time hashing so a missing account is not distinguishable
        // from a wrong password by response timing.
        let _ = auth::verify_password(&request.password, DUMMY_HASH);
        return Err(unauthorized("That email address or password is incorrect"));
    };
    let Some(hash) = account.password_hash.clone() else {
        return Err(unauthorized(
            "This account signs in with GitHub. Use the GitHub button instead.",
        ));
    };
    if !auth::verify_password(&request.password, &hash) {
        return Err(unauthorized("That email address or password is incorrect"));
    }
    cloud
        .throttle
        .lock()
        .map_err(internal)?
        .reset(&format!("login:{email}"));

    let (token, lifetime) = cloud.start_session(&account, user_agent(&headers)).await?;
    let actor = Actor {
        account,
        session_id: auth::sha256_hex(&token),
    };
    let payload = session_payload(&cloud, &actor).await?;
    Ok((
        [set_session_cookie(&cloud, &headers, &token, lifetime)],
        Json(payload),
    )
        .into_response())
}

/// A valid Argon2 encoded hash of an unusable password, used to keep the
/// unknown-account branch of sign-in as slow as the known-account branch.
const DUMMY_HASH: &str =
    "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHR2YWx1ZQ$K8Y3eCvJFOjzHGE1p0M2sG5qyYq9QAoCr+W9h1KcJGU";

async fn logout(State(cloud): State<CloudState>, headers: HeaderMap) -> Result<Response> {
    if let Some(token) = auth::read_cookie(
        headers
            .get(axum::http::header::COOKIE)
            .and_then(|v| v.to_str().ok()),
        auth::SESSION_COOKIE,
    ) {
        cloud.end_session(&token).await;
    }
    Ok((
        [(
            SET_COOKIE,
            auth::cookie(auth::SESSION_COOKIE, "", 0, cloud.secure_cookies(&headers)),
        )],
        Json(json!({"signed_out": true})),
    )
        .into_response())
}

async fn session(State(cloud): State<CloudState>, actor: Actor) -> Result<Json<Value>> {
    Ok(Json(session_payload(&cloud, &actor).await?))
}

async fn guest(State(cloud): State<CloudState>, headers: HeaderMap) -> Result<Response> {
    let ip = client_key(&headers);
    if !cloud.throttle.lock().map_err(internal)?.allow(
        &format!("guest:{ip}"),
        6,
        60 * 60 * 1000,
        now_ms(),
    ) {
        return Err(throttled(
            "This address has started several demo workspaces recently. Create a free account to continue.",
        ));
    }
    if super::guest_capacity_reached(super::live_guests(&cloud).await) {
        return Err(Fail {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "guest_capacity",
            message: "The demo is at capacity right now. Create a free account, or try again in a few minutes.".into(),
        });
    }
    let handle = auth::random_id(10);
    let account = Account {
        id: format!("gst_{handle}"),
        email: format!("guest-{handle}@demo.fluxdb.dev"),
        name: "Guest explorer".into(),
        password_hash: None,
        avatar_url: None,
        kind: AccountKind::Guest,
        created_at: now_ms(),
        last_seen_at: now_ms(),
        expires_at: Some(now_ms() + limits::GUEST_LIFETIME_SECONDS * 1000),
    };
    cloud.store.create(&account).await?;
    provision_workspace(&cloud, &account, true).await?;
    let (token, lifetime) = cloud.start_session(&account, user_agent(&headers)).await?;
    let actor = Actor {
        account,
        session_id: auth::sha256_hex(&token),
    };
    let payload = session_payload(&cloud, &actor).await?;
    Ok((
        StatusCode::CREATED,
        [set_session_cookie(&cloud, &headers, &token, lifetime)],
        Json(payload),
    )
        .into_response())
}

#[derive(Deserialize)]
struct PasswordChange {
    current_password: Option<String>,
    new_password: String,
}

async fn change_password(
    State(cloud): State<CloudState>,
    actor: Actor,
    Json(request): Json<PasswordChange>,
) -> Result<Json<Value>> {
    actor.requires_account()?;
    super::model::validate_password(&request.new_password).map_err(bad_request)?;
    let mut account = actor.account.clone();
    if let Some(existing) = &account.password_hash {
        let supplied = request.current_password.as_deref().unwrap_or("");
        if !auth::verify_password(supplied, existing) {
            return Err(unauthorized("That current password is incorrect"));
        }
    }
    account.password_hash = Some(auth::hash_password(&request.new_password).map_err(internal)?);
    cloud.store.save(&account).await?;
    // Every other session is invalidated so changing a password ejects anyone
    // else who had it, while the browser making the change stays signed in.
    let mut revoked = 0;
    for session in cloud
        .store
        .list_by_owner::<super::model::Session>(&account.id)
        .await?
    {
        if session.id != actor.session_id {
            cloud
                .store
                .delete::<super::model::Session>(&session.id)
                .await?;
            revoked += 1;
        }
    }
    cloud
        .audit(
            &actor,
            super::DEMO_ORG_ID,
            None,
            "account.password",
            &account.email,
            "",
        )
        .await;
    Ok(Json(json!({"updated": true, "sessions_revoked": revoked})))
}

async fn delete_account(State(cloud): State<CloudState>, actor: Actor) -> Result<Json<Value>> {
    // Organizations the account owns alone are removed with it; shared ones
    // must be handed over first, which is a decision only a human can make.
    let memberships = cloud
        .store
        .list_by_owner::<Member>(&actor.account.id)
        .await?;
    for membership in &memberships {
        if membership.org_id == super::DEMO_ORG_ID {
            continue;
        }
        let Some(org) = cloud.store.get::<Org>(&membership.org_id).await? else {
            continue;
        };
        let others = cloud
            .store
            .list_by_parent::<Member>(&org.id)
            .await?
            .into_iter()
            .filter(|member| member.account_id != actor.account.id)
            .count();
        if membership.role == Role::Owner && others > 0 {
            return Err(conflict(format!(
                "Transfer ownership of {} to another member before deleting your account",
                org.name
            )));
        }
        if membership.role == Role::Owner {
            cloud.delete_org(&org).await?;
        } else {
            cloud.store.delete::<Member>(&membership.id()).await?;
        }
    }
    cloud
        .store
        .delete_by_owner::<super::model::Session>(&actor.account.id)
        .await?;
    cloud
        .store
        .delete_by_owner::<Member>(&actor.account.id)
        .await?;
    cloud
        .store
        .delete_by_owner::<super::model::Identity>(&actor.account.id)
        .await?;
    cloud.store.delete::<Account>(&actor.account.id).await?;
    Ok(Json(json!({"deleted": true})))
}

// ============================================================================
// Provisioning
// ============================================================================

/// Give a new account its own organization, a first project and a bucket. Guest
/// workspaces are pre-filled so the demo has something to explore immediately;
/// registered accounts start with an empty bucket and an explicit "load sample
/// data" action, because silently inventing data in someone's own workspace is
/// the wrong default.
async fn provision_workspace(cloud: &Cloud, account: &Account, is_guest: bool) -> Result<()> {
    let org_name = if is_guest {
        "Guest workspace".to_string()
    } else {
        format!("{}'s workspace", account.name)
    };
    let org = cloud
        .create_org(account, &org_name, if is_guest { "demo" } else { "free" })
        .await?;
    let project = cloud
        .create_project(
            &account.id,
            &org.id,
            if is_guest { "Demo sandbox" } else { "First project" },
            if is_guest {
                "A private, writable copy you can break freely. Removed automatically after 24 hours."
            } else {
                "Your first FluxDB project. Create buckets, send data, and build dashboards."
            },
        )
        .await?;
    let bucket_name = if is_guest {
        seed::SANDBOX_BUCKET
    } else {
        "metrics"
    };
    let bucket = cloud
        .create_bucket(&account.id, &project, bucket_name, 0)
        .await?;
    if is_guest {
        let db = cloud.open_bucket(&bucket)?;
        tokio::task::spawn_blocking(move || seed::seed_sandbox(&db))
            .await
            .map_err(internal)?
            .map_err(internal)?;
    }
    cloud.grant_demo_access(account).await?;
    Ok(())
}

/// Turn invitations addressed to this account's email into memberships.
async fn accept_pending_invites(cloud: &Cloud, account: &Account) -> Result<()> {
    for invite in cloud.store.list_by_owner::<Invite>(&account.email).await? {
        if matches!(invite.expires_at, Some(at) if at <= now_ms()) {
            let _ = cloud.store.delete::<Invite>(&invite.id).await;
            continue;
        }
        let member = Member {
            org_id: invite.org_id.clone(),
            account_id: account.id.clone(),
            email: account.email.clone(),
            name: account.name.clone(),
            role: invite.role,
            created_at: now_ms(),
        };
        let _ = cloud.store.create(&member).await;
        let _ = cloud.store.delete::<Invite>(&invite.id).await;
    }
    Ok(())
}

fn user_agent(headers: &HeaderMap) -> &str {
    headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown")
}

// ============================================================================
// GitHub
// ============================================================================

#[derive(Deserialize)]
struct StartParams {
    next: Option<String>,
}

/// Only same-origin paths are accepted as a post-sign-in destination, so the
/// OAuth round trip cannot be used as an open redirect.
fn safe_next(next: Option<String>) -> String {
    next.filter(|path| {
        path.starts_with('/') && !path.starts_with("//") && !path.contains('\\') && path.len() < 512
    })
    .unwrap_or_else(|| "/app".to_string())
}

async fn github_start(
    State(cloud): State<CloudState>,
    headers: HeaderMap,
    Query(params): Query<StartParams>,
) -> Result<Response> {
    let app = cloud.secrets.github.as_ref().ok_or_else(|| {
        bad_request(
            "GitHub sign-in is not configured on this deployment. Use email and password, or the demo.",
        )
    })?;
    let base = cloud
        .base_url(&headers)
        .ok_or_else(|| internal("no usable base URL for the OAuth callback"))?;
    let next = safe_next(params.next);
    let nonce = auth::random_token(24);
    // The state carries the return path and is signed, so neither can be
    // tampered with while it sits in the browser.
    let state = cloud.secrets.sign(&format!("{nonce}|{next}"));
    let redirect_uri = format!("{base}/api/cloud/auth/github/callback");
    Ok((
        [(
            SET_COOKIE,
            auth::cookie(
                auth::OAUTH_COOKIE,
                &nonce,
                600,
                cloud.secure_cookies(&headers),
            ),
        )],
        Redirect::temporary(&auth::github_authorize_url(app, &redirect_uri, &state)),
    )
        .into_response())
}

#[derive(Deserialize)]
struct CallbackParams {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

#[derive(Deserialize)]
struct GithubToken {
    access_token: Option<String>,
    error_description: Option<String>,
}

#[derive(Deserialize)]
struct GithubUser {
    id: i64,
    login: String,
    name: Option<String>,
    email: Option<String>,
    avatar_url: Option<String>,
}

#[derive(Deserialize)]
struct GithubEmail {
    email: String,
    primary: bool,
    verified: bool,
}

/// Send the browser back to the sign-in screen with a readable reason, instead
/// of rendering a bare JSON error in the middle of an OAuth redirect.
fn oauth_failure(reason: &str) -> Response {
    Redirect::temporary(&format!("/login?error={}", urlencoding::encode(reason))).into_response()
}

async fn github_callback(
    State(cloud): State<CloudState>,
    headers: HeaderMap,
    Query(params): Query<CallbackParams>,
) -> Response {
    match github_exchange(&cloud, &headers, params).await {
        Ok(response) => response,
        Err(failure) => oauth_failure(&failure.message),
    }
}

async fn github_exchange(
    cloud: &Cloud,
    headers: &HeaderMap,
    params: CallbackParams,
) -> Result<Response> {
    if let Some(error) = params.error {
        let detail = params.error_description.unwrap_or(error);
        return Err(bad_request(format!(
            "GitHub sign-in was declined: {detail}"
        )));
    }
    let app = cloud
        .secrets
        .github
        .as_ref()
        .ok_or_else(|| bad_request("GitHub sign-in is not configured"))?;
    let code = params
        .code
        .ok_or_else(|| bad_request("GitHub did not return an authorization code"))?;
    let state = params
        .state
        .ok_or_else(|| bad_request("GitHub did not return the request state"))?;
    let payload = cloud
        .secrets
        .verify(&state)
        .ok_or_else(|| forbidden("The sign-in request could not be verified. Start again."))?;
    let (nonce, next) = payload
        .split_once('|')
        .ok_or_else(|| forbidden("The sign-in request was malformed. Start again."))?;
    let cookie_nonce = auth::read_cookie(
        headers
            .get(axum::http::header::COOKIE)
            .and_then(|v| v.to_str().ok()),
        auth::OAUTH_COOKIE,
    )
    .ok_or_else(|| {
        forbidden("The sign-in request expired or cookies were blocked. Start again.")
    })?;
    if !auth::secrets_match(nonce, &cookie_nonce) {
        return Err(forbidden(
            "The sign-in request did not match this browser. Start again.",
        ));
    }
    let base = cloud
        .base_url(headers)
        .ok_or_else(|| internal("no usable base URL for the OAuth callback"))?;

    let token: GithubToken = cloud
        .http
        .post("https://github.com/login/oauth/access_token")
        .header("Accept", "application/json")
        .form(&[
            ("client_id", app.client_id.as_str()),
            ("client_secret", app.client_secret.as_str()),
            ("code", code.as_str()),
            (
                "redirect_uri",
                &format!("{base}/api/cloud/auth/github/callback"),
            ),
        ])
        .send()
        .await
        .map_err(|e| internal(format!("GitHub token exchange failed: {e}")))?
        .json()
        .await
        .map_err(|e| internal(format!("GitHub token response was unreadable: {e}")))?;
    let access_token = token.access_token.ok_or_else(|| {
        bad_request(format!(
            "GitHub declined the sign-in: {}",
            token
                .error_description
                .unwrap_or_else(|| "no access token was issued".into())
        ))
    })?;

    let profile: GithubUser = cloud
        .http
        .get("https://api.github.com/user")
        .bearer_auth(&access_token)
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .map_err(|e| internal(format!("GitHub profile request failed: {e}")))?
        .json()
        .await
        .map_err(|e| internal(format!("GitHub profile was unreadable: {e}")))?;

    // A GitHub profile may hide its email; ask for the verified primary one.
    let email = match profile.email.clone() {
        Some(email) => Some(email),
        None => primary_verified_email(cloud, &access_token).await,
    };
    let email = super::model::validate_email(&email.unwrap_or_else(|| {
        // Fall back to GitHub's no-reply form, which is always deliverable to
        // the account holder and unique per user.
        format!("{}+{}@users.noreply.github.com", profile.login, profile.id)
    }))
    .map_err(bad_request)?;

    let subject = format!("github:{}", profile.id);
    let account = match cloud.store.find::<super::model::Identity>(&subject).await? {
        Some(identity) => cloud.account(&identity.account_id).await?,
        None => match cloud.store.find::<Account>(&email).await? {
            // Same verified address: link the provider to the existing account
            // rather than creating a second one.
            Some(existing) => {
                link_identity(cloud, &existing, &subject, profile.id).await?;
                existing
            }
            None => {
                let account = Account {
                    id: format!("acc_{}", auth::random_id(16)),
                    email: email.clone(),
                    name: profile
                        .name
                        .clone()
                        .filter(|name| !name.trim().is_empty())
                        .unwrap_or_else(|| profile.login.clone()),
                    password_hash: None,
                    avatar_url: profile.avatar_url.clone(),
                    kind: AccountKind::Standard,
                    created_at: now_ms(),
                    last_seen_at: now_ms(),
                    expires_at: None,
                };
                cloud.store.create(&account).await?;
                link_identity(cloud, &account, &subject, profile.id).await?;
                provision_workspace(cloud, &account, false).await?;
                accept_pending_invites(cloud, &account).await?;
                account
            }
        },
    };

    let (session_token, lifetime) = cloud.start_session(&account, user_agent(headers)).await?;
    let secure = cloud.secure_cookies(headers);
    // Sets the session and clears the short-lived OAuth nonce. Both must reach
    // the browser, which is why this appends rather than returning an array of
    // (SET_COOKIE, _) pairs — that form keeps only the last, and dropping the
    // session bounced a successful sign-in straight back to /login.
    let cookies = auth::cookie_headers([
        auth::cookie(auth::SESSION_COOKIE, &session_token, lifetime, secure),
        auth::cookie(auth::OAUTH_COOKIE, "", 0, secure),
    ])
    .map_err(|e| internal(format!("cookie was not a valid header value: {e}")))?;
    Ok((
        cookies,
        Redirect::temporary(&safe_next(Some(next.to_string()))),
    )
        .into_response())
}

/// Best-effort lookup of a verified primary address. A failure here is not
/// fatal: sign-in falls back to GitHub's no-reply address form.
async fn primary_verified_email(cloud: &Cloud, access_token: &str) -> Option<String> {
    let response = cloud
        .http
        .get("https://api.github.com/user/emails")
        .bearer_auth(access_token)
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?;
    let addresses: Vec<GithubEmail> = response.json().await.ok()?;
    addresses
        .into_iter()
        .filter(|entry| entry.verified)
        .max_by_key(|entry| entry.primary)
        .map(|entry| entry.email)
}

async fn link_identity(
    cloud: &Cloud,
    account: &Account,
    subject: &str,
    github_id: i64,
) -> Result<()> {
    let identity = super::model::Identity {
        id: subject.to_string(),
        account_id: account.id.clone(),
        provider: "github".into(),
        subject: github_id.to_string(),
        created_at: now_ms(),
    };
    match cloud.store.create(&identity).await {
        Ok(()) => Ok(()),
        Err(super::store::StoreError::Conflict(_)) => Err(conflict(
            "That GitHub account is already linked to another FluxDB account",
        )),
        Err(other) => Err(internal(other)),
    }
}

/// Reject cross-site state changes. The session cookie is `SameSite=Lax`, which
/// already blocks cross-site form posts, and this adds an explicit `Origin`
/// check for the browsers and proxies that do not.
pub(super) async fn guard_origin(
    State(cloud): State<CloudState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let mutating = !matches!(
        *request.method(),
        axum::http::Method::GET | axum::http::Method::HEAD | axum::http::Method::OPTIONS
    );
    if mutating {
        if let Some(origin) = request
            .headers()
            .get(axum::http::header::ORIGIN)
            .and_then(|value| value.to_str().ok())
        {
            // API-key traffic is not browser traffic and carries no cookies, so
            // it is exempt.
            let api_key_request = request.uri().path().starts_with("/api/ingest/");
            if !api_key_request && !origin_allowed(&cloud, request.headers(), origin) {
                return forbidden(
                    "This request came from an origin this deployment does not allow",
                )
                .into_response();
            }
        }
    }
    next.run(request).await
}

fn origin_allowed(cloud: &Cloud, headers: &HeaderMap, origin: &str) -> bool {
    if cloud.base_url(headers).as_deref() == Some(origin) {
        return true;
    }
    // The development console is served by Vite on another port and proxies to
    // this server, so the configured browser origins are accepted too.
    crate::api::allowed_origins()
        .iter()
        .any(|allowed| allowed == origin)
}
