//! Control-plane entities.
//!
//! Every record the control plane stores is defined here as a typed struct with
//! its index columns, so uniqueness (email address, organization slug, bucket
//! name inside a project, API key id) is enforced by the store rather than by
//! ad-hoc checks at the call sites.

use super::store::Entity;
use serde::{Deserialize, Serialize};

/// Hosted-plan limits. A workspace that exceeds these is asked to self-host,
/// which the product supports as a first-class path rather than an upsell.
pub mod limits {
    pub const PROJECTS_PER_ORG: i64 = 5;
    pub const BUCKETS_PER_PROJECT: i64 = 8;
    pub const KEYS_PER_PROJECT: i64 = 10;
    pub const MONITORS_PER_PROJECT: i64 = 20;
    pub const DASHBOARDS_PER_PROJECT: i64 = 10;
    pub const POINTS_PER_PROJECT: usize = 2_000_000;
    /// Guest workspaces are reclaimed after this long so the shared demo
    /// instance has a bounded working set.
    pub const GUEST_LIFETIME_SECONDS: i64 = 24 * 60 * 60;
    pub const SESSION_LIFETIME_SECONDS: i64 = 30 * 24 * 60 * 60;
    pub const GUEST_SESSION_LIFETIME_SECONDS: i64 = 24 * 60 * 60;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AccountKind {
    /// Registered with an email address and password, or a linked provider.
    Standard,
    /// One-click demo account. Reclaimed automatically.
    Guest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    /// Always stored lowercased; the lookup column makes it unique.
    pub email: String,
    pub name: String,
    /// Absent for accounts that only ever signed in through a provider.
    pub password_hash: Option<String>,
    pub avatar_url: Option<String>,
    pub kind: AccountKind,
    pub created_at: i64,
    pub last_seen_at: i64,
    /// Set for guests only.
    pub expires_at: Option<i64>,
}

impl Entity for Account {
    const KIND: &'static str = "account";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn lookup(&self) -> Option<String> {
        Some(self.email.clone())
    }
}

impl Account {
    pub fn is_guest(&self) -> bool {
        self.kind == AccountKind::Guest
    }
}

/// A link between an account and an external identity provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    /// `"{provider}:{subject}"`, also the unique lookup value.
    pub id: String,
    pub account_id: String,
    pub provider: String,
    pub subject: String,
    pub created_at: i64,
}

impl Entity for Identity {
    const KIND: &'static str = "identity";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn owner(&self) -> Option<String> {
        Some(self.account_id.clone())
    }
    fn lookup(&self) -> Option<String> {
        Some(self.id.clone())
    }
}

/// A browser session. `id` is the SHA-256 of the cookie value, so a leaked
/// control-plane database does not hand over live sessions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub account_id: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub user_agent: String,
}

impl Entity for Session {
    const KIND: &'static str = "session";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn owner(&self) -> Option<String> {
        Some(self.account_id.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Read dashboards and run queries.
    Viewer,
    /// Write data, manage buckets, dashboards and monitors.
    Member,
    /// Also manage projects, API keys and members.
    Admin,
    /// Also rename or delete the organization.
    Owner,
}

impl Role {
    pub fn can_write(self) -> bool {
        self >= Role::Member
    }
    pub fn can_administer(self) -> bool {
        self >= Role::Admin
    }
    pub fn is_owner(self) -> bool {
        self == Role::Owner
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Org {
    pub id: String,
    pub name: String,
    pub slug: String,
    pub owner_id: String,
    /// `free` for registered accounts, `demo` for guest workspaces.
    pub plan: String,
    pub created_at: i64,
    pub expires_at: Option<i64>,
}

impl Entity for Org {
    const KIND: &'static str = "org";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn owner(&self) -> Option<String> {
        Some(self.owner_id.clone())
    }
    fn lookup(&self) -> Option<String> {
        Some(self.slug.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    pub org_id: String,
    pub account_id: String,
    pub email: String,
    pub name: String,
    pub role: Role,
    pub created_at: i64,
}

impl Entity for Member {
    const KIND: &'static str = "member";
    fn id(&self) -> String {
        format!("{}:{}", self.org_id, self.account_id)
    }
    fn parent(&self) -> Option<String> {
        Some(self.org_id.clone())
    }
    fn owner(&self) -> Option<String> {
        Some(self.account_id.clone())
    }
}

/// A pending invitation for an address that has no account yet. Accepted
/// automatically the first time that address signs up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invite {
    pub id: String,
    pub org_id: String,
    pub org_name: String,
    pub email: String,
    pub role: Role,
    pub invited_by: String,
    pub created_at: i64,
    pub expires_at: Option<i64>,
}

impl Entity for Invite {
    const KIND: &'static str = "invite";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn parent(&self) -> Option<String> {
        Some(self.org_id.clone())
    }
    fn owner(&self) -> Option<String> {
        Some(self.email.clone())
    }
    fn lookup(&self) -> Option<String> {
        Some(format!("{}:{}", self.org_id, self.email))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    /// Twelve lowercase base-32 characters. Also the tenant prefix of every
    /// engine database this project owns, which is what keeps one tenant's SQL
    /// from naming another tenant's data.
    pub id: String,
    pub org_id: String,
    pub name: String,
    pub slug: String,
    #[serde(default)]
    pub description: String,
    pub created_by: String,
    pub created_at: i64,
    /// A shared, read-only showcase project that every visitor can open.
    #[serde(default)]
    pub demo: bool,
}

impl Entity for Project {
    const KIND: &'static str = "project";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn parent(&self) -> Option<String> {
        Some(self.org_id.clone())
    }
    fn lookup(&self) -> Option<String> {
        Some(format!("{}/{}", self.org_id, self.slug))
    }
}

/// A time-series database inside a project. `namespace` is the physical engine
/// database; everything the browser and API show is `name`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub namespace: String,
    pub retention_seconds: u64,
    pub created_by: String,
    pub created_at: i64,
}

impl Entity for Bucket {
    const KIND: &'static str = "bucket";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn parent(&self) -> Option<String> {
        Some(self.project_id.clone())
    }
    fn lookup(&self) -> Option<String> {
        Some(format!("{}/{}", self.project_id, self.name))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Read,
    Write,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    /// Sixteen hex characters, shown in the console and embedded in the token.
    pub id: String,
    pub project_id: String,
    pub name: String,
    /// SHA-256 of the secret half. API key secrets are 256 random bits, so a
    /// password-stretching KDF buys nothing and would add tens of milliseconds
    /// to every ingest request; user passwords use Argon2id instead.
    pub secret_hash: String,
    pub scopes: Vec<Scope>,
    pub created_by: String,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
    pub revoked: bool,
}

impl Entity for ApiKey {
    const KIND: &'static str = "apikey";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn parent(&self) -> Option<String> {
        Some(self.project_id.clone())
    }
    fn lookup(&self) -> Option<String> {
        Some(self.id.clone())
    }
}

impl ApiKey {
    pub fn allows(&self, scope: Scope) -> bool {
        !self.revoked && self.scopes.contains(&scope)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionMode {
    /// The browser talks to the server directly. Nothing transits this host and
    /// no credential is ever sent here.
    Browser,
    /// The control plane forwards requests, with the visitor's token supplied
    /// per request and never stored.
    Proxy,
}

/// A FluxDB server the user runs themselves, registered so the console can
/// target it. Tokens are deliberately absent: see [`ConnectionMode`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connection {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub url: String,
    pub mode: ConnectionMode,
    pub created_by: String,
    pub created_at: i64,
    pub last_checked_at: Option<i64>,
    pub last_status: Option<String>,
}

impl Entity for Connection {
    const KIND: &'static str = "connection";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn parent(&self) -> Option<String> {
        Some(self.project_id.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Comparison {
    Above,
    Below,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MonitorState {
    /// Evaluated and within threshold.
    Ok,
    /// Evaluated and breaching threshold.
    Alerting,
    /// Not evaluated yet, or the query returned no rows.
    Unknown,
}

/// A threshold check evaluated server-side on a fixed interval. This is what
/// turns stored points into something that pages a human.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Monitor {
    pub id: String,
    pub project_id: String,
    pub bucket_id: String,
    pub name: String,
    /// SQL whose first numeric column of the first row is compared against
    /// `threshold`.
    pub query: String,
    pub comparison: Comparison,
    pub threshold: f64,
    pub severity: Severity,
    pub enabled: bool,
    pub state: MonitorState,
    pub last_value: Option<f64>,
    pub last_checked_at: Option<i64>,
    pub last_error: Option<String>,
    pub created_by: String,
    pub created_at: i64,
}

impl Entity for Monitor {
    const KIND: &'static str = "monitor";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn parent(&self) -> Option<String> {
        Some(self.project_id.clone())
    }
}

impl Monitor {
    /// Whether `value` breaches the configured threshold.
    pub fn breaches(&self, value: f64) -> bool {
        match self.comparison {
            Comparison::Above => value > self.threshold,
            Comparison::Below => value < self.threshold,
        }
    }
}

/// A recorded monitor state transition. Only transitions are stored, so a
/// permanently breaching monitor produces one event rather than one per sweep.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub id: String,
    pub monitor_id: String,
    pub project_id: String,
    pub monitor_name: String,
    pub state: MonitorState,
    pub severity: Severity,
    pub value: Option<f64>,
    pub message: String,
    pub at: i64,
}

impl Entity for AlertEvent {
    const KIND: &'static str = "alert";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn parent(&self) -> Option<String> {
        Some(self.project_id.clone())
    }
    fn owner(&self) -> Option<String> {
        Some(self.monitor_id.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PanelKind {
    Line,
    Area,
    Bar,
    Stat,
    Table,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Panel {
    pub id: String,
    pub title: String,
    pub kind: PanelKind,
    pub bucket_id: String,
    pub query: String,
    #[serde(default)]
    pub unit: String,
    /// Grid width in twelfths.
    pub span: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub panels: Vec<Panel>,
    pub created_by: String,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Entity for Dashboard {
    const KIND: &'static str = "dashboard";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn parent(&self) -> Option<String> {
        Some(self.project_id.clone())
    }
}

/// An append-only record of privileged actions, surfaced in the console.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub id: String,
    pub org_id: String,
    pub project_id: Option<String>,
    pub account_id: String,
    pub actor: String,
    /// Dotted action name, for example `bucket.create` or `apikey.revoke`.
    pub action: String,
    pub target: String,
    #[serde(default)]
    pub detail: String,
    pub at: i64,
}

impl Entity for AuditEntry {
    const KIND: &'static str = "audit";
    fn id(&self) -> String {
        self.id.clone()
    }
    fn parent(&self) -> Option<String> {
        Some(self.org_id.clone())
    }
    fn owner(&self) -> Option<String> {
        Some(self.account_id.clone())
    }
}

// ============================================================================
// Validation
// ============================================================================

/// Reject addresses that cannot be delivered to and values that would break the
/// lookup column. Deliberately permissive about the local part; the goal is to
/// catch mistakes, not to re-implement RFC 5322.
pub fn validate_email(email: &str) -> Result<String, String> {
    let email = email.trim().to_ascii_lowercase();
    if email.len() < 3 || email.len() > 254 {
        return Err("Enter an email address between 3 and 254 characters".into());
    }
    let (local, domain) = email
        .split_once('@')
        .ok_or("Enter an email address containing @")?;
    if local.is_empty() || domain.is_empty() {
        return Err("Enter text on both sides of @".into());
    }
    if !domain.contains('.') || domain.starts_with('.') || domain.ends_with('.') {
        return Err("Enter a domain such as example.com".into());
    }
    if email
        .chars()
        .any(|c| c.is_whitespace() || c.is_control() || c == ',' || c == ';')
    {
        return Err("Email addresses cannot contain spaces, commas or semicolons".into());
    }
    Ok(email)
}

/// Length-first password policy. Long passphrases are accepted as-is; short
/// ones must at least mix character classes.
pub fn validate_password(password: &str) -> Result<(), String> {
    if password.chars().count() < 10 {
        return Err("Use a password of at least 10 characters".into());
    }
    if password.len() > 256 {
        return Err("Use a password of at most 256 bytes".into());
    }
    if password.chars().count() >= 16 {
        return Ok(());
    }
    let classes = [
        password.chars().any(char::is_lowercase),
        password.chars().any(char::is_uppercase),
        password.chars().any(|c| c.is_ascii_digit()),
        password.chars().any(|c| !c.is_alphanumeric()),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if classes < 3 {
        return Err(
            "Use 16 or more characters, or mix upper case, lower case, digits and symbols".into(),
        );
    }
    Ok(())
}

/// Human-facing display names for accounts, organizations, projects and panels.
pub fn validate_display_name(name: &str, field: &str) -> Result<String, String> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 64 {
        return Err(format!("Enter a {field} between 1 and 64 characters"));
    }
    if name.chars().any(char::is_control) {
        return Err(format!("A {field} cannot contain control characters"));
    }
    Ok(name.to_string())
}

/// Bucket names travel into engine database names, so they are restricted to
/// the character set the engine accepts and left with room for the tenant
/// prefix.
pub fn validate_bucket_name(name: &str) -> Result<String, String> {
    let name = name.trim();
    if name.is_empty() || name.len() > 48 {
        return Err("Use a bucket name of 1–48 characters".into());
    }
    if !name
        .bytes()
        .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
    {
        return Err("Bucket names may use letters, digits, underscores and hyphens".into());
    }
    if !name.as_bytes()[0].is_ascii_alphanumeric() {
        return Err("Bucket names must start with a letter or digit".into());
    }
    Ok(name.to_string())
}

/// URL-safe identifier derived from a display name.
pub fn slugify(name: &str) -> String {
    let mut slug = String::new();
    let mut previous_dash = false;
    for c in name.trim().to_lowercase().chars() {
        if c.is_ascii_alphanumeric() {
            slug.push(c);
            previous_dash = false;
        } else if !previous_dash && !slug.is_empty() {
            slug.push('-');
            previous_dash = true;
        }
    }
    let slug = slug.trim_end_matches('-').to_string();
    if slug.is_empty() {
        "workspace".to_string()
    } else {
        slug.chars().take(40).collect()
    }
}

/// Physical engine database name for a bucket. The project id prefix is the
/// tenancy boundary.
pub fn namespace_for(project_id: &str, bucket: &str) -> String {
    format!("t{project_id}_{bucket}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_validation_normalizes_and_rejects_malformed_input() {
        assert_eq!(
            validate_email("  Ada@Example.COM ").unwrap(),
            "ada@example.com"
        );
        for bad in [
            "ada",
            "ada@",
            "@example.com",
            "ada@example",
            "ada b@example.com",
            "ada@example.com, eve@example.com",
        ] {
            assert!(validate_email(bad).is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn password_policy_accepts_long_passphrases_and_mixed_short_passwords() {
        assert!(validate_password("correct horse battery staple").is_ok());
        assert!(validate_password("Tr0ubador!x").is_ok());
        assert!(validate_password("short").is_err());
        assert!(validate_password("alllowercase1").is_err());
    }

    #[test]
    fn bucket_names_stay_inside_the_engine_character_set_and_length_budget() {
        assert_eq!(validate_bucket_name(" metrics_1 ").unwrap(), "metrics_1");
        for bad in ["", "has space", "punct!", "_leading", &"x".repeat(49)] {
            assert!(
                validate_bucket_name(bad).is_err(),
                "{bad} should be rejected"
            );
        }
        // Longest permitted namespace stays within the engine's 64-byte limit.
        let namespace = namespace_for("abcdefghijkl", &"b".repeat(48));
        assert_eq!(namespace.len(), 62);
        assert!(fluxdb_core::storage::StorageEngine::validate_name(&namespace).is_ok());
    }

    #[test]
    fn slugify_produces_stable_url_safe_identifiers() {
        assert_eq!(slugify("Acme Observability!"), "acme-observability");
        assert_eq!(slugify("  ---  "), "workspace");
        // Characters outside ASCII are not transliterated; they act as
        // separators. Slugs are cosmetic, and uniqueness is enforced by the
        // store's index rather than by the slug's shape.
        assert_eq!(slugify("Ünïcode ✨ text"), "n-code-text");
        assert!(slugify(&"long name ".repeat(20)).len() <= 40);
    }

    #[test]
    fn roles_order_from_least_to_most_privileged() {
        assert!(!Role::Viewer.can_write());
        assert!(Role::Member.can_write());
        assert!(!Role::Member.can_administer());
        assert!(Role::Admin.can_administer());
        assert!(!Role::Admin.is_owner());
        assert!(Role::Owner.is_owner() && Role::Owner.can_write());
    }

    #[test]
    fn monitor_threshold_comparison_respects_direction() {
        let mut monitor = Monitor {
            id: "m".into(),
            project_id: "p".into(),
            bucket_id: "b".into(),
            name: "CPU".into(),
            query: "SELECT MEAN(usage) FROM cpu".into(),
            comparison: Comparison::Above,
            threshold: 80.0,
            severity: Severity::Critical,
            enabled: true,
            state: MonitorState::Unknown,
            last_value: None,
            last_checked_at: None,
            last_error: None,
            created_by: "a".into(),
            created_at: 0,
        };
        assert!(monitor.breaches(80.1));
        assert!(!monitor.breaches(80.0));
        monitor.comparison = Comparison::Below;
        assert!(monitor.breaches(79.9));
        assert!(!monitor.breaches(80.0));
    }
}
