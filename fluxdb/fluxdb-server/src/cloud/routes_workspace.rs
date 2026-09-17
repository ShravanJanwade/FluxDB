//! Organizations, projects, buckets, API keys, members, dashboards, monitors
//! and the audit trail.

use super::model::{
    limits, Account, AlertEvent, ApiKey, AuditEntry, Bucket, Comparison, Connection,
    ConnectionMode, Dashboard, Invite, Member, Monitor, MonitorState, Panel, PanelKind, Project,
    Role, Scope, Severity,
};
use super::store::Entity;
use super::{
    auth, bad_request, conflict, forbidden, internal, not_found, now_ms, seed, Actor, Cloud,
    CloudState, Result,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

pub fn routes() -> Router<CloudState> {
    Router::new()
        .route(
            "/api/cloud/orgs/:org",
            get(org_detail).patch(rename_org).delete(remove_org),
        )
        .route("/api/cloud/orgs/:org/projects", post(create_project))
        .route(
            "/api/cloud/orgs/:org/members",
            get(list_members).post(add_member),
        )
        .route(
            "/api/cloud/orgs/:org/members/:account",
            axum::routing::patch(update_member).delete(remove_member),
        )
        .route("/api/cloud/orgs/:org/audit", get(audit_trail))
        .route(
            "/api/cloud/projects/:project",
            get(project_detail)
                .patch(update_project)
                .delete(remove_project),
        )
        .route("/api/cloud/projects/:project/buckets", post(create_bucket))
        .route(
            "/api/cloud/projects/:project/buckets/:bucket",
            axum::routing::patch(update_bucket).delete(remove_bucket),
        )
        .route(
            "/api/cloud/projects/:project/buckets/:bucket/sample",
            post(load_sample_data),
        )
        .route(
            "/api/cloud/projects/:project/keys",
            get(list_keys).post(create_key),
        )
        .route(
            "/api/cloud/projects/:project/keys/:key",
            axum::routing::delete(revoke_key),
        )
        .route(
            "/api/cloud/projects/:project/connections",
            get(list_connections).post(create_connection),
        )
        .route(
            "/api/cloud/projects/:project/connections/:connection",
            axum::routing::delete(remove_connection),
        )
        .route(
            "/api/cloud/projects/:project/dashboards",
            get(list_dashboards).post(create_dashboard),
        )
        .route(
            "/api/cloud/projects/:project/dashboards/:dashboard",
            axum::routing::put(save_dashboard).delete(remove_dashboard),
        )
        .route(
            "/api/cloud/projects/:project/monitors",
            get(list_monitors).post(create_monitor),
        )
        .route(
            "/api/cloud/projects/:project/monitors/:monitor",
            axum::routing::patch(update_monitor).delete(remove_monitor),
        )
        .route("/api/cloud/projects/:project/alerts", get(list_alerts))
        .route("/api/cloud/projects/:project/examples", get(examples))
}

// ============================================================================
// Shared views
// ============================================================================

/// Bucket view including live engine statistics, so the console never has to
/// guess at storage size or point counts.
fn bucket_json(cloud: &Cloud, bucket: &Bucket) -> Value {
    let stats = cloud
        .engine
        .get_database(&bucket.namespace)
        .map(|db| db.stats());
    json!({
        "id": bucket.id,
        "name": bucket.name,
        "retention_seconds": bucket.retention_seconds,
        "created_at": bucket.created_at,
        "points": stats.as_ref().map(|s| s.total_entries).unwrap_or(0),
        "size_bytes": stats.as_ref().map(|s| s.total_size_bytes).unwrap_or(0),
        "sstables": stats.as_ref().map(|s| s.sstables).unwrap_or(0),
        "memtable_bytes": stats.as_ref().map(|s| s.memtable_size).unwrap_or(0),
    })
}

/// API keys are never returned in full. The secret exists exactly once, in the
/// response to the request that created it.
fn key_json(key: &ApiKey) -> Value {
    json!({
        "id": key.id,
        "name": key.name,
        "scopes": key.scopes,
        "created_at": key.created_at,
        "last_used_at": key.last_used_at,
        "revoked": key.revoked,
        "masked_token": format!("fdbk_{}_{}", key.id, "•".repeat(12)),
    })
}

fn project_json(project: &Project, role: Role) -> Value {
    json!({
        "id": project.id,
        "org_id": project.org_id,
        "name": project.name,
        "slug": project.slug,
        "description": project.description,
        "demo": project.demo,
        "created_at": project.created_at,
        "role": role,
        "writable": role.can_write() && !project.demo,
        "administrable": role.can_administer() && !project.demo,
    })
}

// ============================================================================
// Organizations
// ============================================================================

async fn org_detail(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(org_id): Path<String>,
) -> Result<Json<Value>> {
    let membership = cloud.membership(&org_id, &actor.account.id).await?;
    let org = cloud.org(&org_id).await?;
    let projects = cloud.store.list_by_parent::<Project>(&org.id).await?;
    let members = cloud.store.count::<Member>(Some(&org.id)).await?;
    let mut points = 0usize;
    let mut buckets = 0usize;
    for project in &projects {
        points += cloud.project_points(&project.id);
        buckets += cloud.store.count::<Bucket>(Some(&project.id)).await? as usize;
    }
    Ok(Json(json!({
        "id": org.id,
        "name": org.name,
        "slug": org.slug,
        "plan": org.plan,
        "role": membership.role,
        "created_at": org.created_at,
        "expires_at": org.expires_at,
        "is_demo": org.id == super::DEMO_ORG_ID,
        "members": members,
        "usage": {
            "projects": projects.len(),
            "projects_limit": limits::PROJECTS_PER_ORG,
            "buckets": buckets,
            "points": points,
            "points_limit": limits::POINTS_PER_PROJECT,
        },
        "projects": projects.iter().map(|project| project_json(project, membership.role)).collect::<Vec<_>>(),
    })))
}

#[derive(Deserialize)]
struct NameRequest {
    name: String,
}

async fn rename_org(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(org_id): Path<String>,
    Json(request): Json<NameRequest>,
) -> Result<Json<Value>> {
    let membership = cloud.membership(&org_id, &actor.account.id).await?;
    if !membership.role.is_owner() {
        return Err(forbidden("Only the workspace owner can rename it"));
    }
    let mut org = cloud.org(&org_id).await?;
    if org.id == super::DEMO_ORG_ID {
        return Err(forbidden("The shared demo workspace cannot be renamed"));
    }
    org.name = super::model::validate_display_name(&request.name, "workspace name")
        .map_err(bad_request)?;
    cloud.store.save(&org).await?;
    cloud
        .audit(&actor, &org.id, None, "org.rename", &org.name, "")
        .await;
    Ok(Json(json!({"id": org.id, "name": org.name})))
}

async fn remove_org(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(org_id): Path<String>,
) -> Result<StatusCode> {
    let membership = cloud.membership(&org_id, &actor.account.id).await?;
    if !membership.role.is_owner() {
        return Err(forbidden("Only the workspace owner can delete it"));
    }
    let org = cloud.org(&org_id).await?;
    if org.id == super::DEMO_ORG_ID {
        return Err(forbidden("The shared demo workspace cannot be deleted"));
    }
    // Refuse to leave the account with nothing: every account keeps at least
    // one workspace of its own.
    let owned = cloud
        .store
        .list_by_owner::<Member>(&actor.account.id)
        .await?
        .into_iter()
        .filter(|member| member.role == Role::Owner)
        .count();
    if owned <= 1 {
        return Err(conflict(
            "This is your only workspace. Create another one before deleting it.",
        ));
    }
    cloud.delete_org(&org).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct ProjectRequest {
    name: String,
    #[serde(default)]
    description: String,
}

async fn create_project(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(org_id): Path<String>,
    Json(request): Json<ProjectRequest>,
) -> Result<(StatusCode, Json<Value>)> {
    let membership = cloud.membership(&org_id, &actor.account.id).await?;
    if !membership.role.can_administer() {
        return Err(forbidden(
            "Only workspace admins and owners can create projects",
        ));
    }
    if org_id == super::DEMO_ORG_ID {
        return Err(forbidden(
            "Projects cannot be added to the shared demo workspace",
        ));
    }
    let name =
        super::model::validate_display_name(&request.name, "project name").map_err(bad_request)?;
    cloud
        .check_quota::<Project>(&org_id, limits::PROJECTS_PER_ORG, "projects")
        .await?;
    let project = cloud
        .create_project(
            &actor.account.id,
            &org_id,
            &name,
            request.description.trim(),
        )
        .await?;
    cloud
        .audit(
            &actor,
            &org_id,
            Some(&project.id),
            "project.create",
            &project.name,
            "",
        )
        .await;
    Ok((
        StatusCode::CREATED,
        Json(project_json(&project, membership.role)),
    ))
}

// ============================================================================
// Members
// ============================================================================

async fn list_members(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(org_id): Path<String>,
) -> Result<Json<Value>> {
    let membership = cloud.membership(&org_id, &actor.account.id).await?;
    let members = cloud.store.list_by_parent::<Member>(&org_id).await?;
    let invites = cloud.store.list_by_parent::<Invite>(&org_id).await?;
    Ok(Json(json!({
        "role": membership.role,
        "members": members.iter().map(|member| json!({
            "account_id": member.account_id,
            "email": member.email,
            "name": member.name,
            "role": member.role,
            "created_at": member.created_at,
            "is_you": member.account_id == actor.account.id,
        })).collect::<Vec<_>>(),
        "invites": invites.iter().map(|invite| json!({
            "id": invite.id,
            "email": invite.email,
            "role": invite.role,
            "created_at": invite.created_at,
        })).collect::<Vec<_>>(),
    })))
}

#[derive(Deserialize)]
struct MemberRequest {
    email: String,
    role: Role,
}

async fn add_member(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(org_id): Path<String>,
    Json(request): Json<MemberRequest>,
) -> Result<(StatusCode, Json<Value>)> {
    let membership = cloud.membership(&org_id, &actor.account.id).await?;
    if !membership.role.can_administer() {
        return Err(forbidden("Only admins and owners can invite members"));
    }
    actor.requires_account()?;
    if org_id == super::DEMO_ORG_ID {
        return Err(forbidden(
            "Membership of the shared demo workspace is automatic",
        ));
    }
    if request.role == Role::Owner {
        return Err(bad_request(
            "Transfer ownership from workspace settings instead of inviting a second owner",
        ));
    }
    let email = super::model::validate_email(&request.email).map_err(bad_request)?;
    let org = cloud.org(&org_id).await?;

    // An existing account joins immediately; anyone else gets an invitation
    // that is redeemed the first time they sign up with this address. There is
    // no mail delivery on this deployment, so the invitation link is shown to
    // the inviter to pass on.
    if let Some(account) = cloud.store.find::<Account>(&email).await? {
        if cloud
            .store
            .get::<Member>(&format!("{org_id}:{}", account.id))
            .await?
            .is_some()
        {
            return Err(conflict("That person is already a member"));
        }
        let member = Member {
            org_id: org_id.clone(),
            account_id: account.id.clone(),
            email: account.email.clone(),
            name: account.name.clone(),
            role: request.role,
            created_at: now_ms(),
        };
        cloud.store.create(&member).await?;
        cloud
            .audit(
                &actor,
                &org_id,
                None,
                "member.add",
                &email,
                "joined directly",
            )
            .await;
        return Ok((
            StatusCode::CREATED,
            Json(json!({"status": "added", "email": email, "role": request.role})),
        ));
    }
    let invite = Invite {
        id: format!("inv_{}", auth::random_id(14)),
        org_id: org_id.clone(),
        org_name: org.name.clone(),
        email: email.clone(),
        role: request.role,
        invited_by: actor.account.email.clone(),
        created_at: now_ms(),
        expires_at: Some(now_ms() + 14 * 24 * 60 * 60 * 1000),
    };
    match cloud.store.create(&invite).await {
        Ok(()) => {}
        Err(super::store::StoreError::Conflict(_)) => {
            return Err(conflict("That address has already been invited"))
        }
        Err(other) => return Err(internal(other)),
    }
    cloud
        .audit(&actor, &org_id, None, "member.invite", &email, "")
        .await;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "status": "invited",
            "email": email,
            "role": request.role,
            "note": "This deployment does not send email. Share the sign-up link; the invitation is applied when they register with this address.",
        })),
    ))
}

#[derive(Deserialize)]
struct RoleRequest {
    role: Role,
}

async fn update_member(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((org_id, account_id)): Path<(String, String)>,
    Json(request): Json<RoleRequest>,
) -> Result<Json<Value>> {
    let membership = cloud.membership(&org_id, &actor.account.id).await?;
    if !membership.role.can_administer() {
        return Err(forbidden("Only admins and owners can change roles"));
    }
    let mut target = cloud.membership(&org_id, &account_id).await?;
    if target.role == Role::Owner {
        return Err(forbidden("The workspace owner's role cannot be changed"));
    }
    if request.role == Role::Owner {
        return Err(bad_request(
            "Use transfer of ownership rather than promoting a second owner",
        ));
    }
    target.role = request.role;
    cloud.store.save(&target).await?;
    cloud
        .audit(
            &actor,
            &org_id,
            None,
            "member.role",
            &target.email,
            &format!("{:?}", request.role),
        )
        .await;
    Ok(Json(
        json!({"account_id": account_id, "role": request.role}),
    ))
}

async fn remove_member(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((org_id, account_id)): Path<(String, String)>,
) -> Result<StatusCode> {
    let membership = cloud.membership(&org_id, &actor.account.id).await?;
    // Anyone may remove themselves; removing someone else needs admin rights.
    if account_id != actor.account.id && !membership.role.can_administer() {
        return Err(forbidden("Only admins and owners can remove members"));
    }
    let target = cloud.membership(&org_id, &account_id).await?;
    if target.role == Role::Owner {
        return Err(forbidden(
            "The workspace owner cannot be removed. Transfer ownership or delete the workspace.",
        ));
    }
    cloud.store.delete::<Member>(&target.id()).await?;
    cloud
        .audit(&actor, &org_id, None, "member.remove", &target.email, "")
        .await;
    Ok(StatusCode::NO_CONTENT)
}

async fn audit_trail(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(org_id): Path<String>,
) -> Result<Json<Value>> {
    let membership = cloud.membership(&org_id, &actor.account.id).await?;
    if !membership.role.can_administer() {
        return Err(forbidden("Only admins and owners can read the audit trail"));
    }
    let entries = cloud
        .store
        .recent_by_parent::<AuditEntry>(&org_id, 200)
        .await?;
    Ok(Json(json!({
        "entries": entries.iter().map(|entry| json!({
            "id": entry.id,
            "at": entry.at,
            "actor": entry.actor,
            "action": entry.action,
            "target": entry.target,
            "detail": entry.detail,
            "project_id": entry.project_id,
        })).collect::<Vec<_>>(),
    })))
}

// ============================================================================
// Projects
// ============================================================================

async fn project_detail(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
) -> Result<Json<Value>> {
    let (project, role) = cloud.project_role(&project_id, &actor).await?;
    let org = cloud.org(&project.org_id).await?;
    let buckets = cloud.store.list_by_parent::<Bucket>(&project.id).await?;
    let keys = cloud.store.list_by_parent::<ApiKey>(&project.id).await?;
    let connections = cloud
        .store
        .list_by_parent::<Connection>(&project.id)
        .await?;
    let monitors = cloud.store.list_by_parent::<Monitor>(&project.id).await?;
    let alerts = cloud
        .store
        .recent_by_parent::<AlertEvent>(&project.id, 25)
        .await?;
    let points = cloud.project_points(&project.id);
    Ok(Json(json!({
        "project": project_json(&project, role),
        "org": {"id": org.id, "name": org.name, "slug": org.slug, "plan": org.plan},
        "buckets": buckets.iter().map(|bucket| bucket_json(&cloud, bucket)).collect::<Vec<_>>(),
        "keys": keys.iter().filter(|key| !key.revoked).map(key_json).collect::<Vec<_>>(),
        "connections": connections.iter().map(connection_json).collect::<Vec<_>>(),
        "monitors": monitors.iter().map(monitor_json).collect::<Vec<_>>(),
        "alerts": alerts.iter().map(alert_json).collect::<Vec<_>>(),
        "usage": {
            "points": points,
            "points_limit": limits::POINTS_PER_PROJECT,
            "buckets": buckets.len(),
            "buckets_limit": limits::BUCKETS_PER_PROJECT,
            "size_bytes": buckets.iter().filter_map(|bucket| {
                cloud.engine.get_database(&bucket.namespace).map(|db| db.stats().total_size_bytes)
            }).sum::<u64>(),
        },
    })))
}

#[derive(Deserialize)]
struct ProjectUpdate {
    name: Option<String>,
    description: Option<String>,
}

async fn update_project(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
    Json(request): Json<ProjectUpdate>,
) -> Result<Json<Value>> {
    let mut project = cloud.project_admin(&project_id, &actor).await?;
    if let Some(name) = request.name {
        project.name =
            super::model::validate_display_name(&name, "project name").map_err(bad_request)?;
    }
    if let Some(description) = request.description {
        if description.chars().count() > 280 {
            return Err(bad_request("Keep the description under 280 characters"));
        }
        project.description = description.trim().to_string();
    }
    cloud.store.save(&project).await?;
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "project.update",
            &project.name,
            "",
        )
        .await;
    let (_, role) = cloud.project_role(&project_id, &actor).await?;
    Ok(Json(project_json(&project, role)))
}

async fn remove_project(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
) -> Result<StatusCode> {
    let project = cloud.project_admin(&project_id, &actor).await?;
    let remaining = cloud.store.count::<Project>(Some(&project.org_id)).await?;
    if remaining <= 1 {
        return Err(conflict(
            "A workspace keeps at least one project. Create another before deleting this one.",
        ));
    }
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "project.delete",
            &project.name,
            "all buckets and data removed",
        )
        .await;
    cloud.delete_project(&project).await?;
    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Buckets
// ============================================================================

#[derive(Deserialize)]
struct BucketRequest {
    name: String,
    #[serde(default)]
    retention_seconds: u64,
}

async fn create_bucket(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
    Json(request): Json<BucketRequest>,
) -> Result<(StatusCode, Json<Value>)> {
    let project = cloud.project_writable(&project_id, &actor).await?;
    cloud
        .check_quota::<Bucket>(&project.id, limits::BUCKETS_PER_PROJECT, "buckets")
        .await?;
    let bucket = match cloud
        .create_bucket(
            &actor.account.id,
            &project,
            &request.name,
            request.retention_seconds,
        )
        .await
    {
        Ok(bucket) => bucket,
        Err(failure) if failure.status == StatusCode::CONFLICT => {
            return Err(conflict("This project already has a bucket with that name"))
        }
        Err(failure) => return Err(failure),
    };
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "bucket.create",
            &bucket.name,
            "",
        )
        .await;
    Ok((StatusCode::CREATED, Json(bucket_json(&cloud, &bucket))))
}

#[derive(Deserialize)]
struct RetentionRequest {
    retention_seconds: u64,
}

async fn update_bucket(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, bucket_id)): Path<(String, String)>,
    Json(request): Json<RetentionRequest>,
) -> Result<Json<Value>> {
    let project = cloud.project_writable(&project_id, &actor).await?;
    let mut bucket = cloud.bucket(&project, &bucket_id).await?;
    // Ten years is the practical ceiling; anything larger is indistinguishable
    // from "keep forever", which is what zero already means.
    if request.retention_seconds > 10 * 365 * 24 * 60 * 60 {
        return Err(bad_request(
            "Use a retention period of up to 10 years, or 0 to keep data indefinitely",
        ));
    }
    bucket.retention_seconds = request.retention_seconds;
    let db = cloud.open_bucket(&bucket)?;
    db.set_retention(request.retention_seconds)
        .map_err(|e| bad_request(e.to_string()))?;
    cloud.store.save(&bucket).await?;
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "bucket.retention",
            &bucket.name,
            &format!("{} seconds", request.retention_seconds),
        )
        .await;
    Ok(Json(bucket_json(&cloud, &bucket)))
}

async fn remove_bucket(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, bucket_id)): Path<(String, String)>,
) -> Result<StatusCode> {
    let project = cloud.project_admin(&project_id, &actor).await?;
    let bucket = cloud.bucket(&project, &bucket_id).await?;
    // Monitors pointing at a deleted bucket would evaluate forever against
    // nothing, so they go with it.
    for monitor in cloud.store.list_by_parent::<Monitor>(&project.id).await? {
        if monitor.bucket_id == bucket.id {
            let _ = cloud.store.delete::<Monitor>(&monitor.id).await;
        }
    }
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "bucket.delete",
            &bucket.name,
            "data removed",
        )
        .await;
    cloud.delete_bucket(&bucket).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Fill an empty bucket with the sample fleet dataset, so a new account can see
/// working dashboards without wiring up an agent first.
async fn load_sample_data(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, bucket_id)): Path<(String, String)>,
) -> Result<Json<Value>> {
    let project = cloud.project_writable(&project_id, &actor).await?;
    let bucket = cloud.bucket(&project, &bucket_id).await?;
    let db = cloud.open_bucket(&bucket)?;
    if db.stats().total_entries > 0 {
        return Err(conflict(
            "This bucket already holds data. Load the sample into an empty bucket.",
        ));
    }
    let written = tokio::task::spawn_blocking(move || seed::seed_sandbox(&db))
        .await
        .map_err(internal)?
        .map_err(internal)?;
    // A dashboard and monitors make the sample immediately legible.
    if cloud
        .store
        .list_by_parent::<Dashboard>(&project.id)
        .await?
        .is_empty()
    {
        let _ = cloud
            .store
            .create(&seed::demo_dashboard(&project.id, &bucket.id))
            .await;
    }
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "bucket.sample",
            &bucket.name,
            &format!("{written} points"),
        )
        .await;
    Ok(Json(
        json!({"written": written, "bucket": bucket_json(&cloud, &bucket)}),
    ))
}

// ============================================================================
// API keys
// ============================================================================

async fn list_keys(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
) -> Result<Json<Value>> {
    let (project, role) = cloud.project_role(&project_id, &actor).await?;
    if !role.can_administer() {
        return Err(forbidden("Only admins and owners can manage API keys"));
    }
    let keys = cloud.store.list_by_parent::<ApiKey>(&project.id).await?;
    Ok(Json(json!({
        "keys": keys.iter().map(key_json).collect::<Vec<_>>(),
    })))
}

#[derive(Deserialize)]
struct KeyRequest {
    name: String,
    #[serde(default)]
    scopes: Vec<Scope>,
}

async fn create_key(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
    Json(request): Json<KeyRequest>,
) -> Result<(StatusCode, Json<Value>)> {
    let project = cloud.project_admin(&project_id, &actor).await?;
    cloud
        .check_quota::<ApiKey>(&project.id, limits::KEYS_PER_PROJECT, "API keys")
        .await?;
    let name =
        super::model::validate_display_name(&request.name, "key name").map_err(bad_request)?;
    let scopes = if request.scopes.is_empty() {
        vec![Scope::Read, Scope::Write]
    } else {
        request.scopes.clone()
    };
    let secret = auth::random_token(32);
    let key = ApiKey {
        id: auth::random_hex(8),
        project_id: project.id.clone(),
        name,
        secret_hash: auth::sha256_hex(&secret),
        scopes,
        created_by: actor.account.id.clone(),
        created_at: now_ms(),
        last_used_at: None,
        revoked: false,
    };
    cloud.store.create(&key).await?;
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "apikey.create",
            &key.name,
            &key.id,
        )
        .await;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "key": key_json(&key),
            // Shown exactly once. Only its SHA-256 is stored.
            "token": auth::format_key_token(&key.id, &secret),
            "note": "Copy this token now. It is not stored and cannot be shown again.",
        })),
    ))
}

async fn revoke_key(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, key_id)): Path<(String, String)>,
) -> Result<StatusCode> {
    let project = cloud.project_admin(&project_id, &actor).await?;
    let mut key = cloud
        .store
        .get::<ApiKey>(&key_id)
        .await?
        .filter(|key| key.project_id == project.id)
        .ok_or_else(|| not_found("API key not found"))?;
    // Marked revoked rather than deleted, so the audit trail and "last used"
    // history of a leaked key survive its revocation.
    key.revoked = true;
    cloud.store.save(&key).await?;
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "apikey.revoke",
            &key.name,
            &key.id,
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Self-hosted connections
// ============================================================================

fn connection_json(connection: &Connection) -> Value {
    json!({
        "id": connection.id,
        "name": connection.name,
        "url": connection.url,
        "mode": connection.mode,
        "created_at": connection.created_at,
        "last_checked_at": connection.last_checked_at,
        "last_status": connection.last_status,
    })
}

async fn list_connections(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
) -> Result<Json<Value>> {
    let (project, _) = cloud.project_role(&project_id, &actor).await?;
    let connections = cloud
        .store
        .list_by_parent::<Connection>(&project.id)
        .await?;
    Ok(Json(json!({
        "connections": connections.iter().map(connection_json).collect::<Vec<_>>(),
    })))
}

#[derive(Deserialize)]
struct ConnectionRequest {
    name: String,
    url: String,
    mode: ConnectionMode,
}

async fn create_connection(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
    Json(request): Json<ConnectionRequest>,
) -> Result<(StatusCode, Json<Value>)> {
    let project = cloud.project_writable(&project_id, &actor).await?;
    let name = super::model::validate_display_name(&request.name, "connection name")
        .map_err(bad_request)?;
    let url = super::routes_data::normalize_target(&request.url, request.mode)?;
    if request.mode == ConnectionMode::Proxy {
        // Screened here as well as at forward time, so an unusable target is
        // rejected while the user is still looking at the form.
        super::routes_data::ensure_public_target(&url).await?;
    }
    let connection = Connection {
        id: format!("cnx_{}", auth::random_id(12)),
        project_id: project.id.clone(),
        name,
        url,
        mode: request.mode,
        created_by: actor.account.id.clone(),
        created_at: now_ms(),
        last_checked_at: None,
        last_status: None,
    };
    cloud.store.create(&connection).await?;
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "connection.create",
            &connection.name,
            &connection.url,
        )
        .await;
    Ok((StatusCode::CREATED, Json(connection_json(&connection))))
}

async fn remove_connection(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, connection_id)): Path<(String, String)>,
) -> Result<StatusCode> {
    let project = cloud.project_writable(&project_id, &actor).await?;
    let connection = cloud
        .store
        .get::<Connection>(&connection_id)
        .await?
        .filter(|connection| connection.project_id == project.id)
        .ok_or_else(|| not_found("Connection not found"))?;
    cloud.store.delete::<Connection>(&connection.id).await?;
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "connection.delete",
            &connection.name,
            "",
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Dashboards
// ============================================================================

fn dashboard_json(dashboard: &Dashboard) -> Value {
    json!({
        "id": dashboard.id,
        "name": dashboard.name,
        "panels": dashboard.panels,
        "created_at": dashboard.created_at,
        "updated_at": dashboard.updated_at,
    })
}

async fn list_dashboards(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
) -> Result<Json<Value>> {
    let (project, _) = cloud.project_role(&project_id, &actor).await?;
    let dashboards = cloud.store.list_by_parent::<Dashboard>(&project.id).await?;
    Ok(Json(json!({
        "dashboards": dashboards.iter().map(dashboard_json).collect::<Vec<_>>(),
    })))
}

#[derive(Deserialize)]
struct DashboardRequest {
    name: String,
    #[serde(default)]
    panels: Vec<PanelRequest>,
}

#[derive(Deserialize)]
struct PanelRequest {
    title: String,
    kind: PanelKind,
    bucket_id: String,
    query: String,
    #[serde(default)]
    unit: String,
    #[serde(default = "default_span")]
    span: u8,
}

fn default_span() -> u8 {
    6
}

fn build_panels(requests: Vec<PanelRequest>) -> Result<Vec<Panel>> {
    if requests.len() > 24 {
        return Err(bad_request("A dashboard holds at most 24 panels"));
    }
    requests
        .into_iter()
        .map(|request| {
            Ok(Panel {
                id: format!("pnl_{}", auth::random_id(10)),
                title: super::model::validate_display_name(&request.title, "panel title")
                    .map_err(bad_request)?,
                kind: request.kind,
                bucket_id: request.bucket_id,
                query: {
                    if request.query.trim().is_empty() || request.query.len() > 4096 {
                        return Err(bad_request("A panel query must be 1–4096 characters"));
                    }
                    request.query
                },
                unit: request.unit.chars().take(12).collect(),
                span: request.span.clamp(3, 12),
            })
        })
        .collect()
}

async fn create_dashboard(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
    Json(request): Json<DashboardRequest>,
) -> Result<(StatusCode, Json<Value>)> {
    let project = cloud.project_writable(&project_id, &actor).await?;
    cloud
        .check_quota::<Dashboard>(&project.id, limits::DASHBOARDS_PER_PROJECT, "dashboards")
        .await?;
    let dashboard = Dashboard {
        id: format!("dsh_{}", auth::random_id(12)),
        project_id: project.id.clone(),
        name: super::model::validate_display_name(&request.name, "dashboard name")
            .map_err(bad_request)?,
        panels: build_panels(request.panels)?,
        created_by: actor.account.id.clone(),
        created_at: now_ms(),
        updated_at: now_ms(),
    };
    cloud.store.create(&dashboard).await?;
    Ok((StatusCode::CREATED, Json(dashboard_json(&dashboard))))
}

async fn save_dashboard(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, dashboard_id)): Path<(String, String)>,
    Json(request): Json<DashboardRequest>,
) -> Result<Json<Value>> {
    let project = cloud.project_writable(&project_id, &actor).await?;
    let mut dashboard = cloud
        .store
        .get::<Dashboard>(&dashboard_id)
        .await?
        .filter(|dashboard| dashboard.project_id == project.id)
        .ok_or_else(|| not_found("Dashboard not found"))?;
    dashboard.name = super::model::validate_display_name(&request.name, "dashboard name")
        .map_err(bad_request)?;
    dashboard.panels = build_panels(request.panels)?;
    dashboard.updated_at = now_ms();
    cloud.store.save(&dashboard).await?;
    Ok(Json(dashboard_json(&dashboard)))
}

async fn remove_dashboard(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, dashboard_id)): Path<(String, String)>,
) -> Result<StatusCode> {
    let project = cloud.project_writable(&project_id, &actor).await?;
    let dashboard = cloud
        .store
        .get::<Dashboard>(&dashboard_id)
        .await?
        .filter(|dashboard| dashboard.project_id == project.id)
        .ok_or_else(|| not_found("Dashboard not found"))?;
    cloud.store.delete::<Dashboard>(&dashboard.id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Monitors and alerts
// ============================================================================

fn monitor_json(monitor: &Monitor) -> Value {
    json!({
        "id": monitor.id,
        "name": monitor.name,
        "bucket_id": monitor.bucket_id,
        "query": monitor.query,
        "comparison": monitor.comparison,
        "threshold": monitor.threshold,
        "severity": monitor.severity,
        "enabled": monitor.enabled,
        "state": monitor.state,
        "last_value": monitor.last_value,
        "last_checked_at": monitor.last_checked_at,
        "last_error": monitor.last_error,
        "created_at": monitor.created_at,
    })
}

fn alert_json(alert: &AlertEvent) -> Value {
    json!({
        "id": alert.id,
        "monitor_id": alert.monitor_id,
        "monitor_name": alert.monitor_name,
        "state": alert.state,
        "severity": alert.severity,
        "value": alert.value,
        "message": alert.message,
        "at": alert.at,
    })
}

async fn list_monitors(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
) -> Result<Json<Value>> {
    let (project, _) = cloud.project_role(&project_id, &actor).await?;
    let monitors = cloud.store.list_by_parent::<Monitor>(&project.id).await?;
    Ok(Json(json!({
        "monitors": monitors.iter().map(monitor_json).collect::<Vec<_>>(),
    })))
}

#[derive(Deserialize)]
struct MonitorRequest {
    name: String,
    bucket_id: String,
    query: String,
    comparison: Comparison,
    threshold: f64,
    #[serde(default = "default_severity")]
    severity: Severity,
}

fn default_severity() -> Severity {
    Severity::Warning
}

async fn create_monitor(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
    Json(request): Json<MonitorRequest>,
) -> Result<(StatusCode, Json<Value>)> {
    let project = cloud.project_writable(&project_id, &actor).await?;
    cloud
        .check_quota::<Monitor>(&project.id, limits::MONITORS_PER_PROJECT, "monitors")
        .await?;
    let bucket = cloud.bucket(&project, &request.bucket_id).await?;
    if !request.threshold.is_finite() {
        return Err(bad_request("Enter a finite threshold"));
    }
    // The query is validated by running it once, so a monitor cannot be saved
    // in a state where it silently never evaluates.
    let db = cloud.open_bucket(&bucket)?;
    let probe = request.query.clone();
    tokio::task::spawn_blocking(move || crate::api::data::scalar(&db, &probe))
        .await
        .map_err(internal)?
        .map_err(|e| {
            bad_request(format!(
                "This query cannot be used for a monitor: {}",
                e.message()
            ))
        })?;
    let monitor = Monitor {
        id: format!("mon_{}", auth::random_id(12)),
        project_id: project.id.clone(),
        bucket_id: bucket.id.clone(),
        name: super::model::validate_display_name(&request.name, "monitor name")
            .map_err(bad_request)?,
        query: request.query,
        comparison: request.comparison,
        threshold: request.threshold,
        severity: request.severity,
        enabled: true,
        state: MonitorState::Unknown,
        last_value: None,
        last_checked_at: None,
        last_error: None,
        created_by: actor.account.id.clone(),
        created_at: now_ms(),
    };
    cloud.store.create(&monitor).await?;
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "monitor.create",
            &monitor.name,
            "",
        )
        .await;
    Ok((StatusCode::CREATED, Json(monitor_json(&monitor))))
}

#[derive(Deserialize)]
struct MonitorUpdate {
    enabled: Option<bool>,
    threshold: Option<f64>,
    comparison: Option<Comparison>,
    severity: Option<Severity>,
}

async fn update_monitor(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, monitor_id)): Path<(String, String)>,
    Json(request): Json<MonitorUpdate>,
) -> Result<Json<Value>> {
    let project = cloud.project_writable(&project_id, &actor).await?;
    let mut monitor = cloud
        .store
        .get::<Monitor>(&monitor_id)
        .await?
        .filter(|monitor| monitor.project_id == project.id)
        .ok_or_else(|| not_found("Monitor not found"))?;
    if let Some(enabled) = request.enabled {
        monitor.enabled = enabled;
    }
    if let Some(threshold) = request.threshold {
        if !threshold.is_finite() {
            return Err(bad_request("Enter a finite threshold"));
        }
        monitor.threshold = threshold;
    }
    if let Some(comparison) = request.comparison {
        monitor.comparison = comparison;
    }
    if let Some(severity) = request.severity {
        monitor.severity = severity;
    }
    cloud.store.save(&monitor).await?;
    Ok(Json(monitor_json(&monitor)))
}

async fn remove_monitor(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, monitor_id)): Path<(String, String)>,
) -> Result<StatusCode> {
    let project = cloud.project_writable(&project_id, &actor).await?;
    let monitor = cloud
        .store
        .get::<Monitor>(&monitor_id)
        .await?
        .filter(|monitor| monitor.project_id == project.id)
        .ok_or_else(|| not_found("Monitor not found"))?;
    cloud
        .store
        .delete_by_owner::<AlertEvent>(&monitor.id)
        .await?;
    cloud.store.delete::<Monitor>(&monitor.id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_alerts(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
) -> Result<Json<Value>> {
    let (project, _) = cloud.project_role(&project_id, &actor).await?;
    let alerts = cloud
        .store
        .recent_by_parent::<AlertEvent>(&project.id, 100)
        .await?;
    Ok(Json(json!({
        "alerts": alerts.iter().map(alert_json).collect::<Vec<_>>(),
    })))
}

async fn examples(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path(project_id): Path<String>,
) -> Result<Json<Value>> {
    // Membership is still required: the example list names measurements that
    // exist in the project's data.
    cloud.project_role(&project_id, &actor).await?;
    Ok(Json(json!({
        "examples": seed::example_queries()
            .into_iter()
            .map(|(title, query)| json!({"title": title, "query": query}))
            .collect::<Vec<_>>(),
    })))
}
