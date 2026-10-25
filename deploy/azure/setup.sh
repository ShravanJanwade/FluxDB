#!/usr/bin/env bash
# ============================================================
# One-time Azure setup for FluxDB
# ============================================================
# Run it in Azure Cloud Shell (Bash), which is already signed in:
#
#   git clone https://github.com/ShravanJanwade/FluxDB.git
#   bash FluxDB/deploy/azure/setup.sh
#
# It creates, or reuses when they already exist:
#
#   - resource group rg-portfolio
#   - Container Apps environment cae-portfolio, without Log
#     Analytics: logs are streamed on demand rather than stored
#     and billed per GB
#   - a storage account with the fluxdb-data file share, where
#     the data directory survives the app scaling to zero
#   - the managed identity GitHub Actions signs in as (OIDC, so
#     no password or secret is ever stored in GitHub)
#   - a monthly budget that emails you before credit is wasted
#
# Everything it creates is tagged Owner, Cost Center and IaC
# managed, which Northeastern's Azure tenant requires.
#
# and then sets the repository variables .github/workflows/
# deploy-azure.yml reads. CodeLens ships the same script: both
# projects share everything above except their app and file
# share, so running the second one only adds what it needs.
# Re-running either is safe.
#
# Every setting below can be overridden, for example:
#   LOCATION=eastus2 bash FluxDB/deploy/azure/setup.sh
# ============================================================
set -euo pipefail

APP_NAME="${APP_NAME:-fluxdb}"
SHARE_NAME="${SHARE_NAME:-fluxdb-data}"
WORKFLOW_FILE="deploy-azure.yml"

RESOURCE_GROUP="${RESOURCE_GROUP:-rg-portfolio}"
ENVIRONMENT="${ENVIRONMENT:-cae-portfolio}"
IDENTITY="${IDENTITY:-id-portfolio-github}"
LOCATION="${LOCATION:-}"
BUDGET_NAME="${BUDGET_NAME:-portfolio-monthly}"
BUDGET_AMOUNT="${BUDGET_AMOUNT:-8}"
BUDGET_EMAIL="${BUDGET_EMAIL:-}"
# Tags on everything this script creates. Northeastern's Azure tenant refuses a
# resource group without Owner, Cost Center and IaC managed; its rule only checks
# that Cost Center exists, so it defaults to naming what pays: the student credit.
# Owner defaults to your Azure sign-in, IaC managed to this GitHub repository.
TAG_OWNER="${TAG_OWNER:-}"
TAG_COST_CENTER="${TAG_COST_CENTER:-Azure for Students}"
TAG_IAC_MANAGED="${TAG_IAC_MANAGED:-}"
# Any further tags a policy demands, as "Name=value" pairs separated by ";".
EXTRA_TAGS="${EXTRA_TAGS:-}"
# Closest to the US east coast first. Azure for Students only allows a few
# regions per subscription; the allowed ones are tried in this order.
PREFERRED_LOCATIONS="${PREFERRED_LOCATIONS:-eastus eastus2 centralus northcentralus southcentralus westus2 westus3 canadacentral}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
errors="$(mktemp)"
trap 'rm -f "$errors"' EXIT

# Git Bash on Windows: the Windows Azure CLI ends lines with CRLF.
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) az() { command az "$@" | tr -d '\r'; } ;;
esac

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
info() { printf '    %s\n' "$*"; }
warn() { printf '\033[33m    warning: %s\033[0m\n' "$*" >&2; }
fail() { printf '\033[31merror: %s\033[0m\n' "$*" >&2; exit 1; }

# The signed-in account's email address, or nothing for a guest account.
account_email() {
  local email
  email="$(az account show --query user.name -o tsv)"
  case "$email" in *'#EXT#'* | *' '*) email="" ;; *@*.*) ;; *) email="" ;; esac
  if [ -z "$email" ]; then
    email="$(az ad signed-in-user show --query mail -o tsv 2>/dev/null || true)"
    case "$email" in *@*.*) ;; *) email="" ;; esac
  fi
  printf '%s' "$email"
}

# ---- which GitHub repository -------------------------------------------------

GITHUB_REPO="${GITHUB_REPO:-}"
if [ -z "$GITHUB_REPO" ]; then
  remote="$(git -C "$repo_root" remote get-url origin 2>/dev/null || true)"
  GITHUB_REPO="$(printf '%s' "$remote" |
    sed -E 's#^https://([^/@]+@)?github\.com/##; s#^git@github\.com:##; s#\.git$##; s#/$##')"
fi
case "$GITHUB_REPO" in
  */*) ;;
  *) fail "Could not tell which GitHub repository this is. Re-run with GITHUB_REPO=owner/name." ;;
esac
# The OIDC subject GitHub signs is case-sensitive, so use the canonical name.
canonical="$(curl -fsS "https://api.github.com/repos/$GITHUB_REPO" 2>/dev/null |
  sed -n 's/^  "full_name": "\(.*\)",$/\1/p' | head -n 1 || true)"
GITHUB_REPO="${canonical:-$GITHUB_REPO}"
GITHUB_BRANCH="${GITHUB_BRANCH:-$(git -C "$repo_root" symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null | sed 's#^origin/##' || true)}"
GITHUB_BRANCH="${GITHUB_BRANCH:-master}"

# ---- Azure -------------------------------------------------------------------

step "Azure subscription"
az account show -o none 2>/dev/null ||
  fail "Not signed in to Azure. Run 'az login' first (Cloud Shell is already signed in)."
SUBSCRIPTION_ID="$(az account show --query id -o tsv)"
TENANT_ID="$(az account show --query tenantId -o tsv)"
info "$(az account show --query name -o tsv) ($SUBSCRIPTION_ID)"
info "Deploying $GITHUB_REPO ($GITHUB_BRANCH) as the '$APP_NAME' container app."
info "Wrong subscription? az account set --subscription <name-or-id>, then re-run."
TAG_OWNER="${TAG_OWNER:-$(account_email)}"
TAG_IAC_MANAGED="${TAG_IAC_MANAGED:-$GITHUB_REPO}"
[ -n "$TAG_OWNER" ] || fail "Could not find your email address for the Owner tag. Re-run with TAG_OWNER=you@example.edu."
tags=("Owner=$TAG_OWNER" "Cost Center=$TAG_COST_CENTER" "IaC managed=$TAG_IAC_MANAGED")
IFS=';' read -r -a extra_tags <<< "$EXTRA_TAGS"
for pair in "${extra_tags[@]}"; do
  case "$pair" in ?*=*) tags+=("$pair") ;; '') ;; *) fail "EXTRA_TAGS entries look like Name=value; got '$pair'." ;; esac
done
info "Tags: $(printf '%s, ' "${tags[@]}" | sed 's/, $//')"

step "Azure CLI extension"
az extension add --name containerapp --upgrade --only-show-errors -o none
info "containerapp extension is current"

step "Resource providers"
for namespace in Microsoft.App Microsoft.Storage Microsoft.ManagedIdentity Microsoft.OperationalInsights; do
  state="$(az provider show --namespace "$namespace" --query registrationState -o tsv 2>/dev/null || true)"
  if [ "$state" != "Registered" ]; then
    info "registering $namespace (this can take a couple of minutes)..."
    az provider register --namespace "$namespace" --wait -o none
  fi
  info "$namespace registered"
done

# Region names an "allowed locations" policy permits, if one is assigned.
allowed_locations() {
  command -v python3 >/dev/null 2>&1 || return 0
  az policy assignment list --disable-scope-strict-match -o json 2>/dev/null |
    python3 -c '
import json, re, sys
try:
    assignments = json.load(sys.stdin)
except ValueError:
    sys.exit(0)
found = set()
def walk(value):
    if isinstance(value, dict):
        for item in value.values():
            walk(item)
    elif isinstance(value, list):
        for item in value:
            walk(item)
    elif isinstance(value, str) and re.fullmatch(r"[a-z]+[a-z0-9]*", value.lower()):
        found.add(value.lower())
for assignment in assignments:
    name = (assignment.get("displayName") or "").lower()
    if "location" in name or "region" in name:
        walk(assignment.get("parameters") or {})
print("\n".join(sorted(found)))
' || true
}

# Azure's own words for the last failure, shortened for one line.
reason() { grep -v '^WARNING' "$errors" | grep -m 1 . | cut -c1-300; }

# Regions are tried in order, moving on only when a region list, capacity or
# availability check refuses one. Anything else, such as a policy demanding a
# tag, would fail the same way everywhere, so it stops with Azure's message.
refused() {
  grep -qiE 'RequestDisallowedByAzure|LocationNotAvailableForResourceType|is not available for resource|NoRegisteredProviderFound|not allowed to create environments in region|CapacityHeavyUsage|SkuNotAvailable' "$errors" && return 0
  grep -qi 'RequestDisallowedByPolicy' "$errors" && grep -qiE 'location|region' "$errors" && ! grep -qi 'tag' "$errors"
}

# Print Azure's error, with a hint when an organization policy is the cause.
explain() {
  cat "$errors" >&2
  if grep -qi 'RequestDisallowedByPolicy' "$errors" && grep -qi 'tag' "$errors"; then
    warn "an Azure Policy in your organization wants a tag this script did not set, or a different value."
    warn "set the value with the TAG_* settings at the top of this script, or add tags, for example:"
    warn "  TAG_COST_CENTER=12345 bash $0"
    warn "  EXTRA_TAGS='Department=Computer Science' bash $0"
  fi
}

# Provisioning state of the environment, or nothing when it does not exist.
env_state() {
  az containerapp env show --name "$ENVIRONMENT" --resource-group "$RESOURCE_GROUP" \
    --query properties.provisioningState -o tsv 2>/dev/null || true
}

# A failed creation can leave the environment behind in a Failed state, which
# blocks creating it again, here or in another region.
remove_broken_env() {
  local state
  state="$(env_state)"
  case "$state" in "" | Succeeded) return 0 ;; esac
  info "removing the environment Azure left behind ($state; a minute or two)..."
  az containerapp env delete --name "$ENVIRONMENT" --resource-group "$RESOURCE_GROUP" --yes -o none 2>/dev/null || true
}

step "Region, resource group and Container Apps environment"
# The CLI now creates "express" environments unless told otherwise, and those
# cannot mount an Azure Files share or run the cron scale rule FluxDB's warm-up
# uses. Ask for a standard (workload profiles) environment by name.
env_mode_args=()
if az containerapp env create --help 2>/dev/null | grep -q -- '--environment-mode'; then
  env_mode_args=(--environment-mode WorkloadProfiles)
fi
state="$(env_state)"
if [ "$state" = "Succeeded" ] &&
  [ "$(az containerapp env show --name "$ENVIRONMENT" --resource-group "$RESOURCE_GROUP" --query properties.environmentMode -o tsv 2>/dev/null)" = "Express" ]; then
  apps="$(az containerapp list --resource-group "$RESOURCE_GROUP" --environment "$ENVIRONMENT" --query 'length(@)' -o tsv 2>/dev/null || echo 0)"
  [ "$apps" = "0" ] ||
    fail "Environment $ENVIRONMENT is an express environment, which cannot mount the file share, and it already has apps. Delete them, or the environment, and re-run."
  info "environment $ENVIRONMENT is an express environment, which cannot mount the file share"
  info "recreating it as a standard environment (a minute or two to remove it)..."
  az containerapp env delete --name "$ENVIRONMENT" --resource-group "$RESOURCE_GROUP" --yes -o none
  state=""
fi
case "$state" in
  "" | Succeeded) ;;
  Failed | Canceled | UpgradeFailed)
    info "environment $ENVIRONMENT exists but is $state"
    remove_broken_env
    state=""
    ;;
  *) fail "Environment $ENVIRONMENT is still being set up or removed ($state). Wait a few minutes, then re-run." ;;
esac
if [ "$state" = "Succeeded" ]; then
  LOCATION="$(az containerapp env show --name "$ENVIRONMENT" --resource-group "$RESOURCE_GROUP" --query location -o tsv | tr -d ' ' | tr '[:upper:]' '[:lower:]')"
  info "reusing environment $ENVIRONMENT in $LOCATION"
else
  if [ -n "$LOCATION" ]; then
    candidates="$LOCATION"
  else
    regions="$(az account list-locations --query "[?metadata.regionType=='Physical'].name" -o tsv)"
    allowed="$(allowed_locations | grep -xF "$regions" || true)"
    if [ -n "$allowed" ]; then
      info "your subscription allows: $(printf '%s' "$allowed" | tr '\n' ' ')"
      candidates=""
      for region in $PREFERRED_LOCATIONS $allowed; do
        if printf '%s\n' "$allowed" | grep -qx "$region" && ! printf ' %s ' "$candidates" | grep -q " $region "; then
          candidates="$candidates $region"
        fi
      done
    else
      candidates="$PREFERRED_LOCATIONS"
    fi
  fi
  created=""
  for region in $candidates; do
    if ! az group show --name "$RESOURCE_GROUP" -o none 2>/dev/null; then
      if ! az group create --name "$RESOURCE_GROUP" --location "$region" --tags "${tags[@]}" -o none 2>"$errors"; then
        if refused; then info "$region refused: $(reason)"; continue; fi
        explain
        fail "Could not create resource group $RESOURCE_GROUP."
      fi
    fi
    info "creating environment $ENVIRONMENT in $region (about 3 minutes)..."
    if az containerapp env create --name "$ENVIRONMENT" --resource-group "$RESOURCE_GROUP" \
      --location "$region" "${env_mode_args[@]}" --logs-destination none --tags "${tags[@]}" \
      -o none 2>"$errors"; then
      created="$region"
      break
    fi
    # A policy refusal would repeat in every region, so it stops here. Other
    # failures, such as a platform error in one region, try the next one.
    if ! refused && grep -qi 'RequestDisallowedByPolicy' "$errors"; then
      explain
      fail "Could not create the Container Apps environment."
    fi
    info "$region did not work: $(reason)"
    remove_broken_env
  done
  [ -n "$created" ] || fail "No region worked ($candidates); Azure's reasons are above. If they name your region list, find it under Azure Portal > Policy > Assignments > 'Allowed resource deployment regions' and re-run with LOCATION=<region>; otherwise wait a while and re-run."
  LOCATION="$created"
  if [ "$(az containerapp env show --name "$ENVIRONMENT" --resource-group "$RESOURCE_GROUP" --query properties.environmentMode -o tsv 2>/dev/null)" = "Express" ]; then
    fail "Azure created an express environment, which cannot mount the file share. Update the CLI (az upgrade, then az extension add --name containerapp --upgrade) and re-run; the script replaces it."
  fi
  info "environment $ENVIRONMENT is ready in $LOCATION"
fi
DOMAIN="$(az containerapp env show --name "$ENVIRONMENT" --resource-group "$RESOURCE_GROUP" --query properties.defaultDomain -o tsv)"

step "Storage for data archives"
storage_account="$(az storage account list --resource-group "$RESOURCE_GROUP" --query "[?tags.portfolio=='shared'] | [0].name" -o tsv)"
if [ -z "$storage_account" ]; then
  # Globally unique, but stable for this subscription so re-runs find it.
  storage_account="stportfolio$(printf '%s' "$SUBSCRIPTION_ID" | sha256sum | cut -c1-8)"
  info "creating storage account $storage_account..."
  if ! az storage account create --name "$storage_account" --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" --sku Standard_LRS --kind StorageV2 --min-tls-version TLS1_2 \
    --https-only true --allow-blob-public-access false --tags portfolio=shared "${tags[@]}" \
    -o none 2>"$errors"; then
    explain
    fail "Could not create storage account $storage_account."
  fi
fi
info "storage account $storage_account"
if ! az storage share-rm show --resource-group "$RESOURCE_GROUP" --storage-account "$storage_account" \
  --name "$SHARE_NAME" -o none 2>/dev/null; then
  # Pay-as-you-go share: billed for the few MB actually stored, capped at 5 GiB.
  az storage share-rm create --resource-group "$RESOURCE_GROUP" --storage-account "$storage_account" \
    --name "$SHARE_NAME" --quota 5 --access-tier TransactionOptimized -o none
fi
info "file share $SHARE_NAME"
if ! az containerapp env storage show --name "$ENVIRONMENT" --resource-group "$RESOURCE_GROUP" \
  --storage-name "$SHARE_NAME" -o none 2>/dev/null; then
  storage_key="$(az storage account keys list --resource-group "$RESOURCE_GROUP" \
    --account-name "$storage_account" --query '[0].value' -o tsv)"
  az containerapp env storage set --name "$ENVIRONMENT" --resource-group "$RESOURCE_GROUP" \
    --storage-name "$SHARE_NAME" --azure-file-account-name "$storage_account" \
    --azure-file-account-key "$storage_key" --azure-file-share-name "$SHARE_NAME" \
    --access-mode ReadWrite -o none
fi
info "environment storage $SHARE_NAME is linked to $ENVIRONMENT"

step "Identity for GitHub Actions"
if ! az identity show --name "$IDENTITY" --resource-group "$RESOURCE_GROUP" -o none 2>/dev/null; then
  if ! az identity create --name "$IDENTITY" --resource-group "$RESOURCE_GROUP" --location "$LOCATION" \
    --tags "${tags[@]}" -o none 2>"$errors"; then
    explain
    fail "Could not create managed identity $IDENTITY."
  fi
fi
CLIENT_ID="$(az identity show --name "$IDENTITY" --resource-group "$RESOURCE_GROUP" --query clientId -o tsv)"
PRINCIPAL_ID="$(az identity show --name "$IDENTITY" --resource-group "$RESOURCE_GROUP" --query principalId -o tsv)"
GROUP_ID="$(az group show --name "$RESOURCE_GROUP" --query id -o tsv)"
info "managed identity $IDENTITY"
# Contributor on this one resource group only: it cannot touch anything else.
if [ "$(az role assignment list --assignee "$PRINCIPAL_ID" --scope "$GROUP_ID" --role Contributor --query 'length(@)' -o tsv 2>/dev/null || echo 0)" = "0" ]; then
  for attempt in 1 2 3 4 5 6 7 8; do
    if az role assignment create --assignee-object-id "$PRINCIPAL_ID" --assignee-principal-type ServicePrincipal \
      --role Contributor --scope "$GROUP_ID" -o none 2>"$errors" || grep -q RoleAssignmentExists "$errors"; then
      break
    fi
    # A new identity takes a little while to replicate.
    [ "$attempt" -lt 8 ] || { cat "$errors" >&2; fail "Could not grant the identity access to $RESOURCE_GROUP."; }
    sleep 15
  done
fi
info "Contributor on $RESOURCE_GROUP"
credential="github-$(printf '%s' "$GITHUB_REPO" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9\n' '-')-$GITHUB_BRANCH"
if ! az identity federated-credential show --name "$credential" --identity-name "$IDENTITY" \
  --resource-group "$RESOURCE_GROUP" -o none 2>/dev/null; then
  az identity federated-credential create --name "$credential" --identity-name "$IDENTITY" \
    --resource-group "$RESOURCE_GROUP" --issuer https://token.actions.githubusercontent.com \
    --subject "repo:$GITHUB_REPO:ref:refs/heads/$GITHUB_BRANCH" --audiences api://AzureADTokenExchange -o none
fi
info "trusts workflows on $GITHUB_REPO@$GITHUB_BRANCH"

step "Monthly budget alert"
budget_url="https://management.azure.com/subscriptions/$SUBSCRIPTION_ID/providers/Microsoft.Consumption/budgets/$BUDGET_NAME?api-version=2023-11-01"
if az rest --method get --url "$budget_url" -o none 2>/dev/null; then
  info "budget $BUDGET_NAME already exists"
else
  email="${BUDGET_EMAIL:-$(account_email)}"
  if [ -z "$email" ]; then
    warn "skipped: no email address found. Re-run with BUDGET_EMAIL=you@example.com to add one."
  else
    body="$(cat <<JSON
{
  "properties": {
    "category": "Cost",
    "amount": $BUDGET_AMOUNT,
    "timeGrain": "Monthly",
    "timePeriod": { "startDate": "$(date -u +%Y-%m-01)T00:00:00Z" },
    "notifications": {
      "actual50": { "enabled": true, "operator": "GreaterThan", "threshold": 50, "thresholdType": "Actual", "contactEmails": ["$email"] },
      "actual100": { "enabled": true, "operator": "GreaterThan", "threshold": 100, "thresholdType": "Actual", "contactEmails": ["$email"] },
      "forecast100": { "enabled": true, "operator": "GreaterThan", "threshold": 100, "thresholdType": "Forecasted", "contactEmails": ["$email"] }
    }
  }
}
JSON
)"
    if az rest --method put --url "$budget_url" --body "$body" -o none 2>"$errors"; then
      info "emails $email when monthly spend passes \$$((BUDGET_AMOUNT / 2)) or is forecast to pass \$$BUDGET_AMOUNT"
    else
      warn "could not create a budget (not every student subscription supports them): $(tail -n 1 "$errors")"
      warn "check credit at https://www.microsoftazuresponsorships.com/Balance instead"
    fi
  fi
fi

step "GitHub repository variables"
json_string() { printf '"%s"' "$(printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g')"; }
# The deploy workflow puts the same tags on the container app.
AZURE_TAGS=""
for pair in "${tags[@]}"; do
  AZURE_TAGS="$AZURE_TAGS,$(json_string "${pair%%=*}"):$(json_string "${pair#*=}")"
done
AZURE_TAGS="{${AZURE_TAGS#,}}"
variables=(
  "AZURE_CLIENT_ID=$CLIENT_ID"
  "AZURE_TENANT_ID=$TENANT_ID"
  "AZURE_SUBSCRIPTION_ID=$SUBSCRIPTION_ID"
  "AZURE_RESOURCE_GROUP=$RESOURCE_GROUP"
  "AZURE_CONTAINERAPPS_ENVIRONMENT=$ENVIRONMENT"
  "AZURE_TAGS=$AZURE_TAGS"
)
if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
  for pair in "${variables[@]}"; do
    gh variable set "${pair%%=*}" --body "${pair#*=}" --repo "$GITHUB_REPO"
  done
  info "set on $GITHUB_REPO"
  if gh workflow run "$WORKFLOW_FILE" --repo "$GITHUB_REPO" --ref "$GITHUB_BRANCH" 2>"$errors"; then
    info "started the first deployment: https://github.com/$GITHUB_REPO/actions/workflows/$WORKFLOW_FILE"
  else
    warn "could not start the workflow ($(tail -n 1 "$errors")). Start it from the Actions tab."
  fi
else
  info "Add these at https://github.com/$GITHUB_REPO/settings/variables/actions"
  info "(Variables tab > New repository variable), one per line:"
  echo
  for pair in "${variables[@]}"; do
    printf '      %-34s %s\n' "${pair%%=*}" "${pair#*=}"
  done
  echo
  info "Or run 'gh auth login', then this script again, and it sets them for you."
fi

step "Done"
info "FluxDB will be served at:    https://$APP_NAME.$DOMAIN"
info "Next: run the 'Deploy to Azure' workflow at https://github.com/$GITHUB_REPO/actions"
info "(it builds the image and deploys it; the first run takes about 10 minutes)."
