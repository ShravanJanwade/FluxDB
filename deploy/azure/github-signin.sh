#!/usr/bin/env bash
# ============================================================
# Turn on "Sign in with GitHub" for the deployed FluxDB
# ============================================================
# GitHub only lets you create an OAuth app in the browser, so this
# script prints exactly what to enter there. It then stores the
# client ID and secret as repository secrets (typed into a hidden
# prompt and passed straight to GitHub, never written to disk) and
# redeploys. Run it in Azure Cloud Shell after setup.sh and
# `gh auth login`:
#
#   bash FluxDB/deploy/azure/github-signin.sh
# ============================================================
set -euo pipefail

APP_NAME="${APP_NAME:-fluxdb}"
APP_TITLE="FluxDB"
CALLBACK_PATH="/api/cloud/auth/github/callback"
RESOURCE_GROUP="${RESOURCE_GROUP:-rg-portfolio}"
ENVIRONMENT="${ENVIRONMENT:-cae-portfolio}"
WORKFLOW_FILE="deploy-azure.yml"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Git Bash on Windows: the Windows Azure CLI ends lines with CRLF.
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) az() { command az "$@" | tr -d '\r'; } ;;
esac

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
info() { printf '    %s\n' "$*"; }
fail() { printf '\033[31merror: %s\033[0m\n' "$*" >&2; exit 1; }

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
GITHUB_BRANCH="${GITHUB_BRANCH:-$(git -C "$repo_root" symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null | sed 's#^origin/##' || true)}"
GITHUB_BRANCH="${GITHUB_BRANCH:-master}"

if ! command -v gh >/dev/null 2>&1 || ! gh auth status >/dev/null 2>&1; then
  fail "GitHub CLI is not signed in. Run 'gh auth login' first."
fi
az account show -o none 2>/dev/null ||
  fail "Not signed in to Azure. Run 'az login' first (Cloud Shell is already signed in)."

# The address GitHub sends people back to: the deployed app's, or the one it
# will have once deployed.
fqdn="$(az containerapp show --name "$APP_NAME" --resource-group "$RESOURCE_GROUP" \
  --query properties.configuration.ingress.fqdn -o tsv 2>/dev/null || true)"
if [ -z "$fqdn" ]; then
  domain="$(az containerapp env show --name "$ENVIRONMENT" --resource-group "$RESOURCE_GROUP" \
    --query properties.defaultDomain -o tsv 2>/dev/null || true)"
  [ -n "$domain" ] || fail "Could not find $APP_NAME or $ENVIRONMENT in $RESOURCE_GROUP. Run setup.sh first."
  fqdn="$APP_NAME.$domain"
fi
url="https://$fqdn"

step "1. Register an OAuth app on GitHub"
info "Open https://github.com/settings/applications/new and enter:"
echo
printf '      %-28s %s\n' "Application name" "$APP_TITLE" "Homepage URL" "$url" \
  "Authorization callback URL" "$url$CALLBACK_PATH"
echo
info "Already have a $APP_TITLE OAuth app from Render? Edit it at"
info "https://github.com/settings/developers instead and replace both URLs. An OAuth app"
info "has a single callback URL, so the Render site's GitHub sign-in stops working."
info "Click 'Register application' (or 'Update application'), copy the Client ID, then"
info "click 'Generate a new client secret' and copy the secret."

step "2. Store them as repository secrets on $GITHUB_REPO"
read -r -p "    Client ID: " client_id
read -r -s -p "    Client secret (stays hidden while you paste): " client_secret
echo
client_id="$(printf '%s' "$client_id" | tr -d '[:space:]')"
client_secret="$(printf '%s' "$client_secret" | tr -d '[:space:]')"
[ -n "$client_id" ] && [ -n "$client_secret" ] || fail "Both the client ID and the client secret are needed."
printf '%s' "$client_id" | gh secret set GH_OAUTH_CLIENT_ID --repo "$GITHUB_REPO"
printf '%s' "$client_secret" | gh secret set GH_OAUTH_CLIENT_SECRET --repo "$GITHUB_REPO"
unset client_secret

step "3. Redeploy with GitHub sign-in turned on"
gh workflow run "$WORKFLOW_FILE" --repo "$GITHUB_REPO" --ref "$GITHUB_BRANCH"
sleep 8
run_id="$(gh run list --repo "$GITHUB_REPO" --workflow "$WORKFLOW_FILE" --limit 1 \
  --json databaseId --jq '.[0].databaseId' 2>/dev/null || true)"
if [ -n "$run_id" ]; then
  info "watching https://github.com/$GITHUB_REPO/actions/runs/$run_id"
  info "(Ctrl+C stops watching; the deploy carries on)"
  gh run watch "$run_id" --repo "$GITHUB_REPO" --exit-status ||
    fail "The deploy did not succeed; open the run above for the failing step."
fi

step "Done"
info "Open $url and choose Sign in with GitHub."
