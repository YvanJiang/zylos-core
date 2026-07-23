#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────────
# Zylos Docker Entrypoint
#
# 1. Validates required auth environment variables
# 2. Runs zylos init (creates/updates .env and workspace — every startup)
# 3. Passes through channel env vars to .env
# 4. Starts the Core executor service under PM2
# 5. Keeps container alive via PM2 --no-daemon
# ────────────────────────────────────────────────────────────────────────────
set -euo pipefail

if [ "$(id -u)" = "0" ]; then
  mkdir -p /home/zylos/.codex/skills /home/zylos/.claude /home/zylos/zylos
  if [ -d /opt/zylos/preinstalled-skills ]; then
    cp -a /opt/zylos/preinstalled-skills/. /home/zylos/.codex/skills/
  fi
  chown -R zylos:zylos /home/zylos/.codex /home/zylos/.claude /home/zylos/zylos
  export HOME=/home/zylos
  exec /usr/sbin/runuser --preserve-environment -u zylos -- "$0" "$@"
fi

ZYLOS_DIR="${HOME}/zylos"
ENV_FILE="${ZYLOS_DIR}/.env"
export ZYLOS_PACKAGE_ROOT="${ZYLOS_PACKAGE_ROOT:-${HOME}/.npm-global/lib/node_modules/zylos}"
export ZYLOS_EXECUTOR_IGNORE_ACTIVE_RELEASE="${ZYLOS_EXECUTOR_IGNORE_ACTIVE_RELEASE:-1}"

# ── Colour helpers ────────────────────────────────────────────────────────────
info()  { echo -e "\033[0;36m[zylos]\033[0m $*"; }
ok()    { echo -e "\033[0;32m[zylos]\033[0m ✓ $*"; }
warn()  { echo -e "\033[1;33m[zylos]\033[0m $*"; }
error() { echo -e "\033[0;31m[zylos]\033[0m $*" >&2; }
step()  { echo -e "\033[0;36m[zylos]\033[0m ── Step $1/$TOTAL_STEPS: $2"; }

TOTAL_STEPS=3
ZYLOS_VERSION="$(zylos --version 2>/dev/null || echo 'dev')"

echo ""
info "=========================================="
info "  Zylos ${ZYLOS_VERSION}"
info "=========================================="
echo ""

# ── Step 1: Validate auth ─────────────────────────────────────────────────────
step 1 "Checking authentication..."
# Accept Anthropic credentials (Claude runtime) OR OpenAI/Codex credentials (Codex runtime).
if [ -z "${ANTHROPIC_API_KEY:-}" ] && [ -z "${CLAUDE_CODE_OAUTH_TOKEN:-}" ] && \
   [ -z "${OPENAI_API_KEY:-}" ] && [ -z "${CODEX_API_KEY:-}" ]; then
  # Check mounted .env as fallback
  if ! grep -qE '^(ANTHROPIC_API_KEY|CLAUDE_CODE_OAUTH_TOKEN|OPENAI_API_KEY|CODEX_API_KEY)=' "${ENV_FILE}" 2>/dev/null; then
    error "No auth configured. Set ANTHROPIC_API_KEY / CLAUDE_CODE_OAUTH_TOKEN (Claude) or OPENAI_API_KEY / CODEX_API_KEY (Codex)."
    exit 1
  fi
fi
ok "Authentication configured"

if [ -n "${FEISHU_APP_ID:-}" ] && [ -n "${FEISHU_APP_SECRET:-}" ]; then
  if ! printf '%s\n' "${FEISHU_APP_SECRET}" | lark-cli config init \
    --app-id "${FEISHU_APP_ID}" \
    --app-secret-stdin \
    --brand feishu \
    --name feishu \
    --lang "${FEISHU_LANG:-zh}" >/dev/null 2>&1; then
    error "lark-cli credential initialization failed."
    exit 1
  fi
  ok "lark-cli Feishu profile configured"
fi

if [ -z "${OPENAI_API_KEY:-}" ] && [ -n "${CODEX_API_KEY:-}" ]; then
  export OPENAI_API_KEY="${CODEX_API_KEY}"
fi
if [ -z "${OPENAI_BASE_URL:-}" ] && [ -n "${CODEX_PROVIDER_BASE_URL:-}" ]; then
  export OPENAI_BASE_URL="${CODEX_PROVIDER_BASE_URL}"
fi
if [ -n "${OPENAI_API_KEY:-}" ]; then
  node -e "const fs=require('node:fs'); const os=require('node:os'); const path=require('node:path'); const key=process.env.OPENAI_API_KEY; if (!key) process.exit(0); const dir=path.join(os.homedir(), '.codex'); const file=path.join(dir, 'auth.json'); fs.mkdirSync(dir, {recursive:true}); let auth={}; try { auth=JSON.parse(fs.readFileSync(file, 'utf8')); } catch {} auth.auth_mode='apikey'; auth.OPENAI_API_KEY=key; fs.writeFileSync(file, JSON.stringify(auth, null, 2) + '\n', {mode:0o600});"
  ok "Codex auth store prepared"
fi

# ── Step 2: Workspace initialisation via zylos init ───────────────────────────
# zylos init handles: directory structure, .env creation from template,
# auth credential storage, timezone config, and the executor PM2 ecosystem.
# It uses upsert semantics — safe to re-run (won't overwrite existing values).
# Runs every startup (no marker) so template files stay in sync after image upgrades.
step 2 "Initializing workspace..."

# Resolve auth token — auto-detect type regardless of which env var it's in
AUTH_TOKEN="${CLAUDE_CODE_OAUTH_TOKEN:-${ANTHROPIC_API_KEY:-}}"
AUTH_FLAG=""
if [ -n "${AUTH_TOKEN}" ]; then
  if [[ "${AUTH_TOKEN}" == sk-ant-oat* ]]; then
    AUTH_FLAG="--setup-token ${AUTH_TOKEN}"
  else
    AUTH_FLAG="--api-key ${AUTH_TOKEN}"
  fi
fi

# Detect runtime — if only Codex credentials are present (no Claude creds), default to codex.
# ZYLOS_RUNTIME env var always wins when explicitly set.
RUNTIME_FLAG=""
if [ -z "${ZYLOS_RUNTIME:-}" ]; then
  HAS_CLAUDE_AUTH=false
  HAS_CODEX_AUTH=false
  [ -n "${ANTHROPIC_API_KEY:-}" ] || [ -n "${CLAUDE_CODE_OAUTH_TOKEN:-}" ] && HAS_CLAUDE_AUTH=true
  [ -n "${OPENAI_API_KEY:-}" ] || [ -n "${CODEX_API_KEY:-}" ] && HAS_CODEX_AUTH=true
  if [ "${HAS_CODEX_AUTH}" = true ] && [ "${HAS_CLAUDE_AUTH}" = false ]; then
    RUNTIME_FLAG="--runtime codex"
  fi
fi

# Build init flags
INIT_ARGS="--yes --quiet"
[ -n "${TZ:-}" ] && INIT_ARGS="${INIT_ARGS} --timezone ${TZ}"
[ -n "${AUTH_FLAG}" ] && INIT_ARGS="${INIT_ARGS} ${AUTH_FLAG}"
[ -n "${RUNTIME_FLAG}" ] && INIT_ARGS="${INIT_ARGS} ${RUNTIME_FLAG}"

# shellcheck disable=SC2086
if ! ZYLOS_INIT_SKIP_SERVICE_START=1 zylos init ${INIT_ARGS}; then
  warn "zylos init exited with errors (may be partial). Check logs."
fi

ok "Workspace ready"

# ── Pass through channel env vars to .env ─────────────────────────────────────
# zylos init doesn't write channel tokens — those come from component installs.
# In Docker, we pass them via environment and append to .env here (upsert).
upsert_env() {
  local key="$1" value="$2"
  [ -z "${value}" ] && return
  if grep -q "^${key}=" "${ENV_FILE}" 2>/dev/null; then
    # Update existing value — env vars from docker-compose take precedence
    sed -i "s|^${key}=.*|${key}=${value}|" "${ENV_FILE}" 2>/dev/null || true
  else
    echo "${key}=${value}" >> "${ENV_FILE}" 2>/dev/null || true
  fi
}

upsert_env "TELEGRAM_BOT_TOKEN" "${TELEGRAM_BOT_TOKEN:-}"
upsert_env "LARK_APP_ID" "${LARK_APP_ID:-}"
upsert_env "LARK_APP_SECRET" "${LARK_APP_SECRET:-}"
# Codex API credentials — persist to .env so the executor service can find them
# on subsequent restarts without relying on Docker's environment re-injection.
upsert_env "OPENAI_API_KEY" "${OPENAI_API_KEY:-}"
upsert_env "CODEX_API_KEY" "${CODEX_API_KEY:-}"
upsert_env "OPENAI_BASE_URL" "${OPENAI_BASE_URL:-}"
upsert_env "CODEX_PROVIDER_BASE_URL" "${CODEX_PROVIDER_BASE_URL:-}"
upsert_env "CODEX_NETWORK_ACCESS" "${CODEX_NETWORK_ACCESS:-}"
upsert_env "CODEX_APPROVAL_POLICY" "${CODEX_APPROVAL_POLICY:-}"
upsert_env "CODEX_SANDBOX_MODE" "${CODEX_SANDBOX_MODE:-}"
upsert_env "CODEX_PROVIDER_TURN_TIMEOUT_MS" "${CODEX_PROVIDER_TURN_TIMEOUT_MS:-}"
upsert_env "ZYLOS_PROVIDER_TURN_TIMEOUT_MS" "${ZYLOS_PROVIDER_TURN_TIMEOUT_MS:-}"

# Save current PATH so PM2 services can find claude and node
upsert_env "SYSTEM_PATH" "${PATH}"

# ── Step 3: Start PM2 services ────────────────────────────────────────────────
step 3 "Starting executor service..."

RELEASE_SCRIPT="${ZYLOS_PACKAGE_ROOT}/docker/publish-active-release.js"
if [ ! -f "${RELEASE_SCRIPT}" ]; then
  error "Docker release publisher script is missing: ${RELEASE_SCRIPT}"
  exit 1
fi
node "${RELEASE_SCRIPT}" "${ZYLOS_DIR}" "${ZYLOS_PACKAGE_ROOT}"

PM2_TEMPLATE="${ZYLOS_PACKAGE_ROOT}/templates/pm2/ecosystem.config.cjs"
if [ ! -f "${PM2_TEMPLATE}" ]; then
  error "PM2 ecosystem template is missing: ${PM2_TEMPLATE}"
  exit 1
fi
mkdir -p "${ZYLOS_DIR}/pm2"
cp "${PM2_TEMPLATE}" "${ZYLOS_DIR}/pm2/ecosystem.config.cjs"

FENCE_SCRIPT="${ZYLOS_PACKAGE_ROOT}/docker/prepare-executor-start.js"
if [ ! -f "${FENCE_SCRIPT}" ]; then
  error "Executor start preparation script is missing: ${FENCE_SCRIPT}"
  exit 1
fi
node "${FENCE_SCRIPT}" "${ZYLOS_DIR}"

# ── All done ──────────────────────────────────────────────────────────────────
echo ""
info "=========================================="
ok "Zylos is ready!"
info "=========================================="
echo ""
info "Use 'docker exec <container> zylos status' to check Core health."
exec pm2-runtime start "${ZYLOS_DIR}/pm2/ecosystem.config.cjs" --only zylos-executor
