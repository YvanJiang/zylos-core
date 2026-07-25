# Docker Deployment

Zylos runs one long-lived Core executor service in the container. Claude uses
the Agent SDK transport; Codex uses the official app-server transport. PM2 is
only the operating-system supervisor, while Core health and service identity
remain authoritative.

## Prerequisites

- Docker 24+
- Claude credentials (`CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY`), or
  Codex credentials (`OPENAI_API_KEY` or `CODEX_API_KEY`)

## Docker Run

```bash
docker run -d --name zylos \
  -e CLAUDE_CODE_OAUTH_TOKEN=YOUR_TOKEN_HERE \
  -v zylos-data:/home/zylos/zylos \
  -v claude-config:/home/zylos/.claude \
  ghcr.io/zylos-ai/zylos-core:latest
```

For Codex, provide `OPENAI_API_KEY` and set `ZYLOS_RUNTIME=codex`.

## Docker Compose

```bash
mkdir zylos && cd zylos
curl -fsSLO https://raw.githubusercontent.com/zylos-ai/zylos-core/main/docker-compose.yml
export CLAUDE_CODE_OAUTH_TOKEN=YOUR_TOKEN_HERE
docker compose up -d
```

The named `zylos-data` volume contains the durable Core SQLite state,
configuration, workspace, release pointer, snapshots, and logs. Back it up.
The optional `claude-config` volume preserves native Claude authentication.

## Lifecycle and health

On each container start, the entrypoint:

1. validates provider credentials;
2. runs `zylos init` with service startup suppressed so templates and provider
   prerequisites converge without creating a second executor;
3. persists channel/provider environment values in the managed `.env`;
4. replaces PID 1 with `pm2-runtime`, starting only `zylos-executor`.

Check authoritative Core health rather than inferring it from PM2:

```bash
docker exec zylos zylos status
docker exec zylos zylos doctor --check
docker logs -f zylos
```

The Compose healthcheck runs `zylos status`, so an operating PM2 daemon without
a valid, complete Core health response is not considered ready. A complete
service may be operationally ready while its durable health remains `degraded`;
`zylos status` reports both values and still rejects maintenance, draining,
reconciliation, incomplete, or offline states.

## Environment variables

| Variable | Purpose |
|---|---|
| `ZYLOS_RUNTIME` | `claude` (default) or `codex` |
| `CLAUDE_CODE_OAUTH_TOKEN` | Claude native authentication token |
| `ANTHROPIC_API_KEY` | Anthropic API authentication |
| `OPENAI_API_KEY` / `CODEX_API_KEY` | Codex authentication |
| `TZ` | IANA timezone, default `UTC` |
| `TELEGRAM_BOT_TOKEN` | Optional Telegram channel credential |
| `LARK_APP_ID` / `LARK_APP_SECRET` | Optional Lark/Feishu credentials |

Provider writes remain subject to Core permission handling and durable
workspace-lease fencing. The container configuration does not enable a bypass
mode.

## Updating

```bash
docker compose pull
docker compose up -d
```

For an installed release, `zylos upgrade --self` sends the request to the
executor service. Core owns snapshot, maintenance/drain, release activation,
health verification, commit/rollback, and post-commit cleanup. The stable CLI
and service launchers both read the same durable active-release pointer.

## Troubleshooting

```bash
# Core identity and health
docker exec zylos zylos status

# Diagnose without mutation
docker exec zylos zylos doctor --check

# Executor logs
docker exec zylos zylos logs executor
docker exec zylos zylos logs pm2
```
