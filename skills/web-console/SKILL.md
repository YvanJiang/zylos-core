---
name: web-console
description: Built-in web interface for communicating with Claude without external services. Use when setting up or configuring the web console channel, or troubleshooting browser-based access.

lifecycle:
  npm: true
  service:
    type: pm2
    name: web-console
    entry: scripts/server.js
---

# Web Console (C4 Built-in Channel)

Default communication channel - works without any external service.

## Purpose

Allows users to communicate with Claude even without Telegram/Lark/Discord.
This is the baseline, always-available interface.

## Quick Start

```bash
# Install dependencies
cd ~/zylos/.claude/skills/web-console
npm install

# Start server (default port 3456)
node scripts/server.js

# Or with PM2
pm2 start scripts/server.js --name web-console
```

## Access

Local only: `http://127.0.0.1:3456`

Server binds to `127.0.0.1` by default for security.

## Architecture

```
Browser ──► Core ingress ──► conversation executor ──► Core outbox
   ▲                                                    │
   └──────── Web Console channel owner ◄────────────────┘
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/status` | GET | Get Claude's current status |
| `/api/conversations/recent` | GET | Get recent conversation history |
| `/api/upload` | POST | Upload one attachment for the next message |
| `/api/send` | POST | Send message to Claude |
| `/api/media/:messageId` | GET | Retired legacy endpoint; always fails closed |
| `/api/poll?since_id=N&cursor_scope=S` | GET | Poll the durable mailbox; nonzero cursors require the opaque scope returned in `X-Zylos-Mailbox-Cursor-Scope` |
| `/api/health` | GET | Server health check |

## Files

```
~/zylos/.claude/skills/web-console/
├── SKILL.md
├── package.json
├── scripts/
│   ├── server.js      # Express API server
│   ├── core-outbox-owner.js # Channel-scoped renderer/delivery owner
│   └── send.js        # Retired fail-closed direct-send command
└── public/
    ├── index.html     # Chat UI
    ├── styles.css     # Styling
    ├── mailbox-cursor.js # Scope-bound durable cursor state/reset
    └── app.js         # Frontend logic
```

The mailbox cursor is the pair `(X-Zylos-Mailbox-Cursor-Scope, message id)`.
After a Core scope reconfiguration, HTTP returns `409 mailbox_cursor_scope_mismatch`
and WebSocket returns `cursor_reset`; clients must clear the prior view and reload
from cursor zero in the returned opaque scope.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `WEB_CONSOLE_PORT` | 3456 | Server port |
| `ZYLOS_WEB_PASSWORD` | (empty) | Set to enable password protection (also reads `WEB_CONSOLE_PASSWORD` as fallback) |
| `WEB_CONSOLE_BIND` | 127.0.0.1 | Bind address |
| `ZYLOS_DIR` | ~/zylos | Data directory |
| `ZYLOS_REGION` | global | Exact durable Core region owned by this console |
| `ZYLOS_TENANT_ID` | default | Exact durable Core tenant owned by this console |
| `ZYLOS_BOT_ID` | zylos | Exact durable Core bot owned by this console |
| `WEB_CONSOLE_MAX_UPLOAD_MB` | 20 | Max size per uploaded attachment |

## Authentication

By default, no password is required (suitable for local access).

To enable password protection (recommended when exposing externally):
1. Set `ZYLOS_WEB_PASSWORD` in `~/zylos/.env`
2. Restart the web-console service

## Features

- Provider-neutral Core service, executor, turn, queue, and outbox status
- Message polling every 2 seconds
- Auto-resizing input
- Browser file/image upload via attach button, drag/drop, and paste
- Durable provider-neutral text fallback rendered from Core outbox commands
- Mobile-friendly responsive design
- Dark theme

## Attachments

Browser uploads are stored under `~/zylos/web-console/media/` and delivered to the agent as text annotations:

```text
[attachment:image /Users/howard/zylos/web-console/media/wc-...png name="screenshot.png" 142KB]
[attachment:file /Users/howard/zylos/web-console/media/wc-...pdf name="report.pdf" 1.2MB]
```

Core persists replies in its durable outbox. The Web Console channel owner
claims only `web-console` commands, renders the text model, delivers to a
connected browser, and records the fenced result. The agent never invokes a
direct send script. Rich outbound media needs an explicit future Core contract;
the retired marker-based legacy endpoint fails closed.
