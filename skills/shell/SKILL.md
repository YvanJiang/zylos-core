---
name: shell
description: CLI interactive mode channel backed by canonical Core ingress and durable outbox delivery.
user-invocable: false
---

# Shell Channel

Communication channel for `zylos shell` — the CLI interactive mode.

## How It Works

1. `zylos shell` starts a readline REPL and an owner-only Unix domain socket.
2. User input enters canonical Core ingress through `c4-receive` with the
   shell socket as the durable channel endpoint.
3. The shell's channel-scoped delivery owner claims only `shell` commands from
   the Core outbox, renders the provider-neutral text model, and writes it to
   that exact socket.
4. The owner records the fenced delivery result in Core before later commands
   in the same lane advance.
5. The model never selects a delivery route or invokes a channel send command.

## Socket Protocol

- Socket path is passed as the endpoint (e.g., `/tmp/zylos-shell-<pid>.sock`)
- The in-process channel owner writes the rendered message as UTF-8, then closes the connection
- The REPL reassembles the message from socket data events
