# Retired Custom Session-Start Directives

The historical `~/zylos/custom-hooks/session-start/` injection path is retired.
Core no longer reads provider session UI state or installs per-session runtime
hooks, so files in that directory are inert and must not be treated as active
policy or observability.

Durable cross-provider instructions belong in the managed Zylos instruction
assets. Conversation-specific context belongs in the canonical Core
conversation/turn lineage, and operator memory belongs in the explicit memory
files. Migration cleanup may preserve old directive files as user data, but no
normal package, service, hook, or runtime entrypoint imports or dispatches them.
