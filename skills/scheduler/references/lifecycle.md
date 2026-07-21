# Lifecycle Commands

The commands below manage future scheduling state. They cannot complete an
admitted occurrence; completion is reconciled from its durable Core turn.

## remove

`cli.js remove <task-id>` permanently removes a task and its local history.

## pause

`cli.js pause <task-id>` pauses a pending future occurrence.

## resume

`cli.js resume <task-id>` returns a paused occurrence to pending. A task fenced
by legacy-control migration refuses resume until an operator first runs
`update --bound-conversation-json '<complete identity>'` or explicitly selects
`update --use-synthetic-conversation`.

If migration could not prove an exact admitted Core occurrence (or found
ambiguous legacy started history), the task also carries a durable replay
barrier. Advance its schedule with an `update` timing option before `resume`;
changing only the conversation identity can never replay that occurrence.

All commands support an unambiguous partial task ID.
