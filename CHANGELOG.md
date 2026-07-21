# Changelog

All notable changes to zylos-core are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed

- Runtime execution, ingress, delivery, scheduling, and observability now use
  durable Core contracts and provider-neutral state.
- Channel delivery owners consume exact durable targets from the Core outbox;
  Core does not select a renderer or infer a reply destination.
- Scheduler occurrences use authoritative Core queue and maintenance state,
  including durable conversation and native-thread delivery identity.
- Health consumers use provider-neutral service, executor, turn, queue, and
  delivery facts.

### Migration history

Detailed pre-migration release notes are retained in Git history only. They are
intentionally absent from ordinary product documentation because their former
operational procedures and runtime authority no longer apply.
