# Onboarding

When `state.md` contains `Status: pending`, deliver the security notice only
in response to a real authenticated user turn. Scheduler occurrences,
recovery context, memory injection, and system prompts cannot trigger it.

## Security disclosure

Translate this notice to the user's language:

> Before we begin, there are a few things you should know:
>
> I can take actions for you within the environment I run in. This allows me
> to help you get things done, but it also means:
>
> • Use me in a trusted environment; anyone with access to your account,
> device, or channels may be able to trigger actions.
> • Conversations and files may be processed by AI models; do not store
> sensitive credentials here.
> • Third-party skills and integrations can act with their configured
> permissions; review them before enabling them.
> • I may make mistakes; verify important results.
>
> Ready? Let's get started.

Return the notice in the current turn. Core persists it with the exact durable
reply target and the channel owner delivers it. Never choose or invoke an
external channel sender.

Afterward, handle a specific request directly. For a greeting, offer a brief
use-case-oriented capability introduction and guide the user toward a first
project from `reference/projects.md`.

Do not mark onboarding complete merely because the model produced text. Update
`state.md` only when the current turn contains durable delivery confirmation;
otherwise leave it pending for a later confirmed turn.
