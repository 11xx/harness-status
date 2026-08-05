# harness-status

`harness-status` turns AI harness lifecycle hooks into a small, stable JSON
contract for status bars. The process started with `--follow` owns the live
registry and keeps a tiny volatile restart checkpoint; there is no database,
history store, wrapper, or persistent daemon.

The adapters support Codex, Claude Code, and pi behind the same CLI and JSON
shape. The registry owner can also send generic desktop notifications for
completed turns and requests for attention.

## How it works

```text
Codex, Claude Code, or pi lifecycle event
        |
        | starts `harness-status hook codex|claude|pi`
        v
short-lived hook command
        |
        | non-blocking Unix datagram
        v
harness-status [codex|claude|pi] --follow
        |
        | owns the registry, or subscribes to its existing owner
        | JSON object per line for each follower
        v
Eww deflisten
```

The hook command reads the JSON object the harness provides on standard input,
keeps only status metadata, attempts one non-blocking send, and exits. It
deliberately returns success when no follower is running so status reporting
cannot delay or break the harness.

The socket is an ephemeral IPC endpoint at
`$XDG_RUNTIME_DIR/harness-status.sock`, restricted to the current user. Set
`HARNESS_STATUS_SOCKET` to override it. The owner atomically mirrors status
metadata to a mode-0600 `.state.json` file beside the socket. A replacement
owner restores live entries before printing its first snapshot and removes any
whose recorded process identity is no longer alive. The runtime directory and
checkpoint disappear at logout or reboot. Owner election and follower startup
also remove client socket paths whose encoded PID no longer exists, so status
bar reloads cannot accumulate abandoned Unix sockets.

## Build and install

Rust 1.97 or newer is recommended.

```sh
cargo build --release
cargo install --path . --root "$HOME/.local"
harness-status install codex
harness-status install claude
harness-status install pi
```

## Usage

```sh
# Query the currently running follower.
harness-status
harness-status codex
harness-status claude
harness-status pi

# Own the registry, or subscribe if another follower already owns it.
harness-status --follow
harness-status codex --follow
harness-status claude --follow
harness-status pi --follow

# Commands used by integrations and status-bar actions.
harness-status hook codex
harness-status hook claude
harness-status hook pi
harness-status ack 019abc-thread-id

# Add or remove lifecycle callbacks from the harness's user configuration.
harness-status install codex
harness-status uninstall codex
harness-status install claude
harness-status uninstall claude
harness-status install pi
harness-status uninstall pi
```

With no harness name, snapshot and follow modes include every supported
harness. Adding a harness name narrows the same JSON shape instead of creating a
provider-specific contract.

If no follower is running, snapshot mode prints `{"threads":[]}`. Only one
follower owns the registry, but any number of later `--follow` processes can
subscribe to its current snapshot and future changes. Each subscriber keeps
its own optional harness filter. If the owner exits, subscribers return to
owner election; one restores the runtime checkpoint and the others resubscribe.

## JSON contract

```json
{
  "threads": [
    {
      "id": "019abc...",
      "harness": "codex",
      "state": "working",
      "cwd": "/home/alice/projects/example",
      "name": "example",
      "model": "gpt-5.6-sol",
      "turn_id": "019def...",
      "updated_at": 1784161777
    }
  ]
}
```

The `state` field is `working`, `completed`, or `attention`:

- Codex and Claude Code `UserPromptSubmit` become `working`, while `Stop`
  becomes `completed`. A session that has only started, without a submitted
  prompt, has no entry; non-interactive runs submit a prompt like any other.
- Claude Code's `StopFailure` becomes `attention`. `Notification` also becomes
  `attention` for `permission_prompt`, `elicitation_dialog`, and
  `agent_needs_input`, and returns to `working` for `elicitation_complete` and
  `elicitation_response`. The remaining types, including `idle_prompt`, report
  that a settled session is waiting rather than that it needs interaction, and
  are ignored. `SessionEnd` removes the entry.
- pi `agent_start` becomes `working`, `agent_settled` becomes `completed` only
  after retries, automatic compaction, and queued continuations have finished,
  and the extension event `harness-status:attention` becomes `attention`.
  Cooperating extensions emit `harness-status:resume` with only a validated
  `working` or `completed` state after their interaction closes. They emit
  `harness-status:background` with a boolean while supervised work is active;
  Pi then ignores its intentional idle settlement until the child result wakes
  the parent. `session_shutdown` removes the entry.

A completed or attention entry remains visible until acknowledged, reused by
another prompt, or its owning harness process exits. Claude Code and Pi also
remove it when the session ends cleanly.

The hook does not retain prompts, transcript contents, permission details, tool
arguments, Claude's last response, or stop-failure details. Desktop notification
arguments contain only the harness display name, project display name, and
`finished` or `needs attention`; they never contain working directories, model
or session identifiers, prompts, responses, tools, or permission details.

Only the registry owner sends a desktop notification, and only when explicit
interaction is required or a top-level harness fully settles. Delegated child
completion is recorded but does not notify; its parent remains working until it
processes every child result and later settles. Resolving a reported interaction
restores its prior semantic state without sending a second notification. The
owner invokes `notify-send` directly without a shell and assigns a distinct
application identity to Codex, Claude Code, and pi, preventing a new event from
surfacing another harness's older grouped notifications. Set
`HARNESS_STATUS_NOTIFY=0`, `false`, or `off` to disable notifications,
or set `HARNESS_STATUS_NOTIFY_COMMAND` to override the executable. A missing,
failing, or slow notifier does not affect status publication; notifier processes
are reaped asynchronously and killed after one second. Delivery is best-effort:
an owner process that exits between checkpointing a transition and spawning its
notifier can lose that notification. When it can identify the owning harness
process through `/proc`, the follower removes the entry after
that process exits.

To recognize an Esc interruption that does not emit `Stop`, the Codex adapter
tails only new records from the hook-provided transcript path and reads the
event type and turn ID. An unknown transcript format fails safely and leaves the
click-to-ack fallback. Claude Code also omits `Stop` on user interruption, but
its transcript does not expose Codex's turn-abort record. A Claude working entry
therefore remains until its next lifecycle event, process exit, or manual
acknowledgement. Claude's hook input does not include the active model or a turn
ID, so those optional JSON fields are absent for Claude entries. Pi reports its
active model but has no separate turn ID in this contract. Pi's
`agent_settled` event also handles an Esc interruption without transcript
inspection.

Codex generates local memories in background sessions rooted beneath
`$CODEX_HOME/memories` (normally `~/.codex/memories`). Those internal sessions
are excluded at the hook boundary: they do not enter the user-facing registry,
appear in Eww, or emit task-completion notifications.

## Codex hooks

Install the binary first, then let it merge the lifecycle callbacks into the
Codex user configuration:

```sh
harness-status install codex
```

The command honors `$CODEX_HOME`, defaults to `~/.codex/hooks.json`, preserves
unrelated definitions, and records the absolute path to the current executable.
It is safe to repeat. To remove only the `harness-status` callbacks, run:

```sh
harness-status uninstall codex
```

The example [`examples/codex-hooks.json`](examples/codex-hooks.json) remains
available for manual configuration. Codex combines matching hooks from its
active configuration layers.

Open `/hooks` in Codex once after installing or changing the definition and
trust both commands. The callbacks are short-lived because Codex command hooks
run synchronously.

T3 Code launches the system Codex app-server with the inherited environment,
so the same user-level hooks and runtime socket work without wrapping either
program.

## Claude Code hooks

Install the lifecycle callbacks into Claude Code's user settings:

```sh
harness-status install claude
```

The command honors `$CLAUDE_CONFIG_DIR`, defaults to
`~/.claude/settings.json`, preserves unrelated settings and hook definitions,
and records the absolute path to the current executable. It is idempotent,
drops its own callbacks for events it no longer uses, and removes only its own
callbacks with:

```sh
harness-status uninstall claude
```

Claude Code watches settings for changes, so new sessions do not require a
wrapper or daemon. Open `/hooks` to verify the active user callbacks. The
example [`examples/claude-settings.json`](examples/claude-settings.json) is
available for manual configuration. See Claude Code's official
[hooks reference](https://code.claude.com/docs/en/hooks) for the lifecycle and
input contract.

## pi extension

Install the global lifecycle extension with:

```sh
harness-status install pi
```

The command honors `$PI_CODING_AGENT_DIR`, defaults to
`~/.pi/agent/extensions/harness-status.ts`, and embeds the absolute path to the
current executable. It refuses to overwrite or remove an unmanaged file at
that path. Installation and removal are idempotent:

```sh
harness-status uninstall pi
```

Run `/reload` in pi sessions that were already open. New sessions discover the
extension automatically. [`pi/harness-status.ts`](pi/harness-status.ts) is also
available for a manual PATH-based installation. The extension sends only the
session ID, working directory, active model ID, lifecycle event, and pi process
ID; it does not send conversation or tool content.

## Eww module

Eww has no separate module registry or package format. Reusable widgets are
ordinary Yuck files loaded with `include`; styles are included by the
consumer's main CSS or SCSS file.

Copy [`eww/harness-status.yuck`](eww/harness-status.yuck) and optionally
[`eww/harness-status.scss`](eww/harness-status.scss) into the Eww configuration,
for example under `modules/harness-status/`. Then add this to `eww.yuck`:

```yuck
(include "./modules/harness-status/harness-status.yuck")
```

Add the optional structural styles to `eww.scss`:

```scss
@import "./modules/harness-status/harness-status.scss";
```

Place the exported widget wherever indicators should appear:

```yuck
(harness-status-codex)
(harness-status-claude)
(harness-status-pi)
```

The module defines no window or bar and assumes no window manager, monitor,
paths, fonts, or consumer palette variables. Consumers can theme the semantic classes
`.harness-status__indicator--working`,
`.harness-status__indicator--completed`,
`.harness-status__indicator--attention`,
`.harness-status__indicator--codex`,
`.harness-status__indicator--claude`, and
`.harness-status__indicator--pi`. Working Codex turns use their ten-frame
Braille sequence, pi loops `π ϖ π ℼ` at 480 ms per frame, and Claude uses its
Linux pulse sequence (`· ✢ * ✶ ✻ ✽` and back) at 300 ms per frame. All
animation stays in Eww
rather than the JSON stream. Completed Codex turns use a small dot, completed
Claude turns use Claude Code's static `✻`, and completed pi turns use `π`.
Attention uses a static `!` for every harness. Clicking any state acknowledges
the indicator.

All adapters share the same semantic working/completed/attention classes, so pi
stays consistent with the other Eww indicators. Glyph choices live entirely in
the Eww module; the JSON contract exposes state data rather than presentation.

## Verification

The project keeps focused end-to-end smoke tests rather than a broad unit-test
suite:

```sh
cargo test
```
