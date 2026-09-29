# harness-status

`harness-status` turns AI harness lifecycle hooks and Codex activity records
into a small, stable JSON contract for status bars. The process started with
`--follow` owns the live registry and keeps a tiny volatile restart checkpoint;
there is no database, history store, wrapper, or persistent daemon.

The adapters support Codex, Claude Code, and pi behind the same CLI and JSON
shape. The registry owner can also send generic desktop notifications for
completed turns and requests for attention.

## How it works

```text
Codex, Claude Code, or pi lifecycle event
or recent Codex native activity
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

If the socket cannot accept a hook, the command makes one best-effort write
to a private file beside the socket with extension `hook-failure`. The fixed
33-byte line contains only the Unix timestamp and OS error code of the latest
failure, including a missing listener or a full socket queue. Successful sends
leave that marker intact. `harness-status guide` displays its timestamp and
error code when readable; the snapshot JSON contract stays the same. Recording
does not lock, retry, sync to disk, or create directories. An unavailable,
oversized, non-private, symlink, or non-regular diagnostic file is skipped;
the hook still exits successfully. The marker is bounded diagnostic evidence,
not a delivery counter or acknowledgement.

The Codex owner also reconciles a recent rollout when its hook was not
delivered. A `task_started` record, a later `task_complete` or `turn_aborted`
record, and a live Codex process in the rollout's recorded directory provide
the working/completed lifecycle; hooks remain the source for exact transition
timing and metadata such as background work. A rollout never reopens a turn
that has already settled, since it can trail the hook that settled it. A hook
names its own owning process, while a recovered entry follows the newest Codex
process in its directory.

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

Rust 1.97 or newer is recommended. The checkout's `rust-toolchain.toml` pins
Rust 1.97.0, which rustup installs on first use.

```sh
make install
harness-status install codex
harness-status install claude
harness-status install pi
```

`make install` builds the release binary and installs both `harness-status` and
its Claude window-ping helper. Use `BINDIR` and `AI_BINDIR` to override the
installation directories. A direct binary-only installation remains available
with `cargo install --path . --root "$HOME/.local" --force`.

## Usage

```sh
# Print the orientation guide.
harness-status guide

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
harness-status usage
harness-status usage ingest claude
harness-status usage enable|disable|toggle
harness-status ping enable|disable|toggle
```

`harness-status guide` prints an orientation in the terminal: the four
states, what `--follow` streams, the `ack` and `usage` verbs, and which
harness adapters report non-interactive runs.

`harness-status usage` prints one normalized account-window report for Codex
and Claude. It asks Codex's supported app-server `account/rateLimits/read`
method and Claude's read-only OAuth usage endpoint for fresh values at most once
every three minutes. Claude's endpoint is not a public API; the command reads
the existing unexpired Claude access token, sends it to Anthropic through
`curl` standard input, and never refreshes, rewrites, logs, or places it in a
process argument. Neither provider refresh starts a model request.

Claude usage tracking is independently controlled with `harness-status usage disable`,
`enable`, or `toggle`. The disabled state is the marker
`$XDG_STATE_HOME/harness-status/claude-usage.disabled` (falling back to
`~/.local/state`); it stops Claude refresh, passive ingestion, and the Claude
usage widget while retaining the cache. Codex usage remains available.

The five-hour Claude window ping has its own control:
`harness-status ping disable`, `enable`, or `toggle`. The helper reads
`$XDG_STATE_HOME/harness-status/claude-window-ping.disabled` and exits without
calling Claude when that marker exists.

Normal client activity remains the fastest update path. Codex session events
seed the report when the app server is unavailable, and a session observation
newer than the cached app-server value replaces it; each observation is dated
by its own event. Claude's status line passes its already-delivered payload to
`harness-status usage ingest claude`.
Passive Claude payloads fill windows that are missing from the cache but do not
overwrite complete values obtained from the authoritative usage endpoint.
The cache and provider refresh deadlines live in a user-private runtime file.
An advisory lock coalesces concurrent readers across bars and reloads; it
guards only cache reads and writes, never a provider request, so a passive
payload waits for it briefly rather than being dropped. A
provider observation older than five minutes is `stale`; its last known values
remain present with their age. Missing data is `unavailable`, not zero. A
Claude `429` honors `Retry-After` and otherwise backs off from three to six,
twelve, and fifteen minutes.

Each available window includes its absolute `resets_at` timestamp plus dynamic
`resets_in_seconds` and compact `resets_in` countdowns. The compact bar remains
percentage-only; hovering a provider shows when each window resets.

With no harness name, snapshot and follow modes include every supported
harness. Adding a harness name narrows the same JSON shape instead of creating a
provider-specific contract.

If no follower is running, snapshot mode prints `{"threads":[]}`. Only one
follower owns the registry, but any number of later `--follow` processes can
subscribe to its current snapshot and future changes. Each subscriber keeps
its own optional harness filter. If the owner exits, subscribers return to
owner election; one restores the runtime checkpoint and the others resubscribe.
Election is contended by design: losing the race to retire a dead owner's
socket, or querying an owner that answers too slowly, makes a process a
subscriber rather than ending it, because a status bar cannot restart a
`deflisten` command that exits.

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
      "model": "gpt-5.6-sol#medium",
      "turn_id": "019def...",
      "reason": "permission",
      "background": false,
      "started_at": 1784161742,
      "updated_at": 1784161777,
      "duration": "35s"
    }
  ]
}
```

`model` includes the effort as `model#effort` when the harness supplies it.
When effort is unavailable, it contains the model slug alone. A later event
that names the same model without an effort keeps the effort already known.

The `state` field is `working`, `completed`, `idle`, or `attention`:

- Codex and Claude Code `UserPromptSubmit` become `working`, while `Stop`
  becomes `completed`. A Codex rollout modified within 30 seconds with a
  recent `task_started` record,
  no later native completion or abort, and a live process in its recorded
  directory is also recovered as `working` when its hook was missed. A session
  that has only started, without a submitted prompt or active task, has no
  entry; non-interactive runs submit a prompt like any other.
- Claude Code's `StopFailure` becomes `attention`. `Notification` also becomes
  `attention` for `permission_prompt`, `elicitation_dialog`, and
  `agent_needs_input`, and returns to `working` for `elicitation_complete` and
  `elicitation_response`. `SessionEnd` removes the entry.
- A turn the harness itself cannot finish also becomes `attention` with the
  reason `failure`. Codex closes a usage-limit turn with a completed event
  naming its error and fires no `Stop`, so the transcript tail is what sees the
  turn end. The error code must match exactly: an unknown code has not shown
  what it means, and a turn it belongs to keeps running until a hook says
  otherwise.
- A hook reports by a datagram it does not wait on, so a report can be lost.
  Claude Code records in its transcript that its `Stop` hooks ran (a
  `stop_hook_summary` record that did not prevent continuation), and the
  transcript tail settles the turn as `completed` from that record when the
  hook's own report never arrived.
- `working` is a claim the session's transcript has to back. A Codex or Claude
  turn whose transcript has been silent for 15 minutes with no tool call open
  becomes `idle`, dated from its last activity, without a notification. Claude
  Code's session registry (`sessions/<pid>.json` in its configuration
  directory, read up to 64 KiB and never written) can still vouch for it: a
  record naming the session as `busy` keeps it `working`.
- A completed thread becomes `idle` once it has waited five minutes, or as soon
  as Claude Code reports its own `idle_prompt`. Waiting is not attention: an
  idle report never creates a thread and never interrupts one that is working.
- pi `agent_start` becomes `working`, `agent_settled` becomes `completed` only
  after retries, automatic compaction, and queued continuations have finished,
  and the extension event `harness-status:attention` becomes `attention`.
  Cooperating extensions emit `harness-status:resume` with only a validated
  `working` or `completed` state after their interaction closes. They emit
  `harness-status:background` with a boolean while supervised work is active;
  Pi then ignores its intentional idle settlement until the child result wakes
  the parent. `session_shutdown` removes the entry.

An entry remains in the registry until acknowledged or its owning harness
process exits. Fresh activity updates the same entry. The `hidden` boolean is
a presentation hint: it becomes true after one hour in `idle`, unless
`background` is true. Working and attention entries are never auto-hidden.
Eww hides the whole thread widget when `hidden` is true; fresh activity makes
it visible again. The idle clock survives registry-owner restarts. Checkpoints
without an idle timestamp use five minutes after the last update as its start.
Claude Code and Pi also remove it when the session ends cleanly.

The optional fields carry what a bar needs beyond the state itself. `reason`
names why a thread wants the user — `permission`, `elicitation`, `subagent`, or
`failure` — as a category, never the request itself. `background` marks a thread
that stopped answering while its own work keeps running, which Claude Code
reports through the background tasks and session crons named in its `Stop`
event; only their presence is read, never their content. `started_at` is when
the current turn began, and only a new prompt starts it over — a resume returns
to a turn already under way and leaves its clock alone. Threads are ordered by
harness, project name, and identifier rather than by the order their events
arrived, so an indicator keeps its place while its neighbours come and go.

`duration` is that turn's length, already formatted, so a bar renders it without
arithmetic or a clock of its own: whole seconds below a minute, then whole
minutes, then `2h05m`, then whole days as `2d01h`, always the unit reached
rather than the nearest one. A
running turn is measured against now and a settled one against the moment it
settled, so a settled thread reports how long its turn ran instead of a number
that climbs while nothing happens. Because a running turn advances with no event
to announce it, follow mode republishes as the duration changes; a registry
whose threads all sit still emits nothing.

The hook does not retain prompts, transcript contents, permission details, tool
arguments, Claude's last response, or stop-failure details. Desktop notification
arguments contain only the harness display name, project display name, and
`finished` or `needs attention` with the reason category; they never contain
working directories, model or session identifiers, prompts, responses, tools, or
permission details.

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

Neither Codex nor Claude Code emits `Stop` for an Esc interruption, so while a
hook-sourced turn runs both adapters tail only the records their own hook added
to the transcript since the turn began. Codex recognizes its turn-abort event,
and Claude Code an interrupted-message record carrying the same prompt
identifier; either one retires the entry. A Codex rollout recovered from its
native records uses the same tail for later aborts and completions. The tail
also pairs each tool call with its result, and the checkpoint keeps only the
identifiers of calls still open, which is what lets a long-running tool keep a
silent turn `working`. Codex's
`turn_context` records repeat the active model and effort, including after an in-session
model switch, so the running entry keeps its model current from the transcript
tail. Codex also ends a usage-limit turn
without a `Stop` hook, naming `usage_limit_exceeded` in its completed event; that
settles the entry as attention instead of retiring it, because the thread is
waiting on the user's quota rather than gone. An unknown transcript format fails
safely and leaves the click-to-ack fallback. A replacement owner rewinds the transcript
cursors of running Codex and Claude turns before its first tail pass, so checkpoints saved
by an older binary are re-read from the turn's beginning and turns the older
binary missed the end of still settle correctly. Claude's assistant records also
name the answering model and effort, which is how a Claude entry reports them and
stays correct across an in-session model or effort switch. Per-turn effort takes
precedence over session effort, and synthetic error entries do not name a model.
Claude's prompt identifier
serves as its turn ID. Pi reports its active model but has no separate turn ID
in this contract, and its `agent_settled` event handles an Esc interruption
without transcript inspection.

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
unrelated definitions, and records the absolute path to the `harness-status`
executable resolved from `PATH`. Install the deployed binary before running the
installer so hook callbacks continue to use that installed copy even when the
installer was invoked through a build path. It is safe to repeat. To remove
only the `harness-status` callbacks, run:

```sh
harness-status uninstall codex
```

The example [`examples/codex-hooks.json`](examples/codex-hooks.json) remains
available for manual configuration. Codex combines matching hooks from its
active configuration layers.

Open `/hooks` in Codex once after installing or changing the definition and
trust both commands. The callbacks are short-lived because Codex command hooks
run synchronously. Codex rollout reconciliation remains available when a
callback is missed, but it cannot supply hook-only attention or background
metadata.

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
and records the absolute path to the `harness-status` executable resolved from
`PATH`. Install the deployed binary before running the installer so hook
callbacks continue to use that installed copy even when the installer was
invoked through a build path. It is idempotent, drops its own callbacks for
events it no longer uses, and removes only its own callbacks with:

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
`harness-status` executable resolved from `PATH`. Install the deployed binary
before running the installer so the extension continues to use that installed
copy even when the installer was invoked through a build path. It refuses to
overwrite or remove an unmanaged file at that path. Installation and removal
are idempotent:

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
(harness-usage :assets "./modules/harness-status/assets")
```

The module defines no window or bar and assumes no window manager, monitor,
paths, fonts, or consumer palette variables. Consumers can theme the semantic
classes `.harness-status__indicator--working`,
`.harness-status__indicator--completed`,
`.harness-status__indicator--idle`,
`.harness-status__indicator--attention`,
`.harness-status__indicator--reason-permission` and its siblings for the other
reasons, `.harness-status__background`, and one class per harness:
`.harness-status__indicator--codex`,
`.harness-status__indicator--claude`, and
`.harness-status__indicator--pi`. Working Codex turns use their ten-frame
Braille sequence, pi loops `π ϖ π ℼ` at 480 ms per frame, and Claude uses its
Linux pulse sequence (`· ✢ * ✶ ✻ ✽` and back) at 300 ms per frame. All
animation stays in Eww
rather than the JSON stream. Completed Codex turns use a small dot, completed
Claude turns use Claude Code's static `✻`, and completed pi turns use `π`.
Attention uses a static `!`, or `×` when a turn failed. An idle thread keeps its
settled glyph at reduced opacity for one hour before hiding. A thread whose
own work is still running stays visible and carries a trailing `⋯`. The tooltip
names the project, the state or the reason it wants the user, the model, and
the turn's `duration` as the stream already
formatted it, so the module needs no timer or clock variable of its own.
Clicking any state acknowledges the indicator.

The separate `harness-usage` widget shows each provider's SVG mark followed by
its available five-hour and seven-day percentages. Codex windows are classified
by their reported duration, so weekly-only subscriptions show one percentage.
A disabled Claude usage provider is
hidden; its cached values are not displayed or refreshed until usage tracking
is enabled. A leading `~` and reduced opacity mark retained stale values;
a question mark means neither window is available. Percentages count the
remaining allowance down from 100 to 0, and the tooltip labels them as left.
The provider icons are SVGL's [Codex](https://svgl.app/library/codex_dark.svg)
and [Claude](https://svgl.app/library/claude-ai-icon.svg) assets, recolored to
muted neutral and brand tones that remain visible on light and dark bars. A
middle dot separates the two windows, while the percent sign is visually
subordinate to the values. Its tooltip also
states provider freshness and observation age. The module rereads the local
report every 30 seconds; the command's shared persisted schedule keeps actual
provider requests at least three minutes apart. Multiple widget instances
consume the same JSON value without starting one request per provider or bar.

All adapters share the same semantic working/completed/attention classes, so pi
stays consistent with the other Eww indicators. Glyph choices live entirely in
the Eww module; the JSON contract exposes state data rather than presentation.

## Helper installation

The repository owns the five-hour Claude helper. Build and install the binary
and helper with:

```sh
make install
```

`BINDIR` and `AI_BINDIR` can override the default destinations, which are
`$XDG_BIN_HOME` or `~/.local/bin` and its `ai` subdirectory. Scheduling the
helper is external configuration; the repository ships no systemd unit. A user
service that runs it should invoke `ai-window-ping claude` through `sh -c`, so
the user manager's `PATH` from `environment.d` resolves the helper rather than
an absolute path.

## Verification

The project keeps focused end-to-end smoke tests rather than a broad unit-test
suite:

```sh
cargo test
```
