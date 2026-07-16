# harness-status

`harness-status` turns AI harness lifecycle hooks into a small, stable JSON
contract for status bars. The process started with `--follow` owns the live
registry and keeps a tiny volatile restart checkpoint; there is no database,
history store, wrapper, or persistent daemon.

The adapters support Codex and Claude Code behind the same CLI and JSON shape.

## How it works

```text
Codex or Claude Code lifecycle event
        |
        | starts `harness-status hook codex|claude`
        v
short-lived hook command
        |
        | non-blocking Unix datagram
        v
harness-status [codex|claude] --follow
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
checkpoint disappear at logout or reboot.

## Build and install

Rust 1.97 or newer is recommended.

```sh
cargo build --release
cargo install --path . --root "$HOME/.local"
harness-status install codex
harness-status install claude
```

## Usage

```sh
# Query the currently running follower.
harness-status
harness-status codex
harness-status claude

# Own the registry, or subscribe if another follower already owns it.
harness-status --follow
harness-status codex --follow
harness-status claude --follow

# Commands used by integrations and status-bar actions.
harness-status hook codex
harness-status hook claude
harness-status ack 019abc-thread-id

# Add or remove lifecycle callbacks from the harness's user configuration.
harness-status install codex
harness-status uninstall codex
harness-status install claude
harness-status uninstall claude
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

Both adapters intentionally report only states supported cleanly by their
lifecycle hooks:

- `UserPromptSubmit` becomes `working`.
- `Stop` becomes `completed` and remains visible until acknowledged, reused by
  another prompt, or its owning harness process exits.
- Claude Code's `StopFailure` also becomes `completed`: the turn is no longer
  working and needs attention, but the adapter does not expose or retain its
  error details.

The hook does not retain prompts, transcript contents, permission details, tool
arguments, Claude's last response, or stop-failure details. When it can identify
the owning harness process through `/proc`, the follower removes the entry after
that process exits.

To recognize an Esc interruption that does not emit `Stop`, the Codex adapter
tails only new records from the hook-provided transcript path and reads the
event type and turn ID. An unknown transcript format fails safely and leaves the
click-to-ack fallback. Claude Code also omits `Stop` on user interruption, but
its transcript does not expose Codex's turn-abort record. A Claude working entry
therefore remains until its next lifecycle event, process exit, or manual
acknowledgement. Claude's hook input does not include the active model or a turn
ID, so those optional JSON fields are absent for Claude entries.

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
and records the absolute path to the current executable. It is idempotent, and
removes only its own callbacks with:

```sh
harness-status uninstall claude
```

Claude Code watches settings for changes, so new sessions do not require a
wrapper or daemon. Open `/hooks` to verify the active user callbacks. The
example [`examples/claude-settings.json`](examples/claude-settings.json) is
available for manual configuration. See Claude Code's official
[hooks reference](https://code.claude.com/docs/en/hooks) for the lifecycle and
input contract.

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
```

The module defines no window or bar and assumes no window manager, monitor,
paths, fonts, or palette. Consumers can theme the semantic classes
`.harness-status__indicator--working`,
`.harness-status__indicator--completed`,
`.harness-status__indicator--codex`, and
`.harness-status__indicator--claude`. Working Codex turns use the same
ten-frame Braille spinner sequence as the Codex terminal title. Working Claude
turns use Claude Code's Linux pulse sequence (`· ✢ * ✶ ✻ ✽` and back). Both
animate in Eww rather than in the JSON stream. Completed Codex turns use a
small dot, while completed Claude turns use Claude Code's static `✻`. Clicking
either state acknowledges the indicator. These glyph choices live entirely in
the Eww module; the JSON contract exposes state data rather than presentation
icons.

## Verification

The project keeps focused end-to-end smoke tests rather than a broad unit-test
suite:

```sh
cargo test
```
