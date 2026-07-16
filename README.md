# harness-status

`harness-status` turns AI harness lifecycle hooks into a small, stable JSON
contract for status bars. Its registry exists only in the process started with
`--follow`: there is no database, state file, wrapper, or persistent daemon.

The initial adapter supports Codex. The CLI and JSON shape leave room for other
harnesses without making their implementations part of the Codex version.

## How it works

```text
Codex lifecycle event
        |
        | starts `harness-status hook codex`
        v
short-lived hook command
        |
        | non-blocking Unix datagram
        v
harness-status [codex] --follow
        |
        | owns the registry, or subscribes to its existing owner
        | JSON object per line for each follower
        v
Eww deflisten
```

The hook command reads the JSON object Codex provides on standard input, keeps
only status metadata, attempts one non-blocking send, and exits. It deliberately
returns success when no follower is running so status reporting cannot delay or
break the harness.

The socket is an ephemeral IPC endpoint at
`$XDG_RUNTIME_DIR/harness-status.sock`, restricted to the current user. Set
`HARNESS_STATUS_SOCKET` to override it. All thread state remains in memory and
is lost when the follower exits.

## Build and install

Rust 1.97 or newer is recommended.

```sh
cargo build --release
cargo install --path . --root "$HOME/.local"
harness-status install codex
```

## Usage

```sh
# Query the currently running follower.
harness-status
harness-status codex

# Own the registry, or subscribe if another follower already owns it.
harness-status --follow
harness-status codex --follow

# Commands used by integrations and status-bar actions.
harness-status hook codex
harness-status ack 019abc-thread-id

# Add or remove lifecycle callbacks from the harness's user configuration.
harness-status install codex
harness-status uninstall codex
```

With no harness name, snapshot and follow modes include every supported
harness. Adding a harness name narrows the same JSON shape instead of creating a
provider-specific contract.

If no follower is running, snapshot mode prints `{"threads":[]}`. Only one
follower owns the registry, but any number of later `--follow` processes can
subscribe to its current snapshot and future changes. Each subscriber keeps
its own optional harness filter.

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

The Codex adapter intentionally reports only states supported cleanly by
lifecycle hooks:

- `UserPromptSubmit` becomes `working`.
- `Stop` becomes `completed` and remains visible until acknowledged, reused by
  another prompt, its owning Codex process exits, or the follower restarts.

The hook does not retain prompts, transcript contents, permission details, or
tool arguments. When it can identify the owning Codex process through `/proc`,
the follower removes any entry after that process exits. To recognize an Esc
interruption that does not emit `Stop`, the Codex adapter tails only new records
from the hook-provided transcript path and reads the event type and turn ID. An
unknown transcript format fails safely and leaves the click-to-ack fallback.

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
```

The module defines no window or bar and assumes no window manager, monitor,
paths, fonts, or palette. Consumers can theme the semantic classes
`.harness-status__indicator--working` and
`.harness-status__indicator--completed`. Working Codex turns use the same
ten-frame Braille spinner sequence as the Codex terminal title; completed
turns use a small dot. Clicking either state acknowledges the indicator.
These glyph choices live entirely in the Eww module; the JSON contract exposes
state data rather than presentation icons.

## Verification

The project keeps one end-to-end smoke test rather than a broad unit-test suite:

```sh
cargo test
```
