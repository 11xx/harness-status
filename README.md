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
        | JSON object per line
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
```

## Usage

```sh
# Query the currently running follower.
harness-status
harness-status codex

# Own the in-memory registry and print changed snapshots as JSONL.
harness-status --follow
harness-status codex --follow

# Commands used by integrations and status-bar actions.
harness-status hook codex
harness-status ack 019abc-thread-id
```

With no harness name, snapshot and follow modes include every supported
harness. Adding a harness name narrows the same JSON shape instead of creating a
provider-specific contract.

If no follower is running, snapshot mode prints `{"threads":[]}`. Only one
follower can own the registry at a time.

## JSON contract

```json
{
  "threads": [
    {
      "id": "019abc...",
      "harness": "codex",
      "state": "working",
      "cwd": "/home/lobo/code/example",
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
  another prompt, or the follower restarts.

The hook does not retain prompts, transcripts, permission details, or tool
arguments. When it can identify the owning Codex process through `/proc`, the
follower removes a `working` entry if that process exits. `ack` can dismiss any
entry, including an interrupted turn that did not emit `Stop`.

## Codex hooks

Install the binary first, then merge [`examples/codex-hooks.json`](examples/codex-hooks.json)
into `~/.codex/hooks.json`. Codex combines matching hooks from its active
configuration layers.

Open `/hooks` in Codex once after installing or changing the definition and
trust both commands. The callbacks are short-lived because Codex command hooks
run synchronously.

T3 Code launches the system Codex app-server with the inherited environment,
so the same user-level hooks and runtime socket work without wrapping either
program.

## Eww

[`examples/harness-status.yuck`](examples/harness-status.yuck) contains a small
Codex-only listener and indicator widget. Include it and place
`(harness-indicators)` wherever the indicators should appear in the bar.
Working Codex turns use the same ten-frame Braille spinner sequence as the
Codex terminal title; completed turns use a check mark.

The listener process is the in-memory owner:

```yuck
(deflisten harness-status-json
  :initial "{\"threads\":[]}"
  "harness-status codex --follow")
```

Use `harness-status --follow` instead when the bar should include all harnesses.

## Verification

The project keeps one end-to-end smoke test rather than a broad unit-test suite:

```sh
cargo test
```
