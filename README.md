# harness-status

Live state of your Codex, Claude Code, and pi sessions, as JSON for a status
bar.

With several agent sessions open, the only way to know which one finished and
which is stuck on a permission prompt is to visit each terminal. A turn that
needs you waits unnoticed, and a finished one waits for you to come back.

`harness-status` installs lifecycle hooks into each harness and gathers their
events in one registry. It prints that registry as a snapshot, or as one JSON
object per line whenever it changes. Each thread is `working`, `completed`,
`idle`, or `attention`, with its project, model, turn duration, and the reason
it wants you. The process that owns the registry can also send a desktop
notification when a turn settles or needs attention.

Hooks cannot slow or break a harness: each sends one non-blocking datagram and
exits successfully whether or not anything is listening. The registry keeps
status metadata only, never prompts, transcript contents, tool arguments, or
responses. There is no database or persistent daemon; state lives in the
`--follow` process and a checkpoint in the runtime directory, which is cleared
at logout.

## Install

Linux, built from source. The checkout pins Rust 1.97.0 in
`rust-toolchain.toml`, which rustup installs on first use.

```sh
make install                    # harness-status into $XDG_BIN_HOME or ~/.local/bin
harness-status install codex    # add hooks to the harness's user configuration
harness-status install claude
harness-status install pi       # writes a global pi extension
```

The installers record the absolute path of the `harness-status` found on
`PATH`, so install the binary before the hooks. `uninstall HARNESS` removes
only what `install` added. `make install` also installs `ai-window-ping`, an
optional helper that sends Claude a one-word prompt to start its five-hour
usage window; `cargo install --path .` installs the binary alone.

## Watch sessions from a status bar

```sh
harness-status --follow           # own the registry; print a JSON line per change
harness-status claude --follow    # the same stream, narrowed to one harness
harness-status                    # one snapshot from the running follower
harness-status ack 019abc...      # dismiss a thread by its id
```

The first `--follow` process owns the registry and any later ones subscribe to
it, so several bars or monitors can share one stream. With no follower running,
a snapshot prints `{"threads":[]}`. The `eww/` directory holds an Eww module
with per-harness indicators and a usage widget; any bar that reads JSON lines
works the same way.

## Check account usage

```sh
harness-status usage              # Codex and Claude rate-limit windows as JSON
harness-status usage disable      # stop tracking Claude usage; Codex is unaffected
```

Providers are asked at most once every three minutes, and every bar reads the
same cached report. Claude's figures come from its non-public OAuth usage
endpoint, using the existing access token without refreshing or rewriting it.

## More

`harness-status guide` explains the states and how to read an empty snapshot,
and `harness-status --help` lists every command.
[`docs/reference.md`](docs/reference.md) documents the JSON contract, each
harness's hooks, the Eww module, notifications, and the registry's internals.

## License

Released into the public domain under the Unlicense; see [`UNLICENSE`](UNLICENSE).
