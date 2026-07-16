use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_SOCKET_NAME: &str = "harness-status.sock";
const MAX_DATAGRAM_SIZE: usize = 64 * 1024;
const QUERY_TIMEOUT: Duration = Duration::from_millis(250);
const REAP_INTERVAL: Duration = Duration::from_millis(500);
const SUBSCRIBER_HEARTBEAT: Duration = Duration::from_secs(2);
const STATE_VERSION: u32 = 1;
const CODEX_HOOK_EVENTS: [&str; 2] = ["UserPromptSubmit", "Stop"];
const LEGACY_CODEX_HOOK_COMMAND: &str = "harness-status hook codex";

#[derive(Debug)]
enum Action {
    Snapshot { harness: Option<String> },
    Follow { harness: Option<String> },
    Hook { harness: String },
    Ack { id: String },
    Install { harness: String },
    Uninstall { harness: String },
    Help,
    Version,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Owner {
    pid: u32,
    start_ticks: u64,
}

#[derive(Debug, Deserialize)]
struct CodexHookInput {
    hook_event_name: String,
    session_id: String,
    #[serde(default)]
    turn_id: Option<String>,
    #[serde(default)]
    transcript_path: Option<PathBuf>,
    #[serde(default)]
    cwd: String,
    #[serde(default)]
    model: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct HookEvent {
    event: String,
    session_id: String,
    turn_id: Option<String>,
    transcript: Option<TranscriptCursor>,
    cwd: String,
    model: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TranscriptCursor {
    path: PathBuf,
    offset: u64,
}

#[derive(Deserialize)]
struct TranscriptRecord {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    payload: TranscriptPayload,
}

#[derive(Default, Deserialize)]
struct TranscriptPayload {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    turn_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Request {
    Hook {
        harness: String,
        event: HookEvent,
        owner: Option<Owner>,
    },
    Snapshot {
        harness: Option<String>,
    },
    Subscribe {
        harness: Option<String>,
    },
    Ack {
        id: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ThreadStatus {
    id: String,
    harness: String,
    state: String,
    cwd: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    turn_id: Option<String>,
    updated_at: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct Entry {
    status: ThreadStatus,
    owner: Option<Owner>,
    transcript: Option<TranscriptCursor>,
}

#[derive(Debug)]
struct Subscriber {
    path: PathBuf,
    harness: Option<String>,
    last_json: String,
}

#[derive(Serialize)]
struct Snapshot<'a> {
    threads: Vec<&'a ThreadStatus>,
}

#[derive(Default)]
struct Registry {
    entries: Vec<Entry>,
}

#[derive(Deserialize)]
struct StoredRegistry {
    version: u32,
    entries: Vec<Entry>,
}

#[derive(Serialize)]
struct RegistryCheckpoint<'a> {
    version: u32,
    entries: &'a [Entry],
}

impl Registry {
    fn apply_hook(&mut self, harness: &str, event: HookEvent, owner: Option<Owner>) -> bool {
        let HookEvent {
            event,
            session_id,
            turn_id,
            transcript,
            cwd,
            model,
        } = event;
        let state = match (harness, event.as_str()) {
            ("codex", "UserPromptSubmit") => "working",
            ("codex", "Stop") => "completed",
            _ => return false,
        };

        let now = unix_time_seconds();
        let name = display_name(&cwd);
        let model = nonempty(model);
        let starts_turn = state == "working";

        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.status.harness == harness && entry.status.id == session_id)
        {
            entry.status.state = state.to_owned();
            if !cwd.is_empty() {
                entry.status.cwd = cwd;
                entry.status.name = name;
            }
            if model.is_some() {
                entry.status.model = model;
            }
            entry.status.turn_id = turn_id;
            entry.status.updated_at = now;
            if starts_turn || owner.is_some() {
                entry.owner = owner;
            }
            if starts_turn || transcript.is_some() {
                entry.transcript = transcript;
            }
            return true;
        }

        self.entries.push(Entry {
            status: ThreadStatus {
                id: session_id,
                harness: harness.to_owned(),
                state: state.to_owned(),
                cwd,
                name,
                model,
                turn_id,
                updated_at: now,
            },
            owner,
            transcript,
        });
        true
    }

    fn acknowledge(&mut self, id: &str) -> bool {
        let before = self.entries.len();
        self.entries.retain(|entry| entry.status.id != id);
        before != self.entries.len()
    }

    fn reap_dead_owners(&mut self) -> bool {
        let before = self.entries.len();
        self.entries
            .retain(|entry| entry.owner.as_ref().is_none_or(process_matches_owner));
        before != self.entries.len()
    }

    fn reap_interrupted_turns(&mut self) -> bool {
        let before = self.entries.len();
        self.entries.retain_mut(|entry| {
            if entry.status.state != "working" {
                return true;
            }
            let Some(turn_id) = entry.status.turn_id.as_deref() else {
                return true;
            };
            let Some(transcript) = entry.transcript.as_mut() else {
                return true;
            };
            !transcript_reports_abort(transcript, turn_id).unwrap_or(false)
        });
        before != self.entries.len()
    }

    fn snapshot_json(&self, harness: Option<&str>) -> String {
        let threads = self
            .entries
            .iter()
            .filter(|entry| harness.is_none_or(|name| entry.status.harness == name))
            .map(|entry| &entry.status)
            .collect();
        serde_json::to_string(&Snapshot { threads }).expect("serializing status cannot fail")
    }
}

struct SocketGuard {
    path: PathBuf,
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn main() {
    let action = match parse_args() {
        Ok(action) => action,
        Err(message) => {
            eprintln!("harness-status: {message}");
            eprintln!("Try 'harness-status --help' for usage.");
            process::exit(2);
        }
    };

    let result = match action {
        Action::Snapshot { harness } => print_snapshot(harness.as_deref()),
        Action::Follow { harness } => follow(harness.as_deref()),
        Action::Hook { harness } => {
            send_hook(&harness);
            Ok(())
        }
        Action::Ack { id } => acknowledge(&id),
        Action::Install { harness } => install_integration(&harness),
        Action::Uninstall { harness } => uninstall_integration(&harness),
        Action::Help => {
            print_help();
            Ok(())
        }
        Action::Version => {
            println!("harness-status {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    };

    if let Err(error) = result {
        eprintln!("harness-status: {error}");
        process::exit(1);
    }
}

fn parse_args() -> Result<Action, String> {
    let mut args = env::args().skip(1).peekable();
    let Some(first) = args.next() else {
        return Ok(Action::Snapshot { harness: None });
    };

    match first.as_str() {
        "-h" | "--help" => no_more_args(args, Action::Help),
        "-V" | "--version" => no_more_args(args, Action::Version),
        "hook" => {
            let harness = args
                .next()
                .ok_or_else(|| "'hook' requires a harness name".to_owned())?;
            no_more_args(args, Action::Hook { harness })
        }
        "ack" => {
            let id = args
                .next()
                .ok_or_else(|| "'ack' requires a thread id".to_owned())?;
            no_more_args(args, Action::Ack { id })
        }
        "install" => {
            let harness = args
                .next()
                .ok_or_else(|| "'install' requires a harness name".to_owned())?;
            no_more_args(args, Action::Install { harness })
        }
        "uninstall" => {
            let harness = args
                .next()
                .ok_or_else(|| "'uninstall' requires a harness name".to_owned())?;
            no_more_args(args, Action::Uninstall { harness })
        }
        "-f" | "--follow" => {
            let harness = args.next();
            no_more_args(args, Action::Follow { harness })
        }
        value if value.starts_with('-') => Err(format!("unknown option '{value}'")),
        harness => match args.next().as_deref() {
            None => Ok(Action::Snapshot {
                harness: Some(harness.to_owned()),
            }),
            Some("-f" | "--follow") if args.next().is_none() => Ok(Action::Follow {
                harness: Some(harness.to_owned()),
            }),
            Some(other) => Err(format!("unexpected argument '{other}'")),
        },
    }
}

fn no_more_args(mut args: impl Iterator<Item = String>, action: Action) -> Result<Action, String> {
    if let Some(extra) = args.next() {
        Err(format!("unexpected argument '{extra}'"))
    } else {
        Ok(action)
    }
}

fn print_help() {
    println!(
        "\
Usage:
  harness-status [HARNESS] [--follow]
  harness-status --follow [HARNESS]
  harness-status hook HARNESS
  harness-status ack THREAD_ID
  harness-status install HARNESS
  harness-status uninstall HARNESS

Print an in-memory AI harness status snapshot as JSON, or stream changed
snapshots as JSONL. With no HARNESS, snapshot and follow modes include all
configured harnesses.

Commands:
  hook HARNESS        Forward a harness hook event from stdin and exit
  ack THREAD_ID       Remove a thread from the current registry
  install HARNESS     Install the harness integration in its user config
  uninstall HARNESS   Remove the harness integration from its user config

Options:
  -f, --follow       Own or subscribe to the registry and print changed JSON
  -h, --help         Show this help
  -V, --version      Show the version"
    );
}

fn install_integration(harness: &str) -> io::Result<()> {
    match harness {
        "codex" => install_codex_hooks(),
        _ => Err(unsupported_harness(harness)),
    }
}

fn uninstall_integration(harness: &str) -> io::Result<()> {
    match harness {
        "codex" => uninstall_codex_hooks(),
        _ => Err(unsupported_harness(harness)),
    }
}

fn unsupported_harness(harness: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("unsupported harness '{harness}'"),
    )
}

fn install_codex_hooks() -> io::Result<()> {
    let display_path = codex_hooks_path()?;
    let path = writable_config_path(&display_path)?;
    let mut document = read_hook_document(&path, true)?;
    let command = codex_hook_command()?;
    let target_handler = json!({
        "type": "command",
        "command": command,
        "timeout": 5
    });

    let root = root_object_mut(&mut document, &display_path)?;
    let hooks = hooks_object_mut(root, &display_path)?;
    let mut changed = false;
    for event in CODEX_HOOK_EVENTS {
        changed |= install_event_handler(hooks, event, &target_handler, &command)?;
    }

    if changed {
        write_json_atomic(&path, &document)?;
        println!("Installed Codex hooks in {}.", display_path.display());
        println!("Restart Codex, then open /hooks to review and trust them.");
    } else {
        println!(
            "Codex hooks are already installed in {}.",
            display_path.display()
        );
    }
    Ok(())
}

fn uninstall_codex_hooks() -> io::Result<()> {
    let display_path = codex_hooks_path()?;
    let path = writable_config_path(&display_path)?;
    if !path.exists() {
        println!(
            "Codex hooks are already uninstalled from {}.",
            display_path.display()
        );
        return Ok(());
    }

    let mut document = read_hook_document(&path, false)?;
    let command = codex_hook_command()?;
    let root = root_object_mut(&mut document, &display_path)?;
    let mut changed = false;
    let mut hooks_are_empty = false;

    if let Some(hooks_value) = root.get_mut("hooks") {
        let hooks = hooks_value.as_object_mut().ok_or_else(|| {
            invalid_hook_config(&display_path, "top-level 'hooks' must be an object")
        })?;
        let mut empty_events = Vec::new();
        for event in CODEX_HOOK_EVENTS {
            if let Some(groups_value) = hooks.get_mut(event) {
                let groups = groups_value.as_array_mut().ok_or_else(|| {
                    invalid_hook_config(&display_path, &format!("hooks.{event} must be an array"))
                })?;
                changed |= remove_owned_handlers(groups, event, &command)? > 0;
                if groups.is_empty() {
                    empty_events.push(event);
                }
            }
        }
        for event in empty_events {
            hooks.remove(event);
        }
        hooks_are_empty = hooks.is_empty();
    }

    if hooks_are_empty {
        root.remove("hooks");
    }

    if changed {
        write_json_atomic(&path, &document)?;
        println!("Uninstalled Codex hooks from {}.", display_path.display());
        println!("Restart Codex to stop using the removed definitions.");
    } else {
        println!(
            "Codex hooks are already uninstalled from {}.",
            display_path.display()
        );
    }
    Ok(())
}

fn codex_hooks_path() -> io::Result<PathBuf> {
    if let Some(codex_home) = env::var_os("CODEX_HOME").filter(|path| !path.is_empty()) {
        return Ok(PathBuf::from(codex_home).join("hooks.json"));
    }
    let home = env::var_os("HOME")
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "neither CODEX_HOME nor HOME is set",
            )
        })?;
    Ok(PathBuf::from(home).join(".codex/hooks.json"))
}

fn writable_config_path(path: &Path) -> io::Result<PathBuf> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => fs::canonicalize(path),
        Ok(_) => Ok(path.to_owned()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(path.to_owned()),
        Err(error) => Err(error),
    }
}

fn codex_hook_command() -> io::Result<String> {
    let executable = env::current_exe()?;
    let executable = executable.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "harness-status executable path is not valid UTF-8",
        )
    })?;
    Ok(format!("{} hook codex", shell_quote(executable)))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn read_hook_document(path: &Path, allow_missing: bool) -> io::Result<Value> {
    match fs::read_to_string(path) {
        Ok(contents) => serde_json::from_str(&contents)
            .map_err(|error| invalid_hook_config(path, &format!("invalid JSON: {error}"))),
        Err(error) if allow_missing && error.kind() == io::ErrorKind::NotFound => Ok(json!({})),
        Err(error) => Err(error),
    }
}

fn root_object_mut<'a>(
    document: &'a mut Value,
    path: &Path,
) -> io::Result<&'a mut Map<String, Value>> {
    document
        .as_object_mut()
        .ok_or_else(|| invalid_hook_config(path, "the document root must be an object"))
}

fn hooks_object_mut<'a>(
    root: &'a mut Map<String, Value>,
    path: &Path,
) -> io::Result<&'a mut Map<String, Value>> {
    if !root.contains_key("hooks") {
        root.insert("hooks".to_owned(), json!({}));
    }
    root.get_mut("hooks")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid_hook_config(path, "top-level 'hooks' must be an object"))
}

fn install_event_handler(
    hooks: &mut Map<String, Value>,
    event: &str,
    target_handler: &Value,
    command: &str,
) -> io::Result<bool> {
    if !hooks.contains_key(event) {
        hooks.insert(event.to_owned(), json!([]));
    }
    let groups = hooks
        .get_mut(event)
        .and_then(Value::as_array_mut)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("hooks.{event} must be an array"),
            )
        })?;
    validate_event_groups(groups, event)?;

    let owned: Vec<&Value> = groups
        .iter()
        .flat_map(|group| group["hooks"].as_array().expect("validated hook group"))
        .filter(|handler| handler_is_owned(handler, command))
        .collect();
    if owned.len() == 1 && owned[0] == target_handler {
        return Ok(false);
    }

    remove_owned_handlers(groups, event, command)?;
    groups.push(json!({ "hooks": [target_handler.clone()] }));
    Ok(true)
}

fn remove_owned_handlers(groups: &mut Vec<Value>, event: &str, command: &str) -> io::Result<usize> {
    validate_event_groups(groups, event)?;
    let mut removed = 0;
    for group in groups.iter_mut() {
        let handlers = group["hooks"].as_array_mut().expect("validated hook group");
        handlers.retain(|handler| {
            if handler_is_owned(handler, command) {
                removed += 1;
                false
            } else {
                true
            }
        });
    }
    groups.retain(|group| {
        !group["hooks"]
            .as_array()
            .expect("validated hook group")
            .is_empty()
    });
    Ok(removed)
}

fn validate_event_groups(groups: &[Value], event: &str) -> io::Result<()> {
    for (index, group) in groups.iter().enumerate() {
        let valid = group
            .as_object()
            .and_then(|group| group.get("hooks"))
            .is_some_and(Value::is_array);
        if !valid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("hooks.{event}[{index}].hooks must be an array"),
            ));
        }
    }
    Ok(())
}

fn handler_is_owned(handler: &Value, command: &str) -> bool {
    handler.get("type").and_then(Value::as_str) == Some("command")
        && handler
            .get("command")
            .and_then(Value::as_str)
            .is_some_and(|candidate| candidate == command || candidate == LEGACY_CODEX_HOOK_COMMAND)
}

fn invalid_hook_config(path: &Path, detail: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("cannot update {}: {detail}", path.display()),
    )
}

fn write_json_atomic(path: &Path, document: &Value) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "hook path has no parent directory",
        )
    })?;
    fs::create_dir_all(parent)?;

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = parent.join(format!(
        ".hooks.json.harness-status-{}-{nonce}",
        process::id()
    ));
    let permissions = fs::metadata(path)
        .map(|metadata| metadata.permissions().mode() & 0o777)
        .unwrap_or(0o600);
    let mut contents = serde_json::to_vec_pretty(document).map_err(io::Error::other)?;
    contents.push(b'\n');

    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(permissions))?;
        file.write_all(&contents)?;
        file.sync_all()?;
        fs::rename(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn follow(harness: Option<&str>) -> io::Result<()> {
    let path = socket_path()?;
    prepare_socket_parent(&path)?;
    let mut last_json = None;

    loop {
        if path.exists() {
            match query_snapshot(None) {
                Ok(_) => {
                    follow_existing(&path, harness, &mut last_json)?;
                    continue;
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                    ) =>
                {
                    fs::remove_file(&path)?;
                }
                Err(error) => return Err(error),
            }
        }

        match UnixDatagram::bind(&path) {
            Ok(socket) => return follow_as_owner(socket, &path, harness, &mut last_json),
            Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                follow_existing(&path, harness, &mut last_json)?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn follow_as_owner(
    socket: UnixDatagram,
    path: &Path,
    harness: Option<&str>,
    last_json: &mut Option<String>,
) -> io::Result<()> {
    let _guard = SocketGuard {
        path: path.to_owned(),
    };
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    socket.set_read_timeout(Some(REAP_INTERVAL))?;

    let state_path = registry_state_path(path);
    let mut registry = load_registry(&state_path);
    registry.reap_interrupted_turns();
    registry.reap_dead_owners();
    checkpoint_registry(&state_path, &registry);
    let mut subscribers = Vec::new();
    let initial_json = registry.snapshot_json(harness);
    print_json_if_changed(last_json, initial_json);

    let mut buffer = vec![0_u8; MAX_DATAGRAM_SIZE];
    loop {
        let mut changed = false;
        match socket.recv_from(&mut buffer) {
            Ok((length, sender)) => {
                if let Ok(request) = serde_json::from_slice::<Request>(&buffer[..length]) {
                    match request {
                        Request::Hook {
                            harness: source,
                            event,
                            owner,
                        } => changed = registry.apply_hook(&source, event, owner),
                        Request::Snapshot { harness: requested } => {
                            if let Some(sender_path) = sender.as_pathname() {
                                let response = registry.snapshot_json(requested.as_deref());
                                let _ = socket.send_to(response.as_bytes(), sender_path);
                            }
                        }
                        Request::Subscribe { harness: requested } => {
                            if let Some(sender_path) = sender.as_pathname() {
                                subscribe(
                                    &socket,
                                    &registry,
                                    &mut subscribers,
                                    sender_path,
                                    requested,
                                );
                            }
                        }
                        Request::Ack { id } => changed = registry.acknowledge(&id),
                    }
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error),
        }

        changed |= registry.reap_interrupted_turns();
        changed |= registry.reap_dead_owners();
        if changed {
            checkpoint_registry(&state_path, &registry);
            let json = registry.snapshot_json(harness);
            print_json_if_changed(last_json, json);
            publish_subscribers(&socket, &registry, &mut subscribers);
        }
    }
}

fn prepare_socket_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn follow_existing(
    server_path: &Path,
    harness: Option<&str>,
    last_json: &mut Option<String>,
) -> io::Result<()> {
    let client_path = client_socket_path(server_path, "follow");
    let _ = fs::remove_file(&client_path);
    let socket = UnixDatagram::bind(&client_path)?;
    let _guard = SocketGuard {
        path: client_path.clone(),
    };
    fs::set_permissions(&client_path, fs::Permissions::from_mode(0o600))?;
    socket.set_read_timeout(Some(SUBSCRIBER_HEARTBEAT))?;

    let request = Request::Subscribe {
        harness: harness.map(str::to_owned),
    };
    let bytes = serde_json::to_vec(&request).map_err(io::Error::other)?;
    if let Err(error) = send_subscription(&socket, &bytes, server_path) {
        if error.kind() == io::ErrorKind::BrokenPipe {
            return Ok(());
        }
        return Err(error);
    }

    let mut buffer = vec![0_u8; MAX_DATAGRAM_SIZE];
    loop {
        match socket.recv(&mut buffer) {
            Ok(length) => {
                let json = String::from_utf8(buffer[..length].to_vec())
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                print_json_if_changed(last_json, json);
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if let Err(error) = send_subscription(&socket, &bytes, server_path) {
                    if error.kind() == io::ErrorKind::BrokenPipe {
                        return Ok(());
                    }
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

fn registry_state_path(socket_path: &Path) -> PathBuf {
    let mut path = socket_path.to_owned();
    path.set_extension("state.json");
    path
}

fn load_registry(path: &Path) -> Registry {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Registry::default(),
        Err(error) => {
            eprintln!(
                "harness-status: cannot read runtime state {}: {error}",
                path.display()
            );
            return Registry::default();
        }
    };
    let stored = match serde_json::from_slice::<StoredRegistry>(&bytes) {
        Ok(stored) => stored,
        Err(error) => {
            eprintln!(
                "harness-status: ignoring invalid runtime state {}: {error}",
                path.display()
            );
            return Registry::default();
        }
    };
    if stored.version != STATE_VERSION {
        eprintln!(
            "harness-status: ignoring runtime state version {} in {}",
            stored.version,
            path.display()
        );
        return Registry::default();
    }
    Registry {
        entries: stored.entries,
    }
}

fn checkpoint_registry(path: &Path, registry: &Registry) {
    let checkpoint = RegistryCheckpoint {
        version: STATE_VERSION,
        entries: &registry.entries,
    };
    if let Err(error) = write_registry_checkpoint(path, &checkpoint) {
        eprintln!(
            "harness-status: cannot checkpoint runtime state {}: {error}",
            path.display()
        );
    }
}

fn write_registry_checkpoint(path: &Path, checkpoint: &RegistryCheckpoint<'_>) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "runtime state path has no parent directory",
        )
    })?;
    fs::create_dir_all(parent)?;

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = parent.join(format!(
        ".harness-status-state-{}-{nonce}.json",
        process::id()
    ));
    let mut contents = serde_json::to_vec(checkpoint).map_err(io::Error::other)?;
    contents.push(b'\n');

    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
        file.write_all(&contents)?;
        file.sync_all()?;
        fs::rename(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn print_json_if_changed(last_json: &mut Option<String>, json: String) {
    if last_json.as_deref() != Some(json.as_str()) {
        print_json_line(&json);
        *last_json = Some(json);
    }
}

fn send_subscription(socket: &UnixDatagram, bytes: &[u8], server_path: &Path) -> io::Result<()> {
    socket
        .send_to(bytes, server_path)
        .map(|_| ())
        .map_err(|error| {
            if missing_listener(&error) {
                io::Error::new(io::ErrorKind::BrokenPipe, "harness-status owner stopped")
            } else {
                error
            }
        })
}

fn subscribe(
    socket: &UnixDatagram,
    registry: &Registry,
    subscribers: &mut Vec<Subscriber>,
    path: &Path,
    harness: Option<String>,
) {
    let json = registry.snapshot_json(harness.as_deref());
    if socket.send_to(json.as_bytes(), path).is_err() {
        return;
    }

    if let Some(subscriber) = subscribers
        .iter_mut()
        .find(|subscriber| subscriber.path == path)
    {
        subscriber.harness = harness;
        subscriber.last_json = json;
    } else {
        subscribers.push(Subscriber {
            path: path.to_owned(),
            harness,
            last_json: json,
        });
    }
}

fn publish_subscribers(
    socket: &UnixDatagram,
    registry: &Registry,
    subscribers: &mut Vec<Subscriber>,
) {
    subscribers.retain_mut(|subscriber| {
        let json = registry.snapshot_json(subscriber.harness.as_deref());
        if json == subscriber.last_json {
            return true;
        }
        if socket.send_to(json.as_bytes(), &subscriber.path).is_err() {
            return false;
        }
        subscriber.last_json = json;
        true
    });
}

fn send_hook(harness: &str) {
    let event = match harness {
        "codex" => serde_json::from_reader::<_, CodexHookInput>(io::stdin().lock()).map(|input| {
            let transcript = input.transcript_path.map(|path| TranscriptCursor {
                offset: fs::metadata(&path)
                    .map(|metadata| metadata.len())
                    .unwrap_or(0),
                path,
            });
            HookEvent {
                event: input.hook_event_name,
                session_id: input.session_id,
                turn_id: input.turn_id,
                transcript,
                cwd: input.cwd,
                model: input.model,
            }
        }),
        _ => return,
    };
    let Ok(event) = event else {
        return;
    };

    let request = Request::Hook {
        harness: harness.to_owned(),
        event,
        owner: find_codex_owner(),
    };
    send_fire_and_forget(&request);
}

fn transcript_reports_abort(cursor: &mut TranscriptCursor, turn_id: &str) -> io::Result<bool> {
    let mut file = fs::File::open(&cursor.path)?;
    if file.metadata()?.len() < cursor.offset {
        cursor.offset = 0;
    }
    file.seek(SeekFrom::Start(cursor.offset))?;
    let mut reader = BufReader::new(file);

    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 || !line.ends_with('\n') {
            return Ok(false);
        }
        cursor.offset += bytes as u64;

        let Ok(record) = serde_json::from_str::<TranscriptRecord>(&line) else {
            continue;
        };
        if record.kind == "event_msg"
            && record.payload.kind == "turn_aborted"
            && record.payload.turn_id.as_deref() == Some(turn_id)
        {
            return Ok(true);
        }
    }
}

fn acknowledge(id: &str) -> io::Result<()> {
    let request = Request::Ack { id: id.to_owned() };
    let bytes = serde_json::to_vec(&request).map_err(io::Error::other)?;
    let socket = UnixDatagram::unbound()?;
    socket.set_nonblocking(true)?;
    let path = socket_path()?;
    match socket.send_to(&bytes, path) {
        Ok(_) => Ok(()),
        Err(error) if missing_listener(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

fn send_fire_and_forget(request: &Request) {
    let Ok(bytes) = serde_json::to_vec(request) else {
        return;
    };
    let Ok(path) = socket_path() else {
        return;
    };
    let Ok(socket) = UnixDatagram::unbound() else {
        return;
    };
    if socket.set_nonblocking(true).is_err() {
        return;
    }
    let _ = socket.send_to(&bytes, path);
}

fn print_snapshot(harness: Option<&str>) -> io::Result<()> {
    let json = query_snapshot(harness).unwrap_or_else(|_| empty_snapshot());
    print_json_line(&json);
    Ok(())
}

fn query_snapshot(harness: Option<&str>) -> io::Result<String> {
    let server_path = socket_path()?;
    let client_path = client_socket_path(&server_path, "query");
    let _ = fs::remove_file(&client_path);
    let socket = UnixDatagram::bind(&client_path)?;
    let _guard = SocketGuard {
        path: client_path.clone(),
    };
    fs::set_permissions(&client_path, fs::Permissions::from_mode(0o600))?;
    socket.set_read_timeout(Some(QUERY_TIMEOUT))?;

    let request = Request::Snapshot {
        harness: harness.map(str::to_owned),
    };
    let bytes = serde_json::to_vec(&request).map_err(io::Error::other)?;
    socket.send_to(&bytes, server_path)?;

    let mut buffer = vec![0_u8; MAX_DATAGRAM_SIZE];
    let length = socket.recv(&mut buffer)?;
    String::from_utf8(buffer[..length].to_vec())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn socket_path() -> io::Result<PathBuf> {
    if let Some(path) = env::var_os("HARNESS_STATUS_SOCKET") {
        return Ok(PathBuf::from(path));
    }
    if let Some(runtime_dir) = env::var_os("XDG_RUNTIME_DIR") {
        return Ok(PathBuf::from(runtime_dir).join(DEFAULT_SOCKET_NAME));
    }

    let uid = current_uid().unwrap_or_else(|| "unknown".to_owned());
    Ok(env::temp_dir().join(format!("harness-status-{uid}.sock")))
}

fn client_socket_path(server_path: &Path, purpose: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let name = format!(".harness-status-{purpose}-{}-{nonce}.sock", process::id());
    server_path
        .parent()
        .unwrap_or_else(|| Path::new("/tmp"))
        .join(name)
}

fn current_uid() -> Option<String> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("Uid:"))?;
    line.split_ascii_whitespace().nth(1).map(str::to_owned)
}

fn find_codex_owner() -> Option<Owner> {
    let mut pid = process::id();
    for _ in 0..64 {
        let info = read_process(pid)?;
        if info.comm == "codex" {
            return Some(Owner {
                pid,
                start_ticks: info.start_ticks,
            });
        }
        if info.parent_pid == 0 || info.parent_pid == pid {
            break;
        }
        pid = info.parent_pid;
    }
    None
}

struct ProcessInfo {
    comm: String,
    parent_pid: u32,
    start_ticks: u64,
}

fn read_process(pid: u32) -> Option<ProcessInfo> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let open = stat.find('(')?;
    let close = stat.rfind(')')?;
    let comm = stat.get(open + 1..close)?.to_owned();
    let fields: Vec<&str> = stat.get(close + 2..)?.split_ascii_whitespace().collect();
    Some(ProcessInfo {
        comm,
        parent_pid: fields.get(1)?.parse().ok()?,
        start_ticks: fields.get(19)?.parse().ok()?,
    })
}

fn process_matches_owner(owner: &Owner) -> bool {
    read_process(owner.pid).is_some_and(|info| info.start_ticks == owner.start_ticks)
}

fn display_name(cwd: &str) -> String {
    Path::new(cwd)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(cwd)
        .to_owned()
}

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn unix_time_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn empty_snapshot() -> String {
    "{\"threads\":[]}".to_owned()
}

fn print_json_line(json: &str) {
    println!("{json}");
}

fn missing_listener(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused | io::ErrorKind::WouldBlock
    )
}
