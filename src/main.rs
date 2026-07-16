use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io;
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

#[derive(Debug)]
enum Action {
    Snapshot { harness: Option<String> },
    Follow { harness: Option<String> },
    Hook { harness: String },
    Ack { id: String },
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
    cwd: String,
    #[serde(default)]
    model: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct HookEvent {
    event: String,
    session_id: String,
    turn_id: Option<String>,
    cwd: String,
    model: String,
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

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug)]
struct Entry {
    status: ThreadStatus,
    owner: Option<Owner>,
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

impl Registry {
    fn apply_hook(&mut self, harness: &str, event: HookEvent, owner: Option<Owner>) -> bool {
        let state = match (harness, event.event.as_str()) {
            ("codex", "UserPromptSubmit") => "working",
            ("codex", "Stop") => "completed",
            _ => return false,
        };

        let now = unix_time_seconds();
        let name = display_name(&event.cwd);
        let model = nonempty(event.model);

        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.status.harness == harness && entry.status.id == event.session_id)
        {
            entry.status.state = state.to_owned();
            if !event.cwd.is_empty() {
                entry.status.cwd = event.cwd;
                entry.status.name = name;
            }
            if model.is_some() {
                entry.status.model = model;
            }
            entry.status.turn_id = event.turn_id;
            entry.status.updated_at = now;
            if owner.is_some() {
                entry.owner = owner;
            }
            return true;
        }

        self.entries.push(Entry {
            status: ThreadStatus {
                id: event.session_id,
                harness: harness.to_owned(),
                state: state.to_owned(),
                cwd: event.cwd,
                name,
                model,
                turn_id: event.turn_id,
                updated_at: now,
            },
            owner,
        });
        true
    }

    fn acknowledge(&mut self, id: &str) -> bool {
        let before = self.entries.len();
        self.entries.retain(|entry| entry.status.id != id);
        before != self.entries.len()
    }

    fn reap_dead_workers(&mut self) -> bool {
        let before = self.entries.len();
        self.entries.retain(|entry| {
            entry.status.state != "working"
                || entry.owner.as_ref().is_none_or(process_matches_owner)
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

Print an in-memory AI harness status snapshot as JSON, or stream changed
snapshots as JSONL. With no HARNESS, snapshot and follow modes include all
configured harnesses.

Commands:
  hook HARNESS       Forward a harness hook event from stdin and exit
  ack THREAD_ID      Remove a thread from the current registry

Options:
  -f, --follow       Own or subscribe to the registry and print changed JSON
  -h, --help         Show this help
  -V, --version      Show the version"
    );
}

fn follow(harness: Option<&str>) -> io::Result<()> {
    let path = socket_path()?;
    prepare_socket_parent(&path)?;

    if path.exists() {
        match query_snapshot(None) {
            Ok(_) => return follow_existing(&path, harness),
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

    let socket = match UnixDatagram::bind(&path) {
        Ok(socket) => socket,
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
            return follow_existing(&path, harness);
        }
        Err(error) => return Err(error),
    };
    let _guard = SocketGuard { path: path.clone() };
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    socket.set_read_timeout(Some(REAP_INTERVAL))?;

    let mut registry = Registry::default();
    let mut subscribers = Vec::new();
    let mut last_json = registry.snapshot_json(harness);
    print_json_line(&last_json);

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

        changed |= registry.reap_dead_workers();
        if changed {
            let json = registry.snapshot_json(harness);
            if json != last_json {
                print_json_line(&json);
                last_json = json;
            }
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

fn follow_existing(server_path: &Path, harness: Option<&str>) -> io::Result<()> {
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
    send_subscription(&socket, &bytes, server_path)?;

    let mut last_json = None;
    let mut buffer = vec![0_u8; MAX_DATAGRAM_SIZE];
    loop {
        match socket.recv(&mut buffer) {
            Ok(length) => {
                let json = String::from_utf8(buffer[..length].to_vec())
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                if last_json.as_deref() != Some(json.as_str()) {
                    print_json_line(&json);
                    last_json = Some(json);
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                send_subscription(&socket, &bytes, server_path)?;
            }
            Err(error) => return Err(error),
        }
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
            HookEvent {
                event: input.hook_event_name,
                session_id: input.session_id,
                turn_id: input.turn_id,
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
