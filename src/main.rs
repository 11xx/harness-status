use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::process::{self, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_SOCKET_NAME: &str = "harness-status.sock";
const MAX_DATAGRAM_SIZE: usize = 64 * 1024;
const QUERY_TIMEOUT: Duration = Duration::from_millis(250);
const REAP_INTERVAL: Duration = Duration::from_millis(500);
const CODEX_DISCOVERY_INTERVAL: Duration = Duration::from_secs(2);
const SUBSCRIBER_HEARTBEAT: Duration = Duration::from_secs(2);
const IDLE_AFTER_SECONDS: u64 = 300;
const HIDE_IDLE_AFTER_SECONDS: u64 = 3600;
const CODEX_RECENT_ACTIVITY_SECONDS: u64 = 30;
const STATE_VERSION: u32 = 1;
const CODEX_HOOK_EVENTS: [&str; 2] = ["UserPromptSubmit", "Stop"];
const CODEX_RETIRED_HOOK_EVENTS: [&str; 0] = [];
const CLAUDE_HOOK_EVENTS: [&str; 5] = [
    "UserPromptSubmit",
    "Stop",
    "StopFailure",
    "Notification",
    "SessionEnd",
];
const CLAUDE_RETIRED_HOOK_EVENTS: [&str; 1] = ["SessionStart"];
const NOTIFIER_TIMEOUT: Duration = Duration::from_secs(1);
const HARNESS_STATUS_EXECUTABLE: &str = "harness-status";
const LEGACY_CODEX_HOOK_COMMAND: &str = "harness-status hook codex";
const LEGACY_CLAUDE_HOOK_COMMAND: &str = "harness-status hook claude";
const PI_EXTENSION_FILE: &str = "harness-status.ts";
const PI_EXTENSION_MARKER: &str =
    "// Managed by harness-status. Reinstall instead of editing this file.\n";
const PI_EXTENSION_SOURCE: &str = include_str!("../pi/harness-status.ts");
const USAGE_FRESH_SECONDS: u64 = 5 * 60;
const USAGE_REFRESH_SECONDS: u64 = 3 * 60;
const USAGE_BACKOFF_MAX_SECONDS: u64 = 15 * 60;
const USAGE_STATE_FILE: &str = "usage.json";
const USAGE_LOCK_FILE: &str = "usage.lock";
const FEATURE_STATE_DIR: &str = "harness-status";
const CLAUDE_USAGE_DISABLED_MARKER: &str = "claude-usage.disabled";
const CLAUDE_WINDOW_PING_DISABLED_MARKER: &str = "claude-window-ping.disabled";

#[derive(Clone, Copy, Debug)]
enum FeatureControl {
    Enable,
    Disable,
    Toggle,
}

#[derive(Debug)]
enum Action {
    Snapshot { harness: Option<String> },
    Follow { harness: Option<String> },
    Hook { harness: String },
    Ack { id: String },
    Install { harness: String },
    Uninstall { harness: String },
    Usage,
    UsageControl { control: FeatureControl },
    UsageIngest { harness: String },
    PingControl { control: FeatureControl },
    Guide,
    Help,
    Version,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct UsageWindow {
    used_percentage: u8,
    remaining_percentage: u8,
    resets_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredUsage {
    updated_at: u64,
    #[serde(default)]
    authoritative: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    five_hour: Option<UsageWindow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    seven_day: Option<UsageWindow>,
}

#[derive(Serialize)]
struct UsageWindowView {
    used_percentage: u8,
    remaining_percentage: u8,
    resets_at: u64,
    resets_in_seconds: u64,
    resets_in: String,
}

#[derive(Serialize)]
struct ProviderUsage {
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    updated_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    age_minutes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    five_hour: Option<UsageWindowView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    seven_day: Option<UsageWindowView>,
}

#[derive(Serialize)]
struct UsageReport {
    updated_at: u64,
    state: &'static str,
    codex: ProviderUsage,
    claude: ProviderUsage,
}

#[derive(Default, Deserialize, Serialize)]
struct UsageCache {
    #[serde(default)]
    codex: Option<StoredUsage>,
    #[serde(default)]
    claude: Option<StoredUsage>,
    #[serde(default)]
    refresh: RefreshSchedule,
}

#[derive(Default, Deserialize, Serialize)]
struct RefreshSchedule {
    #[serde(default)]
    codex: ProviderRefresh,
    #[serde(default)]
    claude: ProviderRefresh,
}

#[derive(Default, Deserialize, Serialize)]
struct ProviderRefresh {
    #[serde(default)]
    next_at: u64,
    #[serde(default)]
    failures: u8,
}

enum RefreshError {
    RateLimited(Option<u64>),
    Unavailable,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Owner {
    pid: u32,
    start_ticks: u64,
}

#[derive(Debug, Deserialize)]
struct HarnessHookInput {
    hook_event_name: String,
    session_id: String,
    #[serde(default)]
    turn_id: Option<String>,
    #[serde(default)]
    prompt_id: Option<String>,
    #[serde(default)]
    transcript_path: Option<PathBuf>,
    #[serde(default)]
    cwd: String,
    #[serde(default)]
    model: String,
    #[serde(default)]
    effort: Option<String>,
    #[serde(default)]
    owner_pid: Option<u32>,
    #[serde(default)]
    notification_type: Option<String>,
    #[serde(default)]
    delegated: bool,
    #[serde(default)]
    background_tasks: Vec<Value>,
    #[serde(default)]
    session_crons: Vec<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct HookEvent {
    event: String,
    session_id: String,
    turn_id: Option<String>,
    transcript: Option<TranscriptCursor>,
    cwd: String,
    model: String,
    notification_type: Option<String>,
    #[serde(default)]
    delegated: bool,
    #[serde(default)]
    background: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TranscriptCursor {
    path: PathBuf,
    offset: u64,
}

#[derive(Default, Clone, Deserialize)]
struct TranscriptRecord {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    effort: Option<String>,
    #[serde(rename = "perTurnEffort", default)]
    per_turn_effort: Option<String>,
    #[serde(default)]
    payload: TranscriptPayload,
    #[serde(rename = "promptId", default)]
    prompt_id: Option<String>,
    #[serde(rename = "interruptedMessageId", default)]
    interrupted_message_id: Option<String>,
    #[serde(default)]
    subtype: Option<String>,
    #[serde(rename = "preventedContinuation", default)]
    prevented_continuation: bool,
    #[serde(default)]
    message: TranscriptMessage,
}

#[derive(Default, Clone, Deserialize)]
struct TranscriptMessage {
    #[serde(default)]
    model: Option<String>,
}

#[derive(Default)]
struct TurnReport {
    /// The turn was abandoned outright, so its entry leaves the registry.
    aborted: bool,
    /// The harness ended the turn with an error of its own, so the entry
    /// settles as attention instead of running on.
    failed: bool,
    completed: bool,
    model: Option<String>,
}

#[derive(Default)]
struct CodexRollout {
    cwd: Option<String>,
    model: Option<String>,
    turn_id: Option<String>,
    started_at: Option<u64>,
    active: bool,
}

impl TranscriptRecord {
    /// Claude names the answering model on assistant records, while Codex
    /// repeats its active model on every `turn_context` record, including a
    /// model switch within the same session.
    fn reported_model(&self, harness: &str) -> Option<String> {
        match (harness, self.kind.as_str()) {
            ("claude", "assistant") => self
                .message
                .model
                .as_deref()
                .filter(|model| *model != "<synthetic>")
                .map(|model| {
                    model_slug(
                        model,
                        self.per_turn_effort.as_deref().or(self.effort.as_deref()),
                    )
                }),
            ("codex", "turn_context") => self
                .payload
                .model
                .as_deref()
                .map(|model| model_slug(model, self.payload.effort.as_deref())),
            _ => None,
        }
    }

    /// Codex names the turn it completed. Claude records that its Stop hooks
    /// ran, so the transcript holds the turn's end even when the hook's own
    /// report never arrives; the caller binds it to the turn.
    fn reports_completion(&self, harness: &str, turn_id: &str) -> bool {
        match harness {
            "codex" => {
                self.kind == "event_msg"
                    && self.payload.kind == "task_complete"
                    && self.payload.turn_id.as_deref() == Some(turn_id)
            }
            "claude" => {
                self.kind == "system"
                    && self.subtype.as_deref() == Some("stop_hook_summary")
                    && !self.prevented_continuation
            }
            _ => false,
        }
    }

    /// Each harness marks an abandoned turn in its own transcript vocabulary.
    fn reports_abort(&self, harness: &str, turn_id: &str) -> bool {
        match harness {
            "codex" => {
                self.kind == "event_msg"
                    && self.payload.kind == "turn_aborted"
                    && self.payload.turn_id.as_deref() == Some(turn_id)
            }
            "claude" => {
                self.interrupted_message_id.is_some() && self.prompt_id.as_deref() == Some(turn_id)
            }
            _ => false,
        }
    }

    /// A turn the harness itself could not finish ends without a Stop hook.
    /// Codex closes it with a completed event carrying its usage-limit error,
    /// matched narrowly because unknown error codes may yet be recoverable.
    fn reports_limit_failure(&self, harness: &str, turn_id: &str) -> bool {
        harness == "codex"
            && self.kind == "event_msg"
            && self.payload.kind == "task_complete"
            && self.payload.turn_id.as_deref() == Some(turn_id)
            && self
                .payload
                .error
                .as_ref()
                .and_then(|error| error.get("codex_error_info"))
                .and_then(Value::as_str)
                == Some("usage_limit_exceeded")
    }
}

#[derive(Default, Clone, Deserialize)]
struct TranscriptPayload {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    effort: Option<String>,
    #[serde(default)]
    turn_id: Option<String>,
    /// A turn the harness closed with an error of its own carries the error
    /// beside its completion, not on the record itself.
    #[serde(default)]
    error: Option<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Request {
    Hook {
        harness: String,
        event: Box<HookEvent>,
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
    /// Why the thread wants the user, for states that need a reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    /// The thread stopped answering with its own work still running.
    #[serde(default, skip_serializing_if = "is_false")]
    background: bool,
    /// When the current turn began, so a bar can show how long it has run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    started_at: Option<u64>,
    updated_at: u64,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl ThreadStatus {
    /// A running turn is measured against now, a settled one against the moment
    /// it settled, so a settled thread reports how long its turn ran instead of
    /// a number that keeps climbing while nothing happens.
    fn elapsed_seconds(&self, now: u64) -> u64 {
        let end = if self.state == "working" {
            now
        } else {
            self.updated_at
        };
        end.saturating_sub(self.started_at.unwrap_or(self.updated_at))
    }
}

/// Whole units only: a turn is reported by the time it has actually completed,
/// never rounded up into a minute it has not reached.
fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h{:02}m", seconds / 3600, seconds % 3600 / 60)
    } else {
        format!("{}d{:02}h", seconds / 86_400, seconds % 86_400 / 3600)
    }
}

/// The status a subscriber sees: the thread's own fields, plus the turn
/// duration already formatted so a bar never has to compute a clock of its own.
#[derive(Serialize)]
struct ThreadView<'a> {
    #[serde(flatten)]
    status: &'a ThreadStatus,
    duration: String,
    hidden: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct Entry {
    status: ThreadStatus,
    #[serde(default)]
    idle_since: Option<u64>,
    owner: Option<Owner>,
    transcript: Option<TranscriptCursor>,
    #[serde(default)]
    hookless: bool,
    /// A delegated run reports its own completion, so settling it is not news.
    #[serde(default)]
    delegated: bool,
}

impl Entry {
    fn indicator_hidden(&self, now: u64) -> bool {
        let idle_since = self
            .idle_since
            .unwrap_or_else(|| self.status.updated_at.saturating_add(IDLE_AFTER_SECONDS));
        self.status.state == "idle"
            && !self.status.background
            && now.saturating_sub(idle_since) >= HIDE_IDLE_AFTER_SECONDS
    }
}

#[derive(Debug)]
struct Subscriber {
    path: PathBuf,
    harness: Option<String>,
    last_json: String,
}

#[derive(Serialize)]
struct Snapshot<'a> {
    threads: Vec<ThreadView<'a>>,
}

struct HookResult {
    changed: bool,
    notification: Option<Notification>,
}

struct Notification {
    harness: String,
    project: String,
    state: String,
    reason: Option<String>,
}

struct Notifier {
    command: Option<String>,
}

impl Notifier {
    fn from_env() -> Self {
        let enabled = env::var("HARNESS_STATUS_NOTIFY")
            .map(|value| !matches!(value.to_ascii_lowercase().as_str(), "0" | "false" | "off"))
            .unwrap_or(true);
        let command = enabled.then(|| {
            env::var("HARNESS_STATUS_NOTIFY_COMMAND").unwrap_or_else(|_| "notify-send".to_owned())
        });
        Self { command }
    }

    fn send(&self, notification: Notification) {
        let Some(command) = self.command.as_deref() else {
            return;
        };
        let title = harness_display_name(&notification.harness).to_owned();
        let action = match (notification.state.as_str(), notification.reason.as_deref()) {
            ("attention", Some(reason)) => format!("needs attention: {reason}"),
            ("attention", None) => "needs attention".to_owned(),
            _ => "finished".to_owned(),
        };
        let body = format!("{} {action}", notification.project);
        let app_name = notification_app_name(&notification.harness);
        let child = Command::new(command)
            .args(["--app-name", &app_name, &title, &body])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        if let Ok(mut child) = child {
            thread::spawn(move || {
                let deadline = Instant::now() + NOTIFIER_TIMEOUT;
                loop {
                    match child.try_wait() {
                        Ok(Some(_)) => return,
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        Err(_) => {
                            let _ = child.kill();
                            let _ = child.wait();
                            return;
                        }
                        Ok(None) if Instant::now() < deadline => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Ok(None) => {
                            let _ = child.kill();
                            let _ = child.wait();
                            return;
                        }
                    }
                }
            });
        }
    }
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
    fn apply_hook(&mut self, harness: &str, event: HookEvent, owner: Option<Owner>) -> HookResult {
        let HookEvent {
            event,
            session_id,
            turn_id,
            transcript,
            cwd,
            model,
            notification_type,
            delegated,
            background,
        } = event;
        if matches!(
            (harness, event.as_str()),
            ("pi", "SessionShutdown") | ("claude", "SessionEnd")
        ) {
            let before = self.entries.len();
            self.entries
                .retain(|entry| entry.status.harness != harness || entry.status.id != session_id);
            return HookResult {
                changed: before != self.entries.len(),
                notification: None,
            };
        }

        // A settled thread reports that it is waiting only for the session it
        // already belongs to; it never announces one.
        if harness == "claude" && notification_type.as_deref() == Some("idle_prompt") {
            let settled = self.entries.iter_mut().find(|entry| {
                entry.status.harness == harness
                    && entry.status.id == session_id
                    && entry.status.state == "completed"
            });
            let Some(entry) = settled else {
                return HookResult {
                    changed: false,
                    notification: None,
                };
            };
            entry.status.state = "idle".to_owned();
            entry.idle_since = Some(unix_time_seconds());
            return HookResult {
                changed: true,
                notification: None,
            };
        }

        // The third field separates the two ways a thread reaches `working`: a
        // prompt opens a new turn, while a resume returns to one already under
        // way and must leave its clock alone.
        let (state, reason, begins_turn) = match (harness, event.as_str()) {
            ("codex" | "claude", "UserPromptSubmit") | ("pi", "AgentStart") => {
                ("working", None, true)
            }
            ("pi", "ResumeWorking") => ("working", None, false),
            ("codex" | "claude", "Stop") | ("pi", "AgentSettled" | "ResumeCompleted") => {
                ("completed", None, false)
            }
            ("claude", "StopFailure") => ("attention", Some("failure"), false),
            ("pi", "Attention") => ("attention", None, false),
            ("claude", "Notification") => match notification_type.as_deref() {
                Some("permission_prompt") => ("attention", Some("permission"), false),
                Some("elicitation_dialog") => ("attention", Some("elicitation"), false),
                Some("agent_needs_input") => ("attention", Some("subagent"), false),
                Some("elicitation_complete" | "elicitation_response") => ("working", None, false),
                _ => {
                    return HookResult {
                        changed: false,
                        notification: None,
                    };
                }
            },
            _ => {
                return HookResult {
                    changed: false,
                    notification: None,
                };
            }
        };

        let notify_transition = !matches!((harness, event.as_str()), ("pi", "ResumeCompleted"))
            && (state == "attention" || !delegated);
        let now = unix_time_seconds();
        let name = display_name(&cwd);
        let model = nonempty(model);
        let is_working = state == "working";

        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.status.harness == harness && entry.status.id == session_id)
        {
            let transitioned = entry.status.state != state;
            entry.status.state = state.to_owned();
            entry.idle_since = None;
            if !cwd.is_empty() {
                entry.status.cwd = cwd;
                entry.status.name = name;
            }
            if let Some(model) = model
                && !refines_model(entry.status.model.as_deref(), &model)
            {
                entry.status.model = Some(model);
            }
            // Events that carry no turn identity leave the running turn's
            // alone, so interrupt detection survives an interaction.
            if turn_id.is_some() {
                entry.status.turn_id = turn_id;
            }
            entry.status.reason = reason.map(str::to_owned);
            entry.status.background = background;
            if begins_turn {
                entry.status.started_at = Some(now);
            }
            entry.status.updated_at = now;
            if is_working || owner.is_some() {
                entry.owner = owner;
            }
            if is_working || transcript.is_some() {
                entry.transcript = transcript;
            }
            entry.hookless = false;
            entry.delegated = delegated;
            let notification =
                (transitioned && notify_transition && matches!(state, "completed" | "attention"))
                    .then(|| Notification {
                        harness: harness.to_owned(),
                        project: entry.status.name.clone(),
                        state: state.to_owned(),
                        reason: reason.map(str::to_owned),
                    });
            return HookResult {
                changed: true,
                notification,
            };
        }

        self.entries.push(Entry {
            idle_since: None,
            status: ThreadStatus {
                id: session_id,
                harness: harness.to_owned(),
                state: state.to_owned(),
                cwd,
                name,
                model,
                turn_id,
                reason: reason.map(str::to_owned),
                background,
                started_at: begins_turn.then_some(now),
                updated_at: now,
            },
            owner,
            transcript,
            hookless: false,
            delegated,
        });
        let notification =
            (notify_transition && matches!(state, "completed" | "attention")).then(|| {
                Notification {
                    harness: harness.to_owned(),
                    project: display_name(
                        &self.entries.last().expect("entry was inserted").status.cwd,
                    ),
                    state: state.to_owned(),
                    reason: reason.map(str::to_owned),
                }
            });
        HookResult {
            changed: true,
            notification,
        }
    }

    fn acknowledge(&mut self, id: &str) -> bool {
        let before = self.entries.len();
        self.entries.retain(|entry| entry.status.id != id);
        before != self.entries.len()
    }

    /// A thread that settled long ago is still waiting for the user, but it is
    /// no longer news. Claude reports its own idle threshold; every harness
    /// reaches the same state by sitting still.
    fn settle_quiet_entries(&mut self) -> bool {
        let now = unix_time_seconds();
        let mut changed = false;
        for entry in &mut self.entries {
            if entry.status.state == "completed"
                && now.saturating_sub(entry.status.updated_at) >= IDLE_AFTER_SECONDS
            {
                entry.status.state = "idle".to_owned();
                entry.idle_since = Some(entry.status.updated_at.saturating_add(IDLE_AFTER_SECONDS));
                changed = true;
            }
        }
        changed
    }

    fn reap_dead_owners(&mut self) -> bool {
        let before = self.entries.len();
        self.entries
            .retain(|entry| entry.owner.as_ref().is_none_or(process_matches_owner));
        before != self.entries.len()
    }

    /// Codex writes enough lifecycle evidence to recover a fresh turn when its
    /// command hook was not delivered. A live process in the rollout's recorded
    /// directory keeps this fallback from treating a stale file as active.
    fn discover_codex_sessions(&mut self) -> bool {
        let Some(state_root) = codex_state_root() else {
            return false;
        };
        let owners = codex_process_owners();
        if owners.is_empty() {
            return false;
        }
        let mut files = Vec::new();
        if collect_jsonl_files(&state_root.join("sessions"), &mut files).is_err() {
            return false;
        }

        let now = unix_time_seconds();
        let mut changed = false;
        for path in files {
            let Some(session_id) = codex_session_id(&path) else {
                continue;
            };
            if let Some(entry) = self
                .entries
                .iter_mut()
                .find(|entry| entry.status.harness == "codex" && entry.status.id == session_id)
                && entry.status.state == "working"
            {
                // A hook names its own process; only a recovered entry follows
                // the newest Codex process in its directory.
                if entry.hookless
                    && let Some(owner) = codex_owner_for_cwd(&owners, &entry.status.cwd)
                    && entry.owner.as_ref().is_none_or(|current| {
                        current.pid != owner.pid || current.start_ticks != owner.start_ticks
                    })
                {
                    entry.owner = Some(owner);
                    changed = true;
                }
                continue;
            }
            let Some(modified_at) = modified_seconds(&path) else {
                continue;
            };
            if now.saturating_sub(modified_at) > CODEX_RECENT_ACTIVITY_SECONDS {
                continue;
            }
            let Ok(rollout) = read_codex_rollout(&path) else {
                continue;
            };
            if !rollout.active {
                continue;
            }
            let Some(cwd) = rollout.cwd.as_deref().filter(|cwd| !cwd.is_empty()) else {
                continue;
            };
            if is_internal_codex_cwd(cwd, &state_root) {
                continue;
            }
            let Some(owner) = codex_owner_for_cwd(&owners, cwd) else {
                continue;
            };
            changed |=
                self.reconcile_codex_rollout(&session_id, &path, &rollout, owner, modified_at);
        }
        changed
    }

    fn reconcile_codex_rollout(
        &mut self,
        session_id: &str,
        path: &Path,
        rollout: &CodexRollout,
        owner: Owner,
        modified_at: u64,
    ) -> bool {
        let cwd = rollout.cwd.as_deref().unwrap_or_default();
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.status.harness == "codex" && entry.status.id == session_id)
        {
            let was_working = entry.status.state == "working";
            let turn_changed = rollout
                .turn_id
                .as_deref()
                .is_some_and(|turn_id| entry.status.turn_id.as_deref() != Some(turn_id));
            // A rollout can trail the hook that settled its turn, so only a
            // later turn reopens a settled entry.
            if !was_working && !turn_changed {
                return false;
            }
            let new_turn = !was_working || turn_changed;
            let mut changed = false;

            if !was_working {
                entry.status.state = "working".to_owned();
                entry.status.reason = None;
                entry.status.background = false;
                entry.idle_since = None;
                changed = true;
            }
            if !cwd.is_empty()
                && (entry.status.cwd != cwd || entry.status.name != display_name(cwd))
            {
                entry.status.cwd = cwd.to_owned();
                entry.status.name = display_name(cwd);
                changed = true;
            }
            if let Some(model) = rollout.model.as_deref().filter(|model| !model.is_empty())
                && entry.status.model.as_deref() != Some(model)
            {
                entry.status.model = Some(model.to_owned());
                changed = true;
            }
            if let Some(turn_id) = rollout.turn_id.as_deref()
                && entry.status.turn_id.as_deref() != Some(turn_id)
            {
                entry.status.turn_id = Some(turn_id.to_owned());
                changed = true;
            }
            if new_turn {
                entry.status.started_at = Some(rollout.started_at.unwrap_or(modified_at));
                entry.hookless = true;
                changed = true;
            }
            let updated_at = entry.status.updated_at.max(modified_at);
            if entry.status.updated_at != updated_at {
                entry.status.updated_at = updated_at;
                changed = true;
            }
            if entry.owner.as_ref().is_none_or(|current| {
                current.pid != owner.pid || current.start_ticks != owner.start_ticks
            }) {
                entry.owner = Some(owner);
                changed = true;
            }
            if entry
                .transcript
                .as_ref()
                .is_none_or(|transcript| transcript.path != path)
            {
                entry.transcript = Some(TranscriptCursor {
                    path: path.to_owned(),
                    offset: 0,
                });
                changed = true;
            }
            return changed;
        }

        self.entries.push(Entry {
            idle_since: None,
            status: ThreadStatus {
                id: session_id.to_owned(),
                harness: "codex".to_owned(),
                state: "working".to_owned(),
                cwd: cwd.to_owned(),
                name: display_name(cwd),
                model: rollout.model.clone(),
                turn_id: rollout.turn_id.clone(),
                reason: None,
                background: false,
                started_at: Some(rollout.started_at.unwrap_or(modified_at)),
                updated_at: modified_at,
            },
            owner: Some(owner),
            transcript: Some(TranscriptCursor {
                path: path.to_owned(),
                offset: 0,
            }),
            hookless: true,
            delegated: false,
        });
        true
    }

    /// Restored running turns replay their transcripts so the first snapshot
    /// reflects the current model and any end-of-turn record missed by the
    /// previous owner.
    fn rewind_running_transcripts(&mut self) {
        for entry in &mut self.entries {
            if matches!(entry.status.harness.as_str(), "codex" | "claude")
                && entry.status.state == "working"
                && let Some(transcript) = entry.transcript.as_mut()
            {
                transcript.offset = 0;
            }
        }
    }

    /// Tails each running turn's transcript for the facts a lifecycle hook
    /// never delivers: that the user interrupted the turn, that the harness
    /// ended the turn with an error of its own, and which model answered it.
    /// It also catches an end whose hook never arrived. An interrupt retires
    /// the entry; a failure or a recovered end settles it, with the
    /// notification that transition deserves.
    fn follow_running_transcripts(&mut self) -> (bool, Option<Notification>) {
        let before = self.entries.len();
        let mut changed = false;
        let mut failure = None;
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
            let report = read_transcript_turn(&entry.status.harness, transcript, turn_id)
                .unwrap_or_default();
            if let Some(model) = report.model.filter(|model| {
                entry.status.model.as_deref() != Some(model.as_str()) && !model.is_empty()
            }) {
                entry.status.model = Some(model);
                changed = true;
            }
            if report.aborted {
                return false;
            }
            if report.failed {
                entry.status.state = "attention".to_owned();
                entry.status.reason = Some("failure".to_owned());
                entry.status.updated_at = unix_time_seconds();
                changed = true;
                failure = Some(Notification {
                    harness: entry.status.harness.clone(),
                    project: entry.status.name.clone(),
                    state: "attention".to_owned(),
                    reason: Some("failure".to_owned()),
                });
            }
            // Claude's Stop hook reports the same end, but its report can be
            // lost on the way; the transcript keeps the record regardless.
            if report.completed && (entry.hookless || entry.status.harness == "claude") {
                entry.status.state = "completed".to_owned();
                entry.status.reason = None;
                entry.status.background = false;
                entry.status.updated_at = unix_time_seconds();
                entry.hookless = false;
                changed = true;
                if !entry.delegated {
                    failure = Some(Notification {
                        harness: entry.status.harness.clone(),
                        project: entry.status.name.clone(),
                        state: "completed".to_owned(),
                        reason: None,
                    });
                }
            }
            true
        });
        (changed || before != self.entries.len(), failure)
    }

    fn snapshot_json(&self, harness: Option<&str>) -> String {
        // Indicator order follows the threads themselves, not the order their
        // events happened to arrive, so a dot keeps its place in the bar.
        let mut threads: Vec<&Entry> = self
            .entries
            .iter()
            .filter(|entry| harness.is_none_or(|name| entry.status.harness == name))
            .collect();
        threads.sort_unstable_by(|left, right| {
            let left = &left.status;
            let right = &right.status;
            (&left.harness, &left.name, &left.id).cmp(&(&right.harness, &right.name, &right.id))
        });
        let now = unix_time_seconds();
        let threads = threads
            .into_iter()
            .map(|entry| ThreadView {
                duration: format_duration(entry.status.elapsed_seconds(now)),
                hidden: entry.indicator_hidden(now),
                status: &entry.status,
            })
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
        Action::Usage => print_usage_report(),
        Action::UsageControl { control } => control_feature(
            "Claude usage tracking",
            CLAUDE_USAGE_DISABLED_MARKER,
            control,
        ),
        Action::UsageIngest { harness } => ingest_usage(&harness),
        Action::PingControl { control } => control_feature(
            "Claude window ping",
            CLAUDE_WINDOW_PING_DISABLED_MARKER,
            control,
        ),
        Action::Guide => {
            print_guide();
            Ok(())
        }
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
        "guide" => no_more_args(args, Action::Guide),
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
        "usage" => match args.next().as_deref() {
            None => Ok(Action::Usage),
            Some("ingest") => {
                let harness = args
                    .next()
                    .ok_or_else(|| "'usage ingest' requires a harness name".to_owned())?;
                no_more_args(args, Action::UsageIngest { harness })
            }
            Some("enable") => no_more_args(
                args,
                Action::UsageControl {
                    control: FeatureControl::Enable,
                },
            ),
            Some("disable") => no_more_args(
                args,
                Action::UsageControl {
                    control: FeatureControl::Disable,
                },
            ),
            Some("toggle") => no_more_args(
                args,
                Action::UsageControl {
                    control: FeatureControl::Toggle,
                },
            ),
            Some(other) => Err(format!("unexpected argument '{other}'")),
        },
        "ping" => {
            let control = match args.next().as_deref() {
                Some("enable") => FeatureControl::Enable,
                Some("disable") => FeatureControl::Disable,
                Some("toggle") => FeatureControl::Toggle,
                Some(other) => return Err(format!("unexpected argument '{other}'")),
                None => return Err("'ping' requires enable, disable, or toggle".to_owned()),
            };
            no_more_args(args, Action::PingControl { control })
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

const GUIDE: &str = "\
harness-status guide

Answers one question: which harness sessions are live, and which want you.
It reports session state, not a process listing: `completed` means a turn
settled, and a delegated run's exit status is the launcher's record
(agent-delegate).

Bare invocation prints one snapshot as JSON; a harness name narrows it.
Read `state` on each thread:
  working     a turn is under way
  completed   the turn settled
  idle        settled five minutes ago, or Claude Code reported idle
  attention   the turn wants the user: permission, elicitation, subagent,
              or failure

A thread stays listed until you acknowledge it, its session ends, or its
owning process exits. `harness-status ack THREAD_ID` does the first.

Non-interactive runs (codex exec, pi -p, claude -p) are reported by the same
adapters; pi needs its extension loaded, and Claude Code 2.1.250 sent print
mode an empty payload. An empty snapshot means no report, not death.

To wait for a change instead of polling, run `--follow`: it owns the registry
(or subscribes to the process already owning it) and prints one JSON object
per line as snapshots change.

`harness-status usage` prints normalized Codex and Claude account windows.
The README documents the full JSON contract.";

fn print_guide() {
    println!("{GUIDE}");
}

fn print_help() {
    println!(
        "\
Run 'harness-status guide' to learn what this answers.

Usage:
  harness-status [HARNESS] [--follow]
  harness-status guide
  harness-status --follow [HARNESS]
  harness-status hook HARNESS
  harness-status ack THREAD_ID
  harness-status install HARNESS
  harness-status uninstall HARNESS
  harness-status usage
  harness-status usage ingest claude
  harness-status usage enable|disable|toggle
  harness-status ping enable|disable|toggle

Print an in-memory AI harness status snapshot as JSON, or stream changed
snapshots as JSONL. With no HARNESS, snapshot and follow modes include all
configured harnesses.

Commands:
  guide               Explain the states, --follow, and reading absence
  hook HARNESS        Forward a harness hook event from stdin and exit
  ack THREAD_ID       Remove a thread from the current registry
  install HARNESS     Install the harness integration in its user config
  uninstall HARNESS   Remove the harness integration from its user config
  usage               Print normalized Codex and Claude account windows
  usage ingest claude Store sanitized windows from a Claude status payload
  usage enable|disable|toggle
                      Control Claude usage refresh, ingestion, and display
  ping enable|disable|toggle
                      Control the five-hour Claude window ping helper

Options:
  -f, --follow       Own or subscribe to the registry and print changed JSON
  -h, --help         Show this help
  -V, --version      Show the version"
    );
}

fn install_integration(harness: &str) -> io::Result<()> {
    match harness {
        "codex" => install_hooks(
            "Codex",
            "codex",
            codex_hooks_path()?,
            &CODEX_HOOK_EVENTS,
            &CODEX_RETIRED_HOOK_EVENTS,
            LEGACY_CODEX_HOOK_COMMAND,
        ),
        "claude" => install_hooks(
            "Claude Code",
            "claude",
            claude_settings_path()?,
            &CLAUDE_HOOK_EVENTS,
            &CLAUDE_RETIRED_HOOK_EVENTS,
            LEGACY_CLAUDE_HOOK_COMMAND,
        ),
        "pi" => install_pi_extension(),
        _ => Err(unsupported_harness(harness)),
    }
}

fn uninstall_integration(harness: &str) -> io::Result<()> {
    match harness {
        "codex" => uninstall_hooks(
            "Codex",
            "codex",
            codex_hooks_path()?,
            &CODEX_HOOK_EVENTS,
            &CODEX_RETIRED_HOOK_EVENTS,
            LEGACY_CODEX_HOOK_COMMAND,
        ),
        "claude" => uninstall_hooks(
            "Claude Code",
            "claude",
            claude_settings_path()?,
            &CLAUDE_HOOK_EVENTS,
            &CLAUDE_RETIRED_HOOK_EVENTS,
            LEGACY_CLAUDE_HOOK_COMMAND,
        ),
        "pi" => uninstall_pi_extension(),
        _ => Err(unsupported_harness(harness)),
    }
}

fn unsupported_harness(harness: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("unsupported harness '{harness}'"),
    )
}

fn print_usage_report() -> io::Result<()> {
    let now = unix_time_seconds();
    let claude_enabled = claude_usage_enabled()?;
    let path = usage_state_path()?;
    let mut cache = read_usage_cache(&path)?;
    if active_refresh_enabled()
        && let Some(due) = claim_due_refreshes(&path, claude_enabled, now)?
    {
        let codex = due.codex.then(|| refresh_codex_usage(now));
        let claude = due.claude.then(|| refresh_claude_usage(now));
        let _lock = usage_lock()?;
        cache = read_usage_cache(&path)?;
        match codex {
            Some(Ok(usage)) => {
                cache.codex = Some(usage);
                refresh_succeeded(&mut cache.refresh.codex, now);
            }
            Some(Err(error)) => refresh_failed(&mut cache.refresh.codex, now, error),
            None => {}
        }
        match claude {
            Some(Ok(usage)) => {
                let previous = cache.claude.take();
                cache.claude = Some(merge_usage(usage, previous));
                refresh_succeeded(&mut cache.refresh.claude, now);
            }
            Some(Err(error)) => refresh_failed(&mut cache.refresh.claude, now, error),
            None => {}
        }
        write_usage_cache(&path, &cache)?;
    }
    let codex = provider_usage(newest_codex_usage(cache.codex, now), now);
    let claude = if claude_enabled {
        provider_usage(cache.claude, now)
    } else {
        disabled_provider_usage()
    };
    let state = usage_report_state(codex.state, claude.state);
    let report = UsageReport {
        updated_at: now,
        state,
        codex,
        claude,
    };
    println!(
        "{}",
        serde_json::to_string(&report).map_err(io::Error::other)?
    );
    Ok(())
}

/// A fresh cached value stands on its own. Otherwise the session events are
/// scanned, best effort, and the newer observation wins.
fn newest_codex_usage(cached: Option<StoredUsage>, now: u64) -> Option<StoredUsage> {
    if cached
        .as_ref()
        .is_some_and(|usage| now.saturating_sub(usage.updated_at) <= USAGE_FRESH_SECONDS)
    {
        return cached;
    }
    let session = read_codex_usage().ok().flatten();
    [cached, session]
        .into_iter()
        .flatten()
        .max_by_key(|usage| usage.updated_at)
}

struct DueRefreshes {
    codex: bool,
    claude: bool,
}

/// Claims each provider whose refresh is due by pushing its deadline one
/// interval ahead, so concurrent readers skip it while the claimant's
/// requests run outside the lock. The claimant settles the real deadline
/// once they finish.
fn claim_due_refreshes(
    path: &Path,
    claude_enabled: bool,
    now: u64,
) -> io::Result<Option<DueRefreshes>> {
    let Some(_lock) = try_usage_lock()? else {
        return Ok(None);
    };
    let mut cache = read_usage_cache(path)?;
    let due = DueRefreshes {
        codex: cache.refresh.codex.next_at <= now,
        claude: claude_enabled && cache.refresh.claude.next_at <= now,
    };
    if !due.codex && !due.claude {
        return Ok(None);
    }
    if due.codex {
        cache.refresh.codex.next_at = now + USAGE_REFRESH_SECONDS;
    }
    if due.claude {
        cache.refresh.claude.next_at = now + USAGE_REFRESH_SECONDS;
    }
    write_usage_cache(path, &cache)?;
    Ok(Some(due))
}

fn active_refresh_enabled() -> bool {
    env::var_os("HARNESS_STATUS_USAGE_REFRESH").as_deref() != Some(std::ffi::OsStr::new("0"))
}

fn ingest_usage(harness: &str) -> io::Result<()> {
    if harness != "claude" {
        return Err(unsupported_harness(harness));
    }
    if !claude_usage_enabled()? {
        return Ok(());
    }
    let input: Value = match serde_json::from_reader(io::stdin().lock()) {
        Ok(input) => input,
        Err(_) => return Ok(()),
    };
    let Some(rate_limits) = input.get("rate_limits") else {
        return Ok(());
    };
    let stored = StoredUsage {
        updated_at: unix_time_seconds(),
        authoritative: false,
        five_hour: parse_usage_window(rate_limits.get("five_hour")),
        seven_day: parse_usage_window(
            rate_limits
                .get("seven_day")
                .or_else(|| rate_limits.get("weekly")),
        ),
    };
    if stored.five_hour.is_none() && stored.seven_day.is_none() {
        return Ok(());
    }
    let _lock = usage_lock()?;
    let path = usage_state_path()?;
    let mut cache = read_usage_cache(&path)?;
    cache.claude = Some(merge_usage(stored, cache.claude.take()));
    write_usage_cache(&path, &cache)
}

fn merge_usage(incoming: StoredUsage, previous: Option<StoredUsage>) -> StoredUsage {
    let preserve_authoritative_previous = previous
        .as_ref()
        .is_some_and(|usage| usage.authoritative && !incoming.authoritative);
    StoredUsage {
        updated_at: incoming.updated_at,
        authoritative: incoming.authoritative
            || previous.as_ref().is_some_and(|usage| usage.authoritative),
        five_hour: if preserve_authoritative_previous {
            previous
                .as_ref()
                .and_then(|usage| usage.five_hour.clone())
                .or(incoming.five_hour)
        } else {
            incoming
                .five_hour
                .or_else(|| previous.as_ref().and_then(|usage| usage.five_hour.clone()))
        },
        seven_day: if preserve_authoritative_previous {
            previous
                .as_ref()
                .and_then(|usage| usage.seven_day.clone())
                .or(incoming.seven_day)
        } else {
            incoming
                .seven_day
                .or_else(|| previous.and_then(|usage| usage.seven_day))
        },
    }
}

fn parse_usage_window(value: Option<&Value>) -> Option<UsageWindow> {
    let value = value?;
    let percentage = value.get("used_percentage")?.as_f64()?;
    let resets_at = value.get("resets_at")?.as_u64()?;
    if !(0.0..=100.0).contains(&percentage) || resets_at == 0 {
        return None;
    }
    Some(UsageWindow {
        used_percentage: percentage.round() as u8,
        remaining_percentage: 100 - percentage.round() as u8,
        resets_at,
    })
}

fn disabled_provider_usage() -> ProviderUsage {
    ProviderUsage {
        state: "disabled",
        updated_at: None,
        age_minutes: None,
        five_hour: None,
        seven_day: None,
    }
}

fn usage_report_state(codex: &str, claude: &str) -> &'static str {
    match (codex, claude) {
        (codex, "disabled") => match codex {
            "fresh" => "fresh",
            "unavailable" => "unavailable",
            _ => "stale",
        },
        ("disabled", claude) => match claude {
            "fresh" => "fresh",
            "unavailable" => "unavailable",
            _ => "stale",
        },
        ("fresh", "fresh") => "fresh",
        ("unavailable", "unavailable") => "unavailable",
        _ => "stale",
    }
}

fn provider_usage(stored: Option<StoredUsage>, now: u64) -> ProviderUsage {
    let Some(stored) = stored else {
        return ProviderUsage {
            state: "unavailable",
            updated_at: None,
            age_minutes: None,
            five_hour: None,
            seven_day: None,
        };
    };
    ProviderUsage {
        state: if now.saturating_sub(stored.updated_at) <= USAGE_FRESH_SECONDS {
            "fresh"
        } else {
            "stale"
        },
        updated_at: Some(stored.updated_at),
        age_minutes: Some(now.saturating_sub(stored.updated_at) / 60),
        five_hour: stored
            .five_hour
            .map(|window| usage_window_view(window, now)),
        seven_day: stored
            .seven_day
            .map(|window| usage_window_view(window, now)),
    }
}

fn usage_window_view(window: UsageWindow, now: u64) -> UsageWindowView {
    let resets_in_seconds = window.resets_at.saturating_sub(now);
    UsageWindowView {
        used_percentage: window.used_percentage,
        remaining_percentage: window.remaining_percentage,
        resets_at: window.resets_at,
        resets_in_seconds,
        resets_in: format_reset_in(resets_in_seconds),
    }
}

fn format_reset_in(seconds: u64) -> String {
    if seconds == 0 {
        return "now".to_owned();
    }
    let minutes = seconds.saturating_add(59) / 60;
    if minutes < 60 {
        return format!("{minutes}m");
    }
    let hours = minutes / 60;
    let remaining_minutes = minutes % 60;
    if hours < 24 {
        return format!("{hours}h{remaining_minutes:02}m");
    }
    format!("{}d{}h", hours / 24, hours % 24)
}

fn usage_state_path() -> io::Result<PathBuf> {
    usage_runtime_path(USAGE_STATE_FILE)
}

/// Usage files sit beside the socket. In the shared temporary directory, the
/// fallback when neither a socket override nor `XDG_RUNTIME_DIR` is set, their
/// names carry the uid just as the socket's does.
fn usage_runtime_path(file_name: &str) -> io::Result<PathBuf> {
    let socket = socket_path()?;
    let directory = socket.parent().unwrap_or_else(|| Path::new("/tmp"));
    let shared =
        env::var_os("HARNESS_STATUS_SOCKET").is_none() && env::var_os("XDG_RUNTIME_DIR").is_none();
    Ok(if shared {
        let uid = current_uid().unwrap_or_else(|| "unknown".to_owned());
        directory.join(format!("harness-status-{uid}-{file_name}"))
    } else {
        directory.join(format!("harness-status-{file_name}"))
    })
}

fn feature_marker_path(file_name: &str) -> io::Result<PathBuf> {
    Ok(xdg_state_home()?.join(FEATURE_STATE_DIR).join(file_name))
}

fn xdg_state_home() -> io::Result<PathBuf> {
    if let Some(path) = env::var_os("XDG_STATE_HOME").filter(|path| !path.is_empty()) {
        return Ok(PathBuf::from(path));
    }
    env::var_os("HOME")
        .filter(|path| !path.is_empty())
        .map(|path| PathBuf::from(path).join(".local/state"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "HOME is not set"))
}

fn feature_disabled(file_name: &str) -> io::Result<bool> {
    match fs::symlink_metadata(feature_marker_path(file_name)?) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn claude_usage_enabled() -> io::Result<bool> {
    Ok(!feature_disabled(CLAUDE_USAGE_DISABLED_MARKER)?)
}

fn control_feature(label: &str, marker_name: &str, control: FeatureControl) -> io::Result<()> {
    let marker = feature_marker_path(marker_name)?;
    let disabled = match control {
        FeatureControl::Enable => false,
        FeatureControl::Disable => true,
        FeatureControl::Toggle => !feature_disabled(marker_name)?,
    };
    if disabled {
        write_bytes_atomic(&marker, b"disabled\n", ".harness-status-feature")?;
        println!("{label} disabled.");
    } else {
        match fs::remove_file(&marker) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        println!("{label} enabled.");
    }
    Ok(())
}

fn usage_lock_path() -> io::Result<PathBuf> {
    usage_runtime_path(USAGE_LOCK_FILE)
}

/// The usage lock guards only reads and writes of the cache, never a
/// provider request, so waiting for it is brief.
fn usage_lock() -> io::Result<fs::File> {
    let file = open_usage_lock()?;
    file.lock()?;
    Ok(file)
}

fn try_usage_lock() -> io::Result<Option<fs::File>> {
    let file = open_usage_lock()?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(fs::TryLockError::WouldBlock) => Ok(None),
        Err(fs::TryLockError::Error(error)) => Err(error),
    }
}

fn open_usage_lock() -> io::Result<fs::File> {
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(usage_lock_path()?)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

fn read_usage_cache(path: &Path) -> io::Result<UsageCache> {
    match fs::read(path) {
        Ok(contents) => {
            if let Ok(cache) = serde_json::from_slice(&contents) {
                return Ok(cache);
            }
            let claude = serde_json::from_slice(&contents).ok();
            Ok(UsageCache {
                claude,
                ..UsageCache::default()
            })
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(UsageCache::default()),
        Err(error) => Err(error),
    }
}

fn write_usage_cache(path: &Path, cache: &UsageCache) -> io::Result<()> {
    let mut contents = serde_json::to_vec(cache).map_err(io::Error::other)?;
    contents.push(b'\n');
    write_bytes_atomic(path, &contents, ".harness-status-usage")
}

fn refresh_succeeded(refresh: &mut ProviderRefresh, now: u64) {
    refresh.next_at = now + USAGE_REFRESH_SECONDS;
    refresh.failures = 0;
}

fn refresh_failed(refresh: &mut ProviderRefresh, now: u64, error: RefreshError) {
    refresh.failures = refresh.failures.saturating_add(1);
    let exponential = USAGE_REFRESH_SECONDS
        .saturating_mul(1_u64 << u32::from(refresh.failures.saturating_sub(1).min(8)))
        .min(USAGE_BACKOFF_MAX_SECONDS);
    refresh.next_at = now
        + match error {
            RefreshError::RateLimited(Some(retry_after)) => retry_after.max(exponential),
            RefreshError::RateLimited(None) => exponential,
            RefreshError::Unavailable => USAGE_REFRESH_SECONDS,
        };
}

fn refresh_codex_usage(now: u64) -> Result<StoredUsage, RefreshError> {
    let mut child = Command::new("codex")
        .args(["app-server", "--stdio"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| RefreshError::Unavailable)?;
    let mut stdin = child.stdin.take().ok_or(RefreshError::Unavailable)?;
    writeln!(
        stdin,
        "{}",
        json!({
            "method": "initialize",
            "id": 1,
            "params": {
                "clientInfo": {
                    "name": "harness-status",
                    "title": "harness-status",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": {}
            }
        })
    )
    .map_err(|_| RefreshError::Unavailable)?;
    writeln!(stdin, "{}", json!({"method": "initialized", "params": {}}))
        .map_err(|_| RefreshError::Unavailable)?;
    writeln!(
        stdin,
        "{}",
        json!({"method": "account/rateLimits/read", "id": 2})
    )
    .map_err(|_| RefreshError::Unavailable)?;
    stdin.flush().map_err(|_| RefreshError::Unavailable)?;

    let stdout = child.stdout.take().ok_or(RefreshError::Unavailable)?;
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let Ok(value) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if value.get("id").and_then(Value::as_u64) == Some(2) {
                let _ = sender.send(value);
                return;
            }
        }
    });
    let response = receiver.recv_timeout(Duration::from_secs(5));
    let _ = child.kill();
    let _ = child.wait();
    let response = response.map_err(|_| RefreshError::Unavailable)?;
    let limits = response
        .pointer("/result/rateLimits")
        .ok_or(RefreshError::Unavailable)?;
    let (five_hour, seven_day) = codex_windows(limits, "windowDurationMins", parse_active_window);
    if five_hour.is_none() && seven_day.is_none() {
        return Err(RefreshError::Unavailable);
    }
    Ok(StoredUsage {
        updated_at: now,
        authoritative: true,
        five_hour,
        seven_day,
    })
}

// Older payloads omit duration; their slots retain the original 5h/7d meaning.
fn codex_windows(
    limits: &Value,
    duration_key: &str,
    parse: fn(Option<&Value>) -> Option<UsageWindow>,
) -> (Option<UsageWindow>, Option<UsageWindow>) {
    let mut five_hour = None;
    let mut seven_day = None;
    for (slot, default_minutes) in [("primary", 300), ("secondary", 10080)] {
        let Some(value) = limits.get(slot).filter(|value| !value.is_null()) else {
            continue;
        };
        let minutes = match value.get(duration_key) {
            None | Some(Value::Null) => Some(default_minutes),
            Some(duration) => duration.as_u64(),
        };
        match minutes {
            Some(300) => five_hour = parse(Some(value)),
            Some(10080) => seven_day = parse(Some(value)),
            _ => {}
        }
    }
    (five_hour, seven_day)
}

fn parse_active_window(value: Option<&Value>) -> Option<UsageWindow> {
    let value = value?;
    usage_window(
        value.get("usedPercent")?.as_f64()?,
        value.get("resetsAt")?.as_u64()?,
    )
}

fn refresh_claude_usage(now: u64) -> Result<StoredUsage, RefreshError> {
    let credentials_path = claude_config_dir()
        .map_err(|_| RefreshError::Unavailable)?
        .join(".credentials.json");
    let credentials: Value =
        serde_json::from_slice(&fs::read(credentials_path).map_err(|_| RefreshError::Unavailable)?)
            .map_err(|_| RefreshError::Unavailable)?;
    let oauth = credentials
        .get("claudeAiOauth")
        .ok_or(RefreshError::Unavailable)?;
    let token = oauth
        .get("accessToken")
        .and_then(Value::as_str)
        .filter(|token| {
            !token.is_empty()
                && token
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        })
        .ok_or(RefreshError::Unavailable)?;
    if oauth
        .get("expiresAt")
        .and_then(Value::as_u64)
        .is_some_and(|expires_at_ms| expires_at_ms / 1000 <= now)
    {
        return Err(RefreshError::Unavailable);
    }

    let mut child = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--max-time",
            "10",
            "--config",
            "-",
            "--write-out",
            "\n__HARNESS_STATUS_HTTP__%{http_code}|%header{retry-after}",
            "https://api.anthropic.com/api/oauth/usage",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| RefreshError::Unavailable)?;
    let mut stdin = child.stdin.take().ok_or(RefreshError::Unavailable)?;
    writeln!(stdin, "header = \"Authorization: Bearer {token}\"")
        .map_err(|_| RefreshError::Unavailable)?;
    writeln!(stdin, "header = \"anthropic-beta: oauth-2025-04-20\"")
        .map_err(|_| RefreshError::Unavailable)?;
    writeln!(
        stdin,
        "header = \"User-Agent: harness-status/{}\"",
        env!("CARGO_PKG_VERSION")
    )
    .map_err(|_| RefreshError::Unavailable)?;
    drop(stdin);
    let output = child
        .wait_with_output()
        .map_err(|_| RefreshError::Unavailable)?;
    if !output.status.success() {
        return Err(RefreshError::Unavailable);
    }
    let output = String::from_utf8(output.stdout).map_err(|_| RefreshError::Unavailable)?;
    let (body, metadata) = output
        .rsplit_once("\n__HARNESS_STATUS_HTTP__")
        .ok_or(RefreshError::Unavailable)?;
    let (status, retry_after) = metadata.split_once('|').unwrap_or((metadata, ""));
    if status == "429" {
        return Err(RefreshError::RateLimited(parse_retry_after(
            retry_after,
            now,
        )));
    }
    if status != "200" {
        return Err(RefreshError::Unavailable);
    }
    let response: Value = serde_json::from_str(body).map_err(|_| RefreshError::Unavailable)?;
    let five_hour = parse_claude_api_window(response.get("five_hour"));
    let seven_day = parse_claude_api_window(response.get("seven_day"));
    if five_hour.is_none() && seven_day.is_none() {
        return Err(RefreshError::Unavailable);
    }
    Ok(StoredUsage {
        updated_at: now,
        authoritative: true,
        five_hour,
        seven_day,
    })
}

fn claude_config_dir() -> io::Result<PathBuf> {
    if let Some(path) = env::var_os("CLAUDE_CONFIG_DIR").filter(|path| !path.is_empty()) {
        return Ok(PathBuf::from(path));
    }
    env::var_os("HOME")
        .filter(|path| !path.is_empty())
        .map(|path| PathBuf::from(path).join(".claude"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "HOME is not set"))
}

fn parse_claude_api_window(value: Option<&Value>) -> Option<UsageWindow> {
    let value = value?;
    let resets_at = value.get("resets_at")?.as_str()?;
    let resets_at = resets_at.parse::<jiff::Timestamp>().ok()?.as_second() as u64;
    usage_window(value.get("utilization")?.as_f64()?, resets_at)
}

/// `Retry-After` is either delay-seconds or an HTTP-date (RFC 9110).
fn parse_retry_after(value: &str, now: u64) -> Option<u64> {
    let value = value.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(seconds);
    }
    let timestamp = jiff::fmt::rfc2822::DateTimeParser::new()
        .parse_timestamp(value)
        .ok()?
        .as_second();
    Some(u64::try_from(timestamp).ok()?.saturating_sub(now))
}

fn usage_window(percentage: f64, resets_at: u64) -> Option<UsageWindow> {
    if !(0.0..=100.0).contains(&percentage) || resets_at == 0 {
        return None;
    }
    let used_percentage = percentage.round() as u8;
    Some(UsageWindow {
        used_percentage,
        remaining_percentage: 100 - used_percentage,
        resets_at,
    })
}

fn read_codex_usage() -> io::Result<Option<StoredUsage>> {
    let root = codex_home()?.join("sessions");
    let mut files = Vec::new();
    collect_jsonl_files(&root, &mut files)?;
    files.sort_unstable_by_key(|path| {
        fs::metadata(path)
            .and_then(|metadata| metadata.modified())
            .unwrap_or(UNIX_EPOCH)
    });
    for path in files.into_iter().rev() {
        if let Ok(Some(usage)) = read_codex_session_usage(&path) {
            return Ok(Some(usage));
        }
    }
    Ok(None)
}

fn collect_jsonl_files(path: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_jsonl_files(&path, files)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "jsonl")
        {
            files.push(path);
        }
    }
    Ok(())
}

fn read_codex_rollout(path: &Path) -> io::Result<CodexRollout> {
    let file = fs::File::open(path)?;
    let mut rollout = CodexRollout::default();
    for line in BufReader::new(file).lines() {
        let Ok(line) = line else { continue };
        let Ok(record) = serde_json::from_str::<TranscriptRecord>(&line) else {
            continue;
        };
        if let Some(cwd) = record.payload.cwd.as_deref().filter(|cwd| !cwd.is_empty()) {
            rollout.cwd = Some(cwd.to_owned());
        }
        if let Some(model) = record
            .reported_model("codex")
            .filter(|model| !model.is_empty())
        {
            rollout.model = Some(model);
        }
        if record.kind != "event_msg" {
            continue;
        }
        match record.payload.kind.as_str() {
            "task_started" => {
                rollout.active = true;
                rollout.turn_id = record.payload.turn_id.clone();
                rollout.started_at = record_timestamp(&record);
            }
            "task_complete" | "turn_aborted" => rollout.active = false,
            _ => {}
        }
    }
    Ok(rollout)
}

fn record_timestamp(record: &TranscriptRecord) -> Option<u64> {
    record
        .timestamp
        .as_deref()?
        .parse::<jiff::Timestamp>()
        .ok()?
        .as_second()
        .try_into()
        .ok()
}

fn modified_seconds(path: &Path) -> Option<u64> {
    fs::metadata(path)
        .ok()?
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn codex_session_id(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_str()?;
    stem.get(stem.len().checked_sub(36)?..).map(str::to_owned)
}

fn codex_process_owners() -> Vec<(PathBuf, Owner)> {
    let Some(uid) = current_uid().and_then(|uid| uid.parse::<u32>().ok()) else {
        return Vec::new();
    };
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    let mut processes: Vec<(PathBuf, Owner)> = Vec::new();
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if !fs::metadata(entry.path()).is_ok_and(|metadata| metadata.uid() == uid) {
            continue;
        }
        let Some(info) = read_process(pid) else {
            continue;
        };
        if info.comm != "codex" || is_codex_app_server(pid) {
            continue;
        }
        let Ok(cwd) = fs::read_link(format!("/proc/{pid}/cwd")) else {
            continue;
        };
        let owner = Owner {
            pid,
            start_ticks: info.start_ticks,
        };
        if let Some((_, current)) = processes
            .iter_mut()
            .find(|(path, current)| path == &cwd && current.start_ticks == owner.start_ticks)
        {
            if owner.pid < current.pid {
                *current = owner;
            }
        } else {
            processes.push((cwd, owner));
        }
    }
    processes
}

fn codex_owner_for_cwd(owners: &[(PathBuf, Owner)], cwd: &str) -> Option<Owner> {
    owners
        .iter()
        .filter(|(process_cwd, _)| process_cwd == Path::new(cwd))
        .max_by_key(|(_, owner)| owner.start_ticks)
        .map(|(_, owner)| owner.clone())
}

fn is_codex_app_server(pid: u32) -> bool {
    fs::read(format!("/proc/{pid}/cmdline"))
        .ok()
        .is_some_and(|cmdline| {
            cmdline
                .split(|byte| *byte == 0)
                .any(|argument| argument == b"app-server")
        })
}

fn read_codex_session_usage(path: &Path) -> io::Result<Option<StoredUsage>> {
    let file = fs::File::open(path)?;
    let mut found = None;
    for line in BufReader::new(file).lines() {
        let Ok(line) = line else { continue };
        let Ok(record) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(rate_limits) = record
            .get("payload")
            .and_then(|payload| payload.get("rate_limits"))
        else {
            continue;
        };
        let (five_hour, seven_day) =
            codex_windows(rate_limits, "window_minutes", parse_codex_window);
        if five_hour.is_some() || seven_day.is_some() {
            // The observation is as old as its own record; the file's mtime
            // stands in only for a record that carries no timestamp.
            let observed_at = record
                .get("timestamp")
                .and_then(Value::as_str)
                .and_then(|timestamp| timestamp.parse::<jiff::Timestamp>().ok())
                .and_then(|timestamp| u64::try_from(timestamp.as_second()).ok())
                .or_else(|| modified_seconds(path))
                .unwrap_or_else(unix_time_seconds);
            found = Some(StoredUsage {
                updated_at: observed_at,
                authoritative: true,
                five_hour,
                seven_day,
            });
        }
    }
    Ok(found)
}

fn parse_codex_window(value: Option<&Value>) -> Option<UsageWindow> {
    let value = value?;
    let percentage = value.get("used_percent")?.as_f64()?;
    let resets_at = value.get("resets_at")?.as_u64()?;
    if !(0.0..=100.0).contains(&percentage) || resets_at == 0 {
        return None;
    }
    Some(UsageWindow {
        used_percentage: percentage.round() as u8,
        remaining_percentage: 100 - percentage.round() as u8,
        resets_at,
    })
}

fn codex_home() -> io::Result<PathBuf> {
    if let Some(path) = env::var_os("CODEX_HOME").filter(|path| !path.is_empty()) {
        return Ok(PathBuf::from(path));
    }
    env::var_os("HOME")
        .filter(|path| !path.is_empty())
        .map(|path| PathBuf::from(path).join(".codex"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "HOME is not set"))
}

fn install_hooks(
    display_name: &str,
    harness: &str,
    display_path: PathBuf,
    events: &[&str],
    retired_events: &[&str],
    legacy_command: &str,
) -> io::Result<()> {
    let path = writable_config_path(&display_path)?;
    let mut document = read_hook_document(&path, true)?;
    let command = hook_command(harness)?;
    let target_handler = json!({
        "type": "command",
        "command": command,
        "timeout": 5
    });

    let root = root_object_mut(&mut document, &display_path)?;
    let hooks = hooks_object_mut(root, &display_path)?;
    let mut changed = false;
    for &event in events {
        changed |= install_event_handler(hooks, event, &target_handler, harness, legacy_command)?;
    }
    changed |= remove_managed_events(
        hooks,
        retired_events,
        harness,
        legacy_command,
        &display_path,
    )?;

    if changed {
        write_json_atomic(&path, &document)?;
        println!(
            "Installed {display_name} hooks in {}.",
            display_path.display()
        );
        if harness == "codex" {
            println!("Restart Codex, then open /hooks to review and trust them.");
        } else {
            println!("Open /hooks in {display_name} to verify them.");
        }
    } else {
        println!(
            "{display_name} hooks are already installed in {}.",
            display_path.display()
        );
    }
    Ok(())
}

fn uninstall_hooks(
    display_name: &str,
    harness: &str,
    display_path: PathBuf,
    events: &[&str],
    retired_events: &[&str],
    legacy_command: &str,
) -> io::Result<()> {
    let path = writable_config_path(&display_path)?;
    if !path.exists() {
        println!(
            "{display_name} hooks are already uninstalled from {}.",
            display_path.display()
        );
        return Ok(());
    }

    let mut document = read_hook_document(&path, false)?;
    let root = root_object_mut(&mut document, &display_path)?;
    let mut changed = false;
    let mut hooks_are_empty = false;

    if let Some(hooks_value) = root.get_mut("hooks") {
        let hooks = hooks_value.as_object_mut().ok_or_else(|| {
            invalid_hook_config(&display_path, "top-level 'hooks' must be an object")
        })?;
        for list in [events, retired_events] {
            changed |= remove_managed_events(hooks, list, harness, legacy_command, &display_path)?;
        }
        // Ours to drop only when our own removals emptied it. An object that
        // never held anything of ours is not ours to rewrite, and only a write
        // could carry the removal anywhere.
        hooks_are_empty = changed && hooks.is_empty();
    }

    if hooks_are_empty {
        root.remove("hooks");
    }

    if changed {
        write_json_atomic(&path, &document)?;
        println!(
            "Uninstalled {display_name} hooks from {}.",
            display_path.display()
        );
        if harness == "codex" {
            println!("Restart Codex to stop using the removed definitions.");
        }
    } else {
        println!(
            "{display_name} hooks are already uninstalled from {}.",
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

fn claude_settings_path() -> io::Result<PathBuf> {
    if let Some(config_dir) = env::var_os("CLAUDE_CONFIG_DIR").filter(|path| !path.is_empty()) {
        return Ok(PathBuf::from(config_dir).join("settings.json"));
    }
    let home = env::var_os("HOME")
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "neither CLAUDE_CONFIG_DIR nor HOME is set",
            )
        })?;
    Ok(PathBuf::from(home).join(".claude/settings.json"))
}

fn pi_agent_dir() -> io::Result<PathBuf> {
    if let Some(agent_dir) = env::var_os("PI_CODING_AGENT_DIR").filter(|path| !path.is_empty()) {
        return Ok(PathBuf::from(agent_dir));
    }
    let home = env::var_os("HOME")
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "neither PI_CODING_AGENT_DIR nor HOME is set",
            )
        })?;
    Ok(PathBuf::from(home).join(".pi/agent"))
}

fn pi_extension_path() -> io::Result<PathBuf> {
    Ok(pi_agent_dir()?.join("extensions").join(PI_EXTENSION_FILE))
}

fn installed_pi_extension() -> io::Result<Vec<u8>> {
    let executable = path_executable()?;
    let executable = executable.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "harness-status executable path is not valid UTF-8",
        )
    })?;
    let executable = serde_json::to_string(executable).map_err(io::Error::other)?;
    let source = PI_EXTENSION_SOURCE.replacen(
        "const HARNESS_STATUS = \"harness-status\";",
        &format!("const HARNESS_STATUS = {executable};"),
        1,
    );
    Ok(format!("{PI_EXTENSION_MARKER}{source}").into_bytes())
}

fn install_pi_extension() -> io::Result<()> {
    let path = pi_extension_path()?;
    let target = installed_pi_extension()?;
    match fs::read(&path) {
        Ok(current) if current == target => {
            println!("pi extension is already installed in {}.", path.display());
            return Ok(());
        }
        Ok(current) if !current.starts_with(PI_EXTENSION_MARKER.as_bytes()) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "cannot replace unmanaged pi extension {}; move it first",
                    path.display()
                ),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    write_bytes_atomic(&path, &target, ".harness-status-pi-extension")?;
    println!("Installed pi extension in {}.", path.display());
    println!("Run /reload in pi sessions that are already open.");
    Ok(())
}

fn uninstall_pi_extension() -> io::Result<()> {
    let path = pi_extension_path()?;
    match fs::read(&path) {
        Ok(current) if current.starts_with(PI_EXTENSION_MARKER.as_bytes()) => {
            fs::remove_file(&path)?;
            println!("Uninstalled pi extension from {}.", path.display());
            println!("Run /reload in pi sessions that are already open.");
        }
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "refusing to remove unmanaged pi extension {}",
                    path.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            println!(
                "pi extension is already uninstalled from {}.",
                path.display()
            );
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

fn writable_config_path(path: &Path) -> io::Result<PathBuf> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => fs::canonicalize(path),
        Ok(_) => Ok(path.to_owned()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(path.to_owned()),
        Err(error) => Err(error),
    }
}

fn hook_command(harness: &str) -> io::Result<String> {
    let executable = path_executable()?;
    let executable = executable.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "harness-status executable path is not valid UTF-8",
        )
    })?;
    Ok(format!("{} hook {harness}", shell_quote(executable)))
}

fn path_executable() -> io::Result<PathBuf> {
    let path = env::var_os("PATH").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "cannot find 'harness-status': PATH is not set",
        )
    })?;
    let current_dir = env::current_dir()?;
    for directory in env::split_paths(&path) {
        let candidate = if directory.as_os_str().is_empty() {
            current_dir.join(HARNESS_STATUS_EXECUTABLE)
        } else if directory.is_absolute() {
            directory.join(HARNESS_STATUS_EXECUTABLE)
        } else {
            current_dir.join(directory).join(HARNESS_STATUS_EXECUTABLE)
        };
        if fs::metadata(&candidate)
            .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        {
            return Ok(candidate);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "cannot find executable 'harness-status' on PATH",
    ))
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
    harness: &str,
    legacy_command: &str,
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
        .filter(|handler| handler_is_owned(handler, harness, legacy_command))
        .collect();
    if owned.len() == 1 && owned[0] == target_handler {
        return Ok(false);
    }

    remove_owned_handlers(groups, event, harness, legacy_command)?;
    groups.push(json!({ "hooks": [target_handler.clone()] }));
    Ok(true)
}

fn remove_managed_events(
    hooks: &mut Map<String, Value>,
    events: &[&str],
    harness: &str,
    legacy_command: &str,
    display_path: &Path,
) -> io::Result<bool> {
    let mut changed = false;
    let mut empty_events = Vec::new();
    for &event in events {
        let Some(groups_value) = hooks.get_mut(event) else {
            continue;
        };
        let groups = groups_value.as_array_mut().ok_or_else(|| {
            invalid_hook_config(display_path, &format!("hooks.{event} must be an array"))
        })?;
        changed |= remove_owned_handlers(groups, event, harness, legacy_command)? > 0;
        if groups.is_empty() {
            empty_events.push(event);
        }
    }
    for event in empty_events {
        hooks.remove(event);
    }
    Ok(changed)
}

fn remove_owned_handlers(
    groups: &mut Vec<Value>,
    event: &str,
    harness: &str,
    legacy_command: &str,
) -> io::Result<usize> {
    validate_event_groups(groups, event)?;
    let mut removed = 0;
    for group in groups.iter_mut() {
        let handlers = group["hooks"].as_array_mut().expect("validated hook group");
        handlers.retain(|handler| {
            if handler_is_owned(handler, harness, legacy_command) {
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

/// Ownership is read from the handler alone, so a hook stays recognizable
/// after the executable it names has moved or been removed.
fn handler_is_owned(handler: &Value, harness: &str, legacy_command: &str) -> bool {
    handler.get("type").and_then(Value::as_str) == Some("command")
        && handler
            .get("command")
            .and_then(Value::as_str)
            .is_some_and(|candidate| {
                candidate == legacy_command || generated_hook_command(candidate, harness)
            })
}

fn generated_hook_command(candidate: &str, harness: &str) -> bool {
    let suffix = format!("' hook {harness}");
    let Some(executable) = candidate
        .strip_suffix(&suffix)
        .and_then(|command| command.strip_prefix('\''))
    else {
        return false;
    };
    Path::new(executable)
        .file_name()
        .and_then(|name| name.to_str())
        == Some(HARNESS_STATUS_EXECUTABLE)
}

fn invalid_hook_config(path: &Path, detail: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("cannot update {}: {detail}", path.display()),
    )
}

fn write_json_atomic(path: &Path, document: &Value) -> io::Result<()> {
    let mut contents = serde_json::to_vec_pretty(document).map_err(io::Error::other)?;
    contents.push(b'\n');
    write_bytes_atomic(path, &contents, ".hooks.json.harness-status")
}

fn write_bytes_atomic(path: &Path, contents: &[u8], prefix: &str) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "configuration path has no parent directory",
        )
    })?;
    fs::create_dir_all(parent)?;

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = parent.join(format!("{prefix}-{}-{nonce}", process::id()));
    let permissions = fs::metadata(path)
        .map(|metadata| metadata.permissions().mode() & 0o777)
        .unwrap_or(0o600);

    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(permissions))?;
        file.write_all(contents)?;
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
                    retire_dead_owner_socket(&path)?;
                }
                // A busy owner answers late; keep subscribing instead of exiting.
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                    ) =>
                {
                    follow_existing(&path, harness, &mut last_json)?;
                    continue;
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
    cleanup_stale_client_sockets(path)?;

    let state_path = registry_state_path(path);
    let notifier = Notifier::from_env();
    let mut registry = load_registry(&state_path);
    registry.rewind_running_transcripts();
    registry.discover_codex_sessions();
    registry.follow_running_transcripts();
    registry.reap_dead_owners();
    checkpoint_registry(&state_path, &registry);
    let mut subscribers = Vec::new();
    let mut next_codex_discovery = Instant::now() + CODEX_DISCOVERY_INTERVAL;
    let initial_json = registry.snapshot_json(harness);
    print_json_if_changed(last_json, initial_json);

    let mut buffer = vec![0_u8; MAX_DATAGRAM_SIZE];
    loop {
        let mut changed = false;
        let mut notification = None;
        match socket.recv_from(&mut buffer) {
            Ok((length, sender)) => {
                if let Ok(request) = serde_json::from_slice::<Request>(&buffer[..length]) {
                    match request {
                        Request::Hook {
                            harness: source,
                            event,
                            owner,
                        } => {
                            let result = registry.apply_hook(&source, *event, owner);
                            changed = result.changed;
                            notification = result.notification;
                        }
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

        if Instant::now() >= next_codex_discovery {
            changed |= registry.discover_codex_sessions();
            next_codex_discovery = Instant::now() + CODEX_DISCOVERY_INTERVAL;
        }
        let (transcripts_changed, transcript_failure) = registry.follow_running_transcripts();
        changed |= transcripts_changed;
        if notification.is_none() {
            notification = transcript_failure;
        }
        changed |= registry.settle_quiet_entries();
        changed |= registry.reap_dead_owners();
        if changed {
            checkpoint_registry(&state_path, &registry);
        }
        // A running turn's duration advances without any event to announce it,
        // so every wake-up republishes. Both paths compare against what they
        // last sent, which keeps a still registry silent.
        let json = registry.snapshot_json(harness);
        print_json_if_changed(last_json, json);
        publish_subscribers(&socket, &registry, &mut subscribers);
        if let Some(notification) = notification {
            notifier.send(notification);
        }
    }
}

/// Concurrent followers race to retire the same dead owner's socket. Losing
/// that race is how the winner is elected, so an already-removed path succeeds.
fn retire_dead_owner_socket(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
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
    cleanup_stale_client_sockets(server_path)?;
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
    let input = match harness {
        "codex" | "claude" | "pi" => {
            serde_json::from_reader::<_, HarnessHookInput>(io::stdin().lock())
        }
        _ => return,
    };
    let Ok(input) = input else {
        return;
    };
    if harness == "codex"
        && codex_state_root().is_some_and(|root| is_internal_codex_cwd(&input.cwd, &root))
    {
        return;
    }
    let transcript = matches!(harness, "codex" | "claude")
        .then_some(input.transcript_path)
        .flatten()
        .map(|path| TranscriptCursor {
            offset: fs::metadata(&path)
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            path,
        });
    let owner = input
        .owner_pid
        .and_then(owner_for_pid)
        .or_else(|| find_harness_owner(harness));
    let event = HookEvent {
        event: input.hook_event_name,
        session_id: input.session_id,
        turn_id: input.turn_id.or(input.prompt_id),
        transcript,
        cwd: input.cwd,
        model: model_slug(&input.model, input.effort.as_deref()),
        notification_type: input.notification_type,
        delegated: input.delegated || delegated_process(),
        background: !input.background_tasks.is_empty() || !input.session_crons.is_empty(),
    };

    let request = Request::Hook {
        harness: harness.to_owned(),
        event: Box::new(event),
        owner,
    };
    send_fire_and_forget(&request);
}

fn read_transcript_turn(
    harness: &str,
    cursor: &mut TranscriptCursor,
    turn_id: &str,
) -> io::Result<TurnReport> {
    let mut file = fs::File::open(&cursor.path)?;
    if file.metadata()?.len() < cursor.offset {
        cursor.offset = 0;
    }
    file.seek(SeekFrom::Start(cursor.offset))?;
    let mut reader = BufReader::new(file);
    let mut report = TurnReport::default();
    // A replayed transcript holds earlier turns too; a record that names
    // another prompt marks what follows as that turn's, not this one's.
    let mut in_other_turn = false;

    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 || !line.ends_with('\n') {
            return Ok(report);
        }
        cursor.offset += bytes as u64;

        let Ok(record) = serde_json::from_str::<TranscriptRecord>(&line) else {
            continue;
        };
        if let Some(prompt_id) = record.prompt_id.as_deref() {
            in_other_turn = prompt_id != turn_id;
        }
        if let Some(model) = record.reported_model(harness) {
            report.model = Some(model);
        }
        if record.reports_abort(harness, turn_id) {
            report.aborted = true;
            return Ok(report);
        }
        if record.reports_limit_failure(harness, turn_id) {
            report.failed = true;
            return Ok(report);
        }
        if record.reports_completion(harness, turn_id) && !in_other_turn {
            report.completed = true;
            return Ok(report);
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

fn client_socket_pid(name: &str) -> Option<u32> {
    let rest = name.strip_prefix(".harness-status-")?;
    let rest = rest
        .strip_prefix("follow-")
        .or_else(|| rest.strip_prefix("query-"))?;
    rest.split_once('-')?.0.parse().ok()
}

fn cleanup_stale_client_sockets(server_path: &Path) -> io::Result<usize> {
    let parent = server_path.parent().unwrap_or_else(|| Path::new("/tmp"));
    let mut removed = 0;
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(pid) = client_socket_pid(&name) else {
            continue;
        };
        if read_process(pid).is_some() {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => removed += 1,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(removed)
}

fn current_uid() -> Option<String> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("Uid:"))?;
    line.split_ascii_whitespace().nth(1).map(str::to_owned)
}

fn owner_for_pid(pid: u32) -> Option<Owner> {
    Some(Owner {
        pid,
        start_ticks: read_process(pid)?.start_ticks,
    })
}

fn find_harness_owner(harness: &str) -> Option<Owner> {
    let mut pid = process::id();
    for _ in 0..64 {
        let info = read_process(pid)?;
        let matches_harness = match harness {
            "codex" => info.comm == "codex",
            "claude" => matches!(info.comm.as_str(), "claude" | "claude.exe"),
            _ => false,
        };
        if matches_harness {
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

fn delegated_process() -> bool {
    env::var("HARNESS_STATUS_DELEGATED")
        .is_ok_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "on"))
}

fn codex_state_root() -> Option<PathBuf> {
    env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex")))
}

fn is_internal_codex_cwd(cwd: &str, state_root: &Path) -> bool {
    Path::new(cwd).starts_with(state_root.join("memories"))
}

fn notification_app_name(harness: &str) -> String {
    format!("harness-status-{harness}")
}

fn harness_display_name(harness: &str) -> &str {
    match harness {
        "codex" => "Codex",
        "claude" => "Claude Code",
        "pi" => "Pi",
        _ => harness,
    }
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

fn model_slug(model: &str, effort: Option<&str>) -> String {
    match effort.filter(|effort| !effort.is_empty() && !model.contains('#')) {
        Some(effort) if !model.is_empty() => format!("{model}#{effort}"),
        _ => model.to_owned(),
    }
}

/// Whether `current` already names `incoming` with an effort that
/// `incoming` omits, so replacing it would only lose the effort.
fn refines_model(current: Option<&str>, incoming: &str) -> bool {
    current
        .and_then(|current| current.split_once('#'))
        .is_some_and(|(model, _)| model == incoming)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_slug_keeps_effort_in_the_model_value_when_available() {
        assert_eq!(model_slug("gpt-6-sol", Some("high")), "gpt-6-sol#high");
        assert_eq!(model_slug("gpt-6-sol", None), "gpt-6-sol");
        assert_eq!(model_slug("gpt-6-sol", Some("")), "gpt-6-sol");
        assert_eq!(model_slug("gpt-6-sol#high", Some("high")), "gpt-6-sol#high");
    }

    #[test]
    fn a_model_without_effort_does_not_replace_the_same_model_with_one() {
        assert!(refines_model(Some("gpt-6-sol#high"), "gpt-6-sol"));
        assert!(!refines_model(Some("gpt-6-sol#high"), "gpt-6-luna"));
        assert!(!refines_model(Some("gpt-6-sol#high"), "gpt-6-sol#low"));
        assert!(!refines_model(Some("gpt-6-sol"), "gpt-6-sol"));
        assert!(!refines_model(None, "gpt-6-sol"));
    }

    #[test]
    fn claude_synthetic_assistant_does_not_replace_the_model() {
        let record: TranscriptRecord = serde_json::from_value(json!({
            "type": "assistant",
            "effort": "high",
            "message": {"model": "<synthetic>"}
        }))
        .unwrap();
        assert_eq!(record.reported_model("claude"), None);
    }

    #[test]
    fn usage_refresh_backoff_respects_provider_limits() {
        let now = 1_000;
        let mut refresh = ProviderRefresh::default();

        refresh_failed(&mut refresh, now, RefreshError::RateLimited(None));
        assert_eq!(refresh.next_at, now + 180);
        assert_eq!(refresh.failures, 1);

        refresh_failed(&mut refresh, now, RefreshError::RateLimited(None));
        assert_eq!(refresh.next_at, now + 360);

        refresh_failed(&mut refresh, now, RefreshError::RateLimited(Some(600)));
        assert_eq!(refresh.next_at, now + 720);

        refresh_failed(&mut refresh, now, RefreshError::RateLimited(None));
        assert_eq!(refresh.next_at, now + 900);

        refresh_failed(&mut refresh, now, RefreshError::Unavailable);
        assert_eq!(refresh.next_at, now + 180);

        refresh_succeeded(&mut refresh, now);
        assert_eq!(refresh.next_at, now + 180);
        assert_eq!(refresh.failures, 0);
    }

    #[test]
    fn claude_usage_timestamps_and_retry_after_are_normalized() {
        let window = parse_claude_api_window(Some(&json!({
            "utilization": 56.4,
            "resets_at": "2033-05-18T03:33:19Z"
        })))
        .unwrap();
        assert_eq!(window.used_percentage, 56);
        assert_eq!(window.remaining_percentage, 44);
        assert_eq!(window.resets_at, 1_999_999_999);
        assert_eq!(parse_retry_after("240", 1_000), Some(240));
        assert_eq!(
            parse_retry_after("Wed, 18 May 2033 03:33:19 GMT", 1_999_999_000),
            Some(999)
        );
    }

    #[test]
    fn usage_reset_countdowns_are_compact_and_never_round_to_zero_early() {
        assert_eq!(format_reset_in(0), "now");
        assert_eq!(format_reset_in(1), "1m");
        assert_eq!(format_reset_in(60), "1m");
        assert_eq!(format_reset_in(61), "2m");
        assert_eq!(format_reset_in(3_661), "1h02m");
        assert_eq!(format_reset_in(90_000), "1d1h");
    }

    fn event(name: &str, notification_type: Option<&str>) -> HookEvent {
        event_with_delegation(name, notification_type, false)
    }

    fn event_with_delegation(
        name: &str,
        notification_type: Option<&str>,
        delegated: bool,
    ) -> HookEvent {
        HookEvent {
            event: name.to_owned(),
            session_id: "session".to_owned(),
            turn_id: None,
            transcript: None,
            cwd: "/tmp/project".to_owned(),
            model: String::new(),
            notification_type: notification_type.map(str::to_owned),
            delegated,
            background: false,
        }
    }

    #[test]
    fn notifications_follow_semantic_state_transitions() {
        let mut registry = Registry::default();

        let working = registry.apply_hook("claude", event("UserPromptSubmit", None), None);
        assert!(working.changed);
        assert!(working.notification.is_none());

        let completed = registry.apply_hook("claude", event("Stop", None), None);
        assert!(completed.changed);
        assert_eq!(completed.notification.unwrap().state, "completed");

        let duplicate_completed = registry.apply_hook("claude", event("Stop", None), None);
        assert!(duplicate_completed.changed);
        assert!(duplicate_completed.notification.is_none());

        let attention = registry.apply_hook(
            "claude",
            event("Notification", Some("permission_prompt")),
            None,
        );
        assert!(attention.changed);
        assert_eq!(attention.notification.unwrap().state, "attention");

        let duplicate_attention = registry.apply_hook(
            "claude",
            event("Notification", Some("permission_prompt")),
            None,
        );
        assert!(duplicate_attention.changed);
        assert!(duplicate_attention.notification.is_none());

        for ignored_type in ["auth_success", "idle_prompt", "agent_completed"] {
            let ignored =
                registry.apply_hook("claude", event("Notification", Some(ignored_type)), None);
            assert!(!ignored.changed);
            assert!(ignored.notification.is_none());
            assert_eq!(registry.entries[0].status.state, "attention");
        }

        let resumed = registry.apply_hook(
            "claude",
            event("Notification", Some("elicitation_response")),
            None,
        );
        assert!(resumed.notification.is_none());
        assert_eq!(registry.entries[0].status.state, "working");

        let ended = registry.apply_hook("claude", event("SessionEnd", None), None);
        assert!(ended.changed);
        assert!(ended.notification.is_none());
        assert!(registry.entries.is_empty());
    }

    #[test]
    fn idle_visibility_boundary_survives_checkpoints_and_protects_active_states() {
        let mut registry = Registry::default();
        registry.apply_hook("claude", event("Stop", None), None);
        registry.apply_hook("claude", event("Notification", Some("idle_prompt")), None);
        let entry = &mut registry.entries[0];
        let since = entry.idle_since.unwrap();
        assert!(!entry.indicator_hidden(since + 3599));
        assert!(entry.indicator_hidden(since + 3600));
        let encoded = serde_json::to_vec(entry).unwrap();
        let restored: Entry = serde_json::from_slice(&encoded).unwrap();
        assert!(restored.indicator_hidden(since + 3600));
        assert_eq!(restored.idle_since, Some(since));
        entry.status.background = true;
        assert!(!entry.indicator_hidden(since + 7200));
        entry.status.background = false;
        for state in ["working", "attention", "completed"] {
            entry.status.state = state.to_owned();
            assert!(!entry.indicator_hidden(since + 7200));
        }
    }

    #[test]
    fn idle_timeout_hides_only_the_indicator_and_activity_restores_it() {
        let mut registry = Registry::default();
        registry.apply_hook("claude", event("Stop", None), None);
        registry.entries[0].status.updated_at -= 4000;
        registry.settle_quiet_entries();
        let view: Value = serde_json::from_str(&registry.snapshot_json(None)).unwrap();
        assert_eq!(view["threads"][0]["hidden"], true);
        assert_eq!(registry.entries.len(), 1);
        registry.apply_hook("claude", event("UserPromptSubmit", None), None);
        let view: Value = serde_json::from_str(&registry.snapshot_json(None)).unwrap();
        assert_eq!(view["threads"][0]["hidden"], false);
    }

    #[test]
    fn a_settled_thread_goes_quiet_without_announcing_itself() {
        let mut registry = Registry::default();
        registry.apply_hook("claude", event("UserPromptSubmit", None), None);
        registry.apply_hook("claude", event("Stop", None), None);

        // A thread that just finished is still news.
        assert!(!registry.settle_quiet_entries());
        assert_eq!(registry.entries[0].status.state, "completed");

        let idle = registry.apply_hook("claude", event("Notification", Some("idle_prompt")), None);
        assert!(idle.changed);
        assert!(idle.notification.is_none());
        assert_eq!(registry.entries[0].status.state, "idle");

        // Waiting is not attention: an idle report never creates a thread, and
        // never demotes one that is working.
        registry.entries.clear();
        let unknown =
            registry.apply_hook("claude", event("Notification", Some("idle_prompt")), None);
        assert!(!unknown.changed);
        assert!(registry.entries.is_empty());

        registry.apply_hook("claude", event("UserPromptSubmit", None), None);
        registry.apply_hook("claude", event("Notification", Some("idle_prompt")), None);
        assert_eq!(registry.entries[0].status.state, "working");

        // Every harness reaches the same state by sitting still.
        registry.apply_hook("claude", event("Stop", None), None);
        registry.entries[0].status.updated_at -= IDLE_AFTER_SECONDS;
        assert!(registry.settle_quiet_entries());
        assert_eq!(registry.entries[0].status.state, "idle");
    }

    #[test]
    fn attention_reports_its_reason_and_background_work_survives_a_stop() {
        let mut registry = Registry::default();
        registry.apply_hook("claude", event("UserPromptSubmit", None), None);
        assert!(registry.entries[0].status.started_at.is_some());

        let mut stop = event("Stop", None);
        stop.background = true;
        registry.apply_hook("claude", stop, None);
        assert!(registry.entries[0].status.background);
        assert!(registry.entries[0].status.reason.is_none());

        let permission = registry.apply_hook(
            "claude",
            event("Notification", Some("permission_prompt")),
            None,
        );
        assert_eq!(
            permission.notification.expect("attention notifies").reason,
            Some("permission".to_owned())
        );
        assert_eq!(
            registry.entries[0].status.reason.as_deref(),
            Some("permission")
        );

        let subagent = registry.apply_hook(
            "claude",
            event("Notification", Some("agent_needs_input")),
            None,
        );
        assert!(subagent.notification.is_none(), "same state, same news");
        assert_eq!(
            registry.entries[0].status.reason.as_deref(),
            Some("subagent")
        );

        // A new turn clears both the reason and the finished turn's leftovers.
        registry.apply_hook("claude", event("UserPromptSubmit", None), None);
        assert!(registry.entries[0].status.reason.is_none());
        assert!(!registry.entries[0].status.background);
    }

    #[test]
    fn only_a_new_prompt_restarts_the_reported_turn_clock() {
        const BEGAN: u64 = 1_000;
        let mut registry = Registry::default();

        registry.apply_hook("claude", event("UserPromptSubmit", None), None);
        registry.entries[0].status.started_at = Some(BEGAN);

        // Answering a dialog resumes the turn that was already under way, so a
        // bar keeps reporting how long that turn has run.
        for resumed in ["elicitation_complete", "elicitation_response"] {
            registry.apply_hook(
                "claude",
                event("Notification", Some("permission_prompt")),
                None,
            );
            registry.apply_hook("claude", event("Notification", Some(resumed)), None);
            assert_eq!(registry.entries[0].status.state, "working");
            assert_eq!(registry.entries[0].status.started_at, Some(BEGAN));
        }

        // The next prompt is a new turn, and starts its clock over.
        registry.apply_hook("claude", event("UserPromptSubmit", None), None);
        assert!(registry.entries[0].status.started_at > Some(BEGAN));

        let mut registry = Registry::default();
        registry.apply_hook("pi", event("AgentStart", None), None);
        registry.entries[0].status.started_at = Some(BEGAN);
        registry.apply_hook("pi", event("Attention", None), None);
        registry.apply_hook("pi", event("ResumeWorking", None), None);
        assert_eq!(registry.entries[0].status.started_at, Some(BEGAN));
    }

    #[test]
    fn a_turn_is_reported_by_whole_units_it_has_reached() {
        assert_eq!(format_duration(0), "0s");
        assert_eq!(format_duration(59), "59s");
        assert_eq!(format_duration(60), "1m");
        // Ninety seconds is one minute, never the two a nearest-unit round
        // would report.
        assert_eq!(format_duration(90), "1m");
        assert_eq!(format_duration(3599), "59m");
        assert_eq!(format_duration(3600), "1h00m");
        assert_eq!(format_duration(7500), "2h05m");
        assert_eq!(format_duration(86_399), "23h59m");
        assert_eq!(format_duration(86_400), "1d00h");
        assert_eq!(format_duration(178_812), "2d01h");
    }

    #[test]
    fn a_settled_turn_reports_its_own_length_not_how_long_ago_it_began() {
        let mut registry = Registry::default();
        registry.apply_hook("claude", event("UserPromptSubmit", None), None);
        let status = &mut registry.entries[0].status;
        status.started_at = Some(1_000);
        status.updated_at = 1_000;

        // A running turn is measured against now, and keeps climbing.
        assert_eq!(status.elapsed_seconds(1_090), 90);
        assert_eq!(status.elapsed_seconds(1_300), 300);

        // A clock that still lags the turn's start never reads as negative.
        assert_eq!(status.elapsed_seconds(900), 0);

        // Once settled, the turn's length is fixed no matter how long it waits.
        registry.apply_hook("claude", event("Stop", None), None);
        let status = &mut registry.entries[0].status;
        status.started_at = Some(1_000);
        status.updated_at = 1_180;
        assert_eq!(status.elapsed_seconds(1_180), 180);
        assert_eq!(status.elapsed_seconds(9_999), 180);
    }

    #[test]
    fn every_thread_carries_a_duration_a_bar_can_render_directly() {
        let mut registry = Registry::default();
        registry.apply_hook("claude", event("UserPromptSubmit", None), None);
        registry.entries[0].status.started_at = Some(0);
        registry.entries[0].status.updated_at = 0;

        let snapshot: Value =
            serde_json::from_str(&registry.snapshot_json(Some("claude"))).unwrap();
        let thread = &snapshot["threads"][0];
        assert!(thread["duration"].as_str().is_some());
        // The thread's own fields survive alongside it.
        assert_eq!(thread["state"], "working");
        assert_eq!(thread["name"], "project");
    }

    #[test]
    fn indicator_order_follows_the_threads_not_their_event_history() {
        let mut registry = Registry::default();
        for (session, cwd) in [("b", "/tmp/zebra"), ("a", "/tmp/apple")] {
            let mut start = event("UserPromptSubmit", None);
            start.session_id = session.to_owned();
            start.cwd = cwd.to_owned();
            registry.apply_hook("claude", start, None);
        }
        let ordered = registry.snapshot_json(Some("claude"));

        // Re-registering the first thread must not move it to the end.
        let mut restart = event("UserPromptSubmit", None);
        restart.session_id = "b".to_owned();
        restart.cwd = "/tmp/zebra".to_owned();
        registry.entries.retain(|entry| entry.status.id != "b");
        registry.apply_hook("claude", restart, None);

        assert!(ordered.find("apple").unwrap() < ordered.find("zebra").unwrap());
        assert_eq!(registry.snapshot_json(Some("claude")), ordered);
    }

    #[test]
    fn retiring_an_already_retired_owner_socket_succeeds() {
        let path = env::temp_dir().join(format!("harness-status-absent-{}.sock", process::id()));
        let _ = fs::remove_file(&path);
        assert!(retire_dead_owner_socket(&path).is_ok());
        UnixDatagram::bind(&path).expect("the path is free after retirement");
        assert!(retire_dead_owner_socket(&path).is_ok());
        assert!(!path.exists());
    }

    #[test]
    fn delegated_completion_waits_for_the_parent_but_attention_notifies() {
        let mut registry = Registry::default();

        registry.apply_hook("pi", event_with_delegation("AgentStart", None, true), None);
        let completed = registry.apply_hook(
            "pi",
            event_with_delegation("AgentSettled", None, true),
            None,
        );
        assert!(completed.notification.is_none());
        assert_eq!(registry.entries[0].status.state, "completed");

        let attention =
            registry.apply_hook("pi", event_with_delegation("Attention", None, true), None);
        assert_eq!(attention.notification.unwrap().state, "attention");
    }

    #[test]
    fn pi_attention_resume_restores_state_without_a_second_notification() {
        let mut registry = Registry::default();

        let working = registry.apply_hook("pi", event("AgentStart", None), None);
        assert!(working.notification.is_none());
        let attention = registry.apply_hook("pi", event("Attention", None), None);
        assert_eq!(attention.notification.unwrap().state, "attention");
        let resumed_working = registry.apply_hook("pi", event("ResumeWorking", None), None);
        assert!(resumed_working.notification.is_none());
        assert_eq!(registry.entries[0].status.state, "working");

        let attention = registry.apply_hook("pi", event("Attention", None), None);
        assert_eq!(attention.notification.unwrap().state, "attention");
        let resumed_completed = registry.apply_hook("pi", event("ResumeCompleted", None), None);
        assert!(resumed_completed.notification.is_none());
        assert_eq!(registry.entries[0].status.state, "completed");
    }

    #[test]
    fn codex_closing_a_turn_with_its_usage_limit_error_is_a_failure_not_silence() {
        let mut registry = Registry::default();
        let mut start = event("UserPromptSubmit", None);
        start.turn_id = Some("turn-limit".to_owned());
        start.transcript = Some(TranscriptCursor {
            path: env::temp_dir().join("harness-status-absent-transcript.jsonl"),
            offset: 0,
        });
        registry.apply_hook("codex", start, None);
        registry.entries[0].status.started_at = Some(1_000);
        registry.entries[0].status.updated_at = 1_000;

        let limit_record = TranscriptRecord {
            kind: "event_msg".to_owned(),
            timestamp: None,
            effort: None,
            per_turn_effort: None,
            payload: TranscriptPayload {
                kind: "task_complete".to_owned(),
                cwd: None,
                model: None,
                effort: None,
                turn_id: Some("turn-limit".to_owned()),
                error: Some(json!({"codex_error_info": "usage_limit_exceeded"})),
            },
            prompt_id: None,
            interrupted_message_id: None,
            subtype: None,
            prevented_continuation: false,
            message: TranscriptMessage::default(),
        };
        assert!(limit_record.reports_limit_failure("codex", "turn-limit"));
        // The same completion without a turn match, without an error, or with
        // an unrecognized error code leaves the turn alone.
        assert!(!limit_record.reports_limit_failure("codex", "other-turn"));
        let clean = TranscriptRecord {
            payload: TranscriptPayload {
                error: None,
                ..limit_record.payload.clone()
            },
            ..limit_record.clone()
        };
        assert!(!clean.reports_limit_failure("codex", "turn-limit"));
        let other_code = TranscriptRecord {
            payload: TranscriptPayload {
                error: Some(json!({"codex_error_info": "context_window_exceeded"})),
                ..limit_record.payload.clone()
            },
            ..limit_record.clone()
        };
        assert!(!other_code.reports_limit_failure("codex", "turn-limit"));
        // Claude has no such vocabulary; its own StopFailure hook covers it.
        assert!(!limit_record.reports_limit_failure("claude", "turn-limit"));

        // The tail settles the thread as attention, not as a missing entry.
        // The record reaches the tail through a real transcript file.
        let transcript = env::temp_dir().join(format!(
            "harness-status-limit-test-{}-{}.jsonl",
            process::id(),
            unix_time_seconds()
        ));
        fs::write(
            &transcript,
            format!(
                "{}\n{}\n",
                json!({"type": "session_meta"}),
                json!({
                    "type": "event_msg",
                    "payload": {
                        "type": "task_complete",
                        "turn_id": "turn-limit",
                        "error": {"codex_error_info": "usage_limit_exceeded"}
                    }
                })
            ),
        )
        .unwrap();
        registry.entries[0].transcript = Some(TranscriptCursor {
            path: transcript.clone(),
            offset: 0,
        });
        let (changed, notification) = registry.follow_running_transcripts();
        assert!(changed);
        assert_eq!(registry.entries.len(), 1);
        assert_eq!(registry.entries[0].status.state, "attention");
        assert_eq!(
            registry.entries[0].status.reason.as_deref(),
            Some("failure")
        );
        let notification = notification.expect("a failure notifies");
        assert_eq!(notification.state, "attention");
        assert_eq!(notification.reason.as_deref(), Some("failure"));
        fs::remove_file(&transcript).unwrap();
        // A settled entry is never tailed again, so the notification cannot
        // repeat; and the aborted path still retires without notifying.
        let (changed, notification) = registry.follow_running_transcripts();
        assert!(!changed);
        assert!(notification.is_none());
    }

    fn append_records(path: &Path, records: &[Value]) {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        for record in records {
            writeln!(file, "{record}").unwrap();
        }
    }

    fn stop_summary(prevented: bool) -> Value {
        json!({"type": "system", "subtype": "stop_hook_summary", "preventedContinuation": prevented})
    }

    #[test]
    fn a_recorded_claude_stop_settles_only_its_own_turn_and_ends_idle() {
        let transcript = env::temp_dir().join(format!(
            "harness-status-lost-stop-{}-{}.jsonl",
            process::id(),
            unix_time_seconds()
        ));
        // A replay from the start passes an earlier turn's stop first.
        append_records(
            &transcript,
            &[
                json!({"type": "user", "promptId": "turn-old", "message": {"content": "old"}}),
                stop_summary(false),
                json!({"type": "user", "promptId": "turn-now", "message": {"content": "now"}}),
            ],
        );
        let mut registry = Registry::default();
        let mut start = event("UserPromptSubmit", None);
        start.turn_id = Some("turn-now".to_owned());
        start.transcript = Some(TranscriptCursor {
            path: transcript.clone(),
            offset: 0,
        });
        registry.apply_hook("claude", start, None);
        let (_, notification) = registry.follow_running_transcripts();
        assert!(notification.is_none());
        assert_eq!(registry.entries[0].status.state, "working");

        // A Stop hook that blocked the stop keeps the turn going.
        append_records(&transcript, &[stop_summary(true)]);
        registry.follow_running_transcripts();
        assert_eq!(registry.entries[0].status.state, "working");

        append_records(&transcript, &[stop_summary(false)]);
        let (changed, notification) = registry.follow_running_transcripts();
        assert!(changed);
        assert_eq!(registry.entries[0].status.state, "completed");
        assert_eq!(notification.expect("the end notifies").state, "completed");

        // The late hook finds the turn already settled and adds nothing new.
        let result = registry.apply_hook("claude", event("Stop", None), None);
        assert!(result.notification.is_none());

        registry.entries[0].status.updated_at -= IDLE_AFTER_SECONDS;
        assert!(registry.settle_quiet_entries());
        assert_eq!(registry.entries[0].status.state, "idle");
        fs::remove_file(&transcript).unwrap();
    }

    #[test]
    fn a_recorded_stop_in_a_delegated_run_settles_without_notifying() {
        let transcript = env::temp_dir().join(format!(
            "harness-status-delegated-stop-{}-{}.jsonl",
            process::id(),
            unix_time_seconds()
        ));
        append_records(&transcript, &[]);
        let mut registry = Registry::default();
        let mut start = event_with_delegation("UserPromptSubmit", None, true);
        start.turn_id = Some("turn-now".to_owned());
        start.transcript = Some(TranscriptCursor {
            path: transcript.clone(),
            offset: 0,
        });
        registry.apply_hook("claude", start, None);
        append_records(&transcript, &[stop_summary(false)]);
        let (changed, notification) = registry.follow_running_transcripts();
        assert!(changed);
        assert!(notification.is_none());
        assert_eq!(registry.entries[0].status.state, "completed");
        fs::remove_file(&transcript).unwrap();
    }

    #[test]
    fn a_turn_aborted_during_the_tail_still_retires_without_notifying() {
        // The abort path is exercised end to end by the smoke suite; here the
        // empty-report default proves an absent transcript retires nothing.
        let mut registry = Registry::default();
        let mut start = event("UserPromptSubmit", None);
        start.turn_id = Some("turn-x".to_owned());
        start.transcript = Some(TranscriptCursor {
            path: PathBuf::from("/harness-status-nonexistent/transcript.jsonl"),
            offset: 0,
        });
        registry.apply_hook("codex", start, None);
        let (changed, notification) = registry.follow_running_transcripts();
        assert!(!changed);
        assert!(notification.is_none());
        assert_eq!(registry.entries[0].status.state, "working");
    }

    #[test]
    fn restoring_a_checkpoint_rewinds_running_transcripts() {
        let mut registry = Registry::default();
        let mut start = event("UserPromptSubmit", None);
        start.transcript = Some(TranscriptCursor {
            path: PathBuf::from("/tmp/codex-rollout.jsonl"),
            offset: 9_999,
        });
        registry.apply_hook("codex", start, None);

        let mut claude_start = event("UserPromptSubmit", None);
        claude_start.transcript = Some(TranscriptCursor {
            path: PathBuf::from("/tmp/claude-transcript.jsonl"),
            offset: 9_999,
        });
        registry.apply_hook("claude", claude_start, None);

        // A settled thread is left where its last tail put it.
        let mut settled_start = event("UserPromptSubmit", None);
        settled_start.session_id = "settled".to_owned();
        settled_start.transcript = Some(TranscriptCursor {
            path: PathBuf::from("/tmp/settled-transcript.jsonl"),
            offset: 9_999,
        });
        registry.apply_hook("claude", settled_start, None);
        let mut settled_stop = event("Stop", None);
        settled_stop.session_id = "settled".to_owned();
        registry.apply_hook("claude", settled_stop, None);

        registry.rewind_running_transcripts();
        assert_eq!(registry.entries.len(), 3);
        assert_eq!(registry.entries[0].transcript.as_ref().unwrap().offset, 0);
        assert_eq!(registry.entries[1].transcript.as_ref().unwrap().offset, 0);
        assert_eq!(
            registry.entries[2].transcript.as_ref().unwrap().offset,
            9_999
        );
    }

    #[test]
    fn internal_codex_memory_work_is_outside_the_user_facing_registry() {
        let root = Path::new("/home/test/.codex");
        assert!(is_internal_codex_cwd("/home/test/.codex/memories", root));
        assert!(is_internal_codex_cwd(
            "/home/test/.codex/memories/work",
            root
        ));
        assert!(!is_internal_codex_cwd("/home/test/code/memories", root));
    }

    #[test]
    fn notification_groups_are_distinct_per_harness() {
        assert_eq!(notification_app_name("codex"), "harness-status-codex");
        assert_eq!(notification_app_name("claude"), "harness-status-claude");
        assert_eq!(notification_app_name("pi"), "harness-status-pi");
    }

    #[test]
    fn stale_client_socket_cleanup_preserves_live_pid_paths() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            env::temp_dir().join(format!("harness-status-sockets-{}-{nonce}", process::id()));
        fs::create_dir(&root).unwrap();
        let server = root.join("harness-status.sock");
        let dead = root.join(".harness-status-follow-2147483647-1.sock");
        let live = root.join(format!(".harness-status-follow-{}-2.sock", process::id()));
        drop(UnixDatagram::bind(&dead).unwrap());
        let live_socket = UnixDatagram::bind(&live).unwrap();

        assert_eq!(cleanup_stale_client_sockets(&server).unwrap(), 1);
        assert!(!dead.exists());
        assert!(live.exists());

        drop(live_socket);
        fs::remove_file(live).unwrap();
        fs::remove_dir(root).unwrap();
    }
}
