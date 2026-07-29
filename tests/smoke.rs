use serde_json::{Value, json};
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Follower {
    child: Child,
    dir: PathBuf,
}

impl Drop for Follower {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn codex_hook_to_jsonl_smoke() {
    let binary = env!("CARGO_BIN_EXE_harness-status");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("harness-status-test-{nonce}"));
    fs::create_dir(&dir).unwrap();
    let socket = dir.join("status.sock");
    verify_codex_hook_lifecycle(binary, &dir);

    let child = Command::new(binary)
        .args(["codex", "--follow"])
        .env("HARNESS_STATUS_SOCKET", &socket)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let mut owner = Follower {
        child,
        dir: dir.clone(),
    };
    wait_for_socket(&socket);
    let state_path = state_path(&socket);
    wait_for_path(&state_path);
    assert_eq!(
        fs::metadata(&state_path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let mut subscriber = Command::new(binary)
        .args(["codex", "--follow"])
        .env("HARNESS_STATUS_SOCKET", &socket)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_follow_socket(&dir);

    let transcript = dir.join("transcript.jsonl");
    fs::write(&transcript, "{\"type\":\"session_meta\"}\n").unwrap();
    send_hook(
        binary,
        &socket,
        "codex",
        "UserPromptSubmit",
        Some(&transcript),
    );
    wait_for_state(binary, &socket, "codex", "working");
    assert!(
        !fs::read_to_string(&state_path)
            .unwrap()
            .contains("not retained")
    );

    owner.child.kill().unwrap();
    owner.child.wait().unwrap();
    wait_for_state(binary, &socket, "codex", "working");

    let mut transcript_file = fs::OpenOptions::new()
        .append(true)
        .open(&transcript)
        .unwrap();
    writeln!(
        transcript_file,
        "{}",
        json!({
            "type": "event_msg",
            "payload": {
                "type": "turn_aborted",
                "turn_id": "turn-smoke",
                "reason": "interrupted"
            }
        })
    )
    .unwrap();
    wait_for_thread_count(binary, &socket, "codex", 0);

    send_hook(binary, &socket, "codex", "UserPromptSubmit", None);
    let working = wait_for_state(binary, &socket, "codex", "working");
    assert_eq!(working["threads"][0]["id"], "thread-smoke");
    assert_eq!(working["threads"][0]["name"], "example");

    send_hook(binary, &socket, "codex", "Stop", None);
    wait_for_state(binary, &socket, "codex", "completed");

    subscriber.kill().unwrap();
    let output = subscriber.wait_with_output().unwrap();

    let mut replacement = Command::new(binary)
        .args(["codex", "--follow"])
        .env("HARNESS_STATUS_SOCKET", &socket)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    wait_for_state(binary, &socket, "codex", "completed");
    assert!(
        Command::new(binary)
            .args(["ack", "thread-smoke"])
            .env("HARNESS_STATUS_SOCKET", &socket)
            .status()
            .unwrap()
            .success()
    );
    let empty = wait_for_thread_count(binary, &socket, "codex", 0);
    assert_eq!(empty["threads"].as_array().unwrap().len(), 0);
    replacement.kill().unwrap();
    replacement.wait().unwrap();

    let mut final_owner = Command::new(binary)
        .args(["codex", "--follow"])
        .env("HARNESS_STATUS_SOCKET", &socket)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    wait_for_thread_count(binary, &socket, "codex", 0);
    final_owner.kill().unwrap();
    final_owner.wait().unwrap();

    let states: Vec<Option<String>> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .map(|snapshot| {
            snapshot["threads"]
                .as_array()
                .and_then(|threads| threads.first())
                .and_then(|thread| thread["state"].as_str())
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(
        states,
        [
            None,
            Some("working".to_owned()),
            None,
            Some("working".to_owned()),
            Some("completed".to_owned())
        ]
    );
}

#[test]
fn claude_hook_to_jsonl_smoke() {
    let binary = env!("CARGO_BIN_EXE_harness-status");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("harness-status-claude-test-{nonce}"));
    fs::create_dir(&dir).unwrap();
    let socket = dir.join("status.sock");
    verify_claude_hook_lifecycle(binary, &dir);

    let child = Command::new(binary)
        .args(["claude", "--follow"])
        .env("HARNESS_STATUS_SOCKET", &socket)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let _follower = Follower {
        child,
        dir: dir.clone(),
    };
    wait_for_socket(&socket);

    let transcript = dir.join("claude-transcript.jsonl");
    fs::write(&transcript, "sensitive transcript content\n").unwrap();
    send_hook(
        binary,
        &socket,
        "claude",
        "UserPromptSubmit",
        Some(&transcript),
    );
    let working = wait_for_state(binary, &socket, "claude", "working");
    assert_eq!(working["threads"][0]["id"], "claude-thread-smoke");
    assert_eq!(working["threads"][0]["harness"], "claude");
    assert_eq!(working["threads"][0]["name"], "example");
    assert!(working["threads"][0].get("model").is_none());
    assert!(working["threads"][0].get("turn_id").is_none());
    let checkpoint = fs::read_to_string(state_path(&socket)).unwrap();
    assert!(!checkpoint.contains("sensitive transcript content"));
    assert!(!checkpoint.contains("claude-transcript.jsonl"));
    assert!(!checkpoint.contains("not retained"));

    send_hook(binary, &socket, "claude", "Stop", Some(&transcript));
    wait_for_state(binary, &socket, "claude", "completed");
    send_hook(
        binary,
        &socket,
        "claude",
        "UserPromptSubmit",
        Some(&transcript),
    );
    wait_for_state(binary, &socket, "claude", "working");
    send_hook(binary, &socket, "claude", "StopFailure", Some(&transcript));
    wait_for_state(binary, &socket, "claude", "completed");
    assert!(
        Command::new(binary)
            .args(["ack", "claude-thread-smoke"])
            .env("HARNESS_STATUS_SOCKET", &socket)
            .status()
            .unwrap()
            .success()
    );
    wait_for_thread_count(binary, &socket, "claude", 0);
}

#[test]
fn pi_extension_to_jsonl_smoke() {
    let binary = env!("CARGO_BIN_EXE_harness-status");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("harness-status-pi-test-{nonce}"));
    fs::create_dir(&dir).unwrap();
    let socket = dir.join("status.sock");
    verify_pi_extension_lifecycle(binary, &dir);

    let child = Command::new(binary)
        .args(["pi", "--follow"])
        .env("HARNESS_STATUS_SOCKET", &socket)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let _follower = Follower {
        child,
        dir: dir.clone(),
    };
    wait_for_socket(&socket);

    send_hook(binary, &socket, "pi", "AgentStart", None);
    let working = wait_for_state(binary, &socket, "pi", "working");
    assert_eq!(working["threads"][0]["id"], "pi-thread-smoke");
    assert_eq!(working["threads"][0]["harness"], "pi");
    assert_eq!(working["threads"][0]["name"], "example");
    assert_eq!(working["threads"][0]["model"], "pi-model-smoke");
    assert!(working["threads"][0].get("turn_id").is_none());
    let checkpoint = fs::read_to_string(state_path(&socket)).unwrap();
    assert!(!checkpoint.contains("not retained"));

    send_hook(binary, &socket, "pi", "AgentSettled", None);
    wait_for_state(binary, &socket, "pi", "completed");
    send_hook(binary, &socket, "pi", "AgentStart", None);
    wait_for_state(binary, &socket, "pi", "working");
    send_hook(binary, &socket, "pi", "SessionShutdown", None);
    wait_for_thread_count(binary, &socket, "pi", 0);

    let mut pi_owner = Command::new("sleep").arg("30").spawn().unwrap();
    send_hook_with_owner(
        binary,
        &socket,
        "pi",
        "AgentStart",
        None,
        Some(pi_owner.id()),
    );
    wait_for_state(binary, &socket, "pi", "working");
    pi_owner.kill().unwrap();
    pi_owner.wait().unwrap();
    wait_for_thread_count(binary, &socket, "pi", 0);
}

fn verify_codex_hook_lifecycle(binary: &str, dir: &Path) {
    let codex_home = dir.join("codex-home");
    fs::create_dir(&codex_home).unwrap();
    let hooks_path = codex_home.join("hooks.json");
    fs::write(
        &hooks_path,
        r#"{
  "hooks": {
    "SessionStart": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "keep-me"
          }
        ]
      }
    ],
    "UserPromptSubmit": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "harness-status hook codex",
            "timeout": 5
          }
        ]
      }
    ]
  }
}
"#,
    )
    .unwrap();

    let install = Command::new(binary)
        .args(["install", "codex"])
        .env("CODEX_HOME", &codex_home)
        .output()
        .unwrap();
    assert!(install.status.success());
    let installed_bytes = fs::read(&hooks_path).unwrap();
    let installed: Value = serde_json::from_slice(&installed_bytes).unwrap();
    assert_eq!(
        installed["hooks"]["SessionStart"][0]["hooks"][0]["command"],
        "keep-me"
    );
    for event in ["UserPromptSubmit", "Stop"] {
        let handlers = installed["hooks"][event]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["hooks"].as_array().unwrap());
        let commands: Vec<&str> = handlers
            .map(|handler| handler["command"].as_str().unwrap())
            .collect();
        assert_eq!(commands.len(), 1);
        assert!(commands[0].ends_with("' hook codex"));
        assert!(commands[0].contains(binary));
    }

    let reinstall = Command::new(binary)
        .args(["install", "codex"])
        .env("CODEX_HOME", &codex_home)
        .output()
        .unwrap();
    assert!(reinstall.status.success());
    assert_eq!(fs::read(&hooks_path).unwrap(), installed_bytes);

    let uninstall = Command::new(binary)
        .args(["uninstall", "codex"])
        .env("CODEX_HOME", &codex_home)
        .output()
        .unwrap();
    assert!(uninstall.status.success());
    let uninstalled_bytes = fs::read(&hooks_path).unwrap();
    let uninstalled: Value = serde_json::from_slice(&uninstalled_bytes).unwrap();
    assert_eq!(
        uninstalled["hooks"]["SessionStart"][0]["hooks"][0]["command"],
        "keep-me"
    );
    assert!(uninstalled["hooks"].get("UserPromptSubmit").is_none());
    assert!(uninstalled["hooks"].get("Stop").is_none());

    let reuninstall = Command::new(binary)
        .args(["uninstall", "codex"])
        .env("CODEX_HOME", &codex_home)
        .output()
        .unwrap();
    assert!(reuninstall.status.success());
    assert_eq!(fs::read(&hooks_path).unwrap(), uninstalled_bytes);
}

fn verify_claude_hook_lifecycle(binary: &str, dir: &Path) {
    let claude_config = dir.join("claude-config");
    fs::create_dir(&claude_config).unwrap();
    let settings_path = claude_config.join("settings.json");
    fs::write(
        &settings_path,
        r#"{
  "model": "keep-me",
  "hooks": {
    "SessionStart": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "also-keep-me"
          }
        ]
      }
    ],
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "harness-status hook claude",
            "timeout": 5
          }
        ]
      }
    ]
  }
}
"#,
    )
    .unwrap();

    let install = Command::new(binary)
        .args(["install", "claude"])
        .env("CLAUDE_CONFIG_DIR", &claude_config)
        .output()
        .unwrap();
    assert!(install.status.success());
    let installed_bytes = fs::read(&settings_path).unwrap();
    let installed: Value = serde_json::from_slice(&installed_bytes).unwrap();
    assert_eq!(installed["model"], "keep-me");
    assert_eq!(
        installed["hooks"]["SessionStart"][0]["hooks"][0]["command"],
        "also-keep-me"
    );
    for event in ["UserPromptSubmit", "Stop", "StopFailure"] {
        let handlers = installed["hooks"][event]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["hooks"].as_array().unwrap());
        let commands: Vec<&str> = handlers
            .map(|handler| handler["command"].as_str().unwrap())
            .collect();
        assert_eq!(commands.len(), 1);
        assert!(commands[0].ends_with("' hook claude"));
        assert!(commands[0].contains(binary));
    }

    let reinstall = Command::new(binary)
        .args(["install", "claude"])
        .env("CLAUDE_CONFIG_DIR", &claude_config)
        .output()
        .unwrap();
    assert!(reinstall.status.success());
    assert_eq!(fs::read(&settings_path).unwrap(), installed_bytes);

    let uninstall = Command::new(binary)
        .args(["uninstall", "claude"])
        .env("CLAUDE_CONFIG_DIR", &claude_config)
        .output()
        .unwrap();
    assert!(uninstall.status.success());
    let uninstalled_bytes = fs::read(&settings_path).unwrap();
    let uninstalled: Value = serde_json::from_slice(&uninstalled_bytes).unwrap();
    assert_eq!(uninstalled["model"], "keep-me");
    assert_eq!(
        uninstalled["hooks"]["SessionStart"][0]["hooks"][0]["command"],
        "also-keep-me"
    );
    assert!(uninstalled["hooks"].get("UserPromptSubmit").is_none());
    assert!(uninstalled["hooks"].get("Stop").is_none());
    assert!(uninstalled["hooks"].get("StopFailure").is_none());

    let reuninstall = Command::new(binary)
        .args(["uninstall", "claude"])
        .env("CLAUDE_CONFIG_DIR", &claude_config)
        .output()
        .unwrap();
    assert!(reuninstall.status.success());
    assert_eq!(fs::read(&settings_path).unwrap(), uninstalled_bytes);
}

fn verify_pi_extension_lifecycle(binary: &str, dir: &Path) {
    let pi_agent_dir = dir.join("pi-agent");
    let extension_path = pi_agent_dir.join("extensions/harness-status.ts");

    let install = Command::new(binary)
        .args(["install", "pi"])
        .env("PI_CODING_AGENT_DIR", &pi_agent_dir)
        .output()
        .unwrap();
    assert!(install.status.success());
    let installed_bytes = fs::read(&extension_path).unwrap();
    let installed = String::from_utf8(installed_bytes.clone()).unwrap();
    assert!(installed.starts_with("// Managed by harness-status."));
    assert!(installed.contains(binary));
    assert!(installed.contains("agent_start"));
    assert!(installed.contains("agent_settled"));
    assert!(installed.contains("session_shutdown"));
    assert_eq!(
        fs::metadata(&extension_path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let reinstall = Command::new(binary)
        .args(["install", "pi"])
        .env("PI_CODING_AGENT_DIR", &pi_agent_dir)
        .output()
        .unwrap();
    assert!(reinstall.status.success());
    assert_eq!(fs::read(&extension_path).unwrap(), installed_bytes);

    let uninstall = Command::new(binary)
        .args(["uninstall", "pi"])
        .env("PI_CODING_AGENT_DIR", &pi_agent_dir)
        .output()
        .unwrap();
    assert!(uninstall.status.success());
    assert!(!extension_path.exists());

    let reuninstall = Command::new(binary)
        .args(["uninstall", "pi"])
        .env("PI_CODING_AGENT_DIR", &pi_agent_dir)
        .output()
        .unwrap();
    assert!(reuninstall.status.success());

    fs::create_dir_all(extension_path.parent().unwrap()).unwrap();
    fs::write(&extension_path, "// user-owned extension\n").unwrap();
    let refused = Command::new(binary)
        .args(["install", "pi"])
        .env("PI_CODING_AGENT_DIR", &pi_agent_dir)
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert_eq!(
        fs::read_to_string(&extension_path).unwrap(),
        "// user-owned extension\n"
    );
    fs::remove_file(extension_path).unwrap();
}

fn send_hook(
    binary: &str,
    socket: &Path,
    harness: &str,
    event: &str,
    transcript_path: Option<&Path>,
) {
    let owner_pid = (harness == "pi").then_some(std::process::id());
    send_hook_with_owner(binary, socket, harness, event, transcript_path, owner_pid);
}

fn send_hook_with_owner(
    binary: &str,
    socket: &Path,
    harness: &str,
    event: &str,
    transcript_path: Option<&Path>,
    owner_pid: Option<u32>,
) {
    let mut child = Command::new(binary)
        .args(["hook", harness])
        .env("HARNESS_STATUS_SOCKET", socket)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let session_id = match harness {
        "codex" => "thread-smoke",
        "claude" => "claude-thread-smoke",
        "pi" => "pi-thread-smoke",
        _ => panic!("unsupported test harness {harness}"),
    };
    let mut input = json!({
        "hook_event_name": event,
        "session_id": session_id,
        "transcript_path": transcript_path,
        "cwd": "/tmp/example",
        "prompt": "not retained",
        "last_assistant_message": "also not retained"
    });
    if harness == "codex" {
        input["turn_id"] = json!("turn-smoke");
        input["model"] = json!("gpt-smoke");
    } else if harness == "pi" {
        input["model"] = json!("pi-model-smoke");
    }
    if let Some(owner_pid) = owner_pid {
        input["owner_pid"] = json!(owner_pid);
    }
    serde_json::to_writer(child.stdin.take().unwrap(), &input).unwrap();
    assert!(child.wait().unwrap().success());
}

fn wait_for_socket(socket: &Path) {
    wait_for_path(socket);
}

fn wait_for_path(path: &Path) {
    for _ in 0..500 {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("path did not appear: {}", path.display());
}

fn wait_for_follow_socket(dir: &Path) {
    for _ in 0..500 {
        if fs::read_dir(dir).unwrap().flatten().any(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".harness-status-follow-")
        }) {
            thread::sleep(Duration::from_millis(10));
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("subscriber did not create a follow socket");
}

fn wait_for_state(binary: &str, socket: &Path, harness: &str, state: &str) -> Value {
    for _ in 0..500 {
        let snapshot = snapshot(binary, socket, harness);
        if snapshot["threads"]
            .as_array()
            .is_some_and(|threads| threads.iter().any(|thread| thread["state"] == state))
        {
            return snapshot;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("status never reached {state}");
}

fn wait_for_thread_count(binary: &str, socket: &Path, harness: &str, count: usize) -> Value {
    for _ in 0..500 {
        let snapshot = snapshot(binary, socket, harness);
        if snapshot["threads"].as_array().map(Vec::len) == Some(count) {
            return snapshot;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("status never reached {count} threads");
}

fn snapshot(binary: &str, socket: &Path, harness: &str) -> Value {
    let output = Command::new(binary)
        .arg(harness)
        .env("HARNESS_STATUS_SOCKET", socket)
        .output()
        .unwrap();
    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).unwrap()
}

fn state_path(socket: &Path) -> PathBuf {
    let mut path = socket.to_owned();
    path.set_extension("state.json");
    path
}
