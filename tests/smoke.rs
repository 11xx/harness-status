use serde_json::{Value, json};
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
        .env("HARNESS_STATUS_NOTIFY", "0")
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let mut owner = Follower {
        child,
        dir: dir.clone(),
    };
    wait_for_socket(&socket);
    send_internal_codex_hook(binary, &socket, &dir.join("codex-home"));
    wait_for_thread_count(binary, &socket, "codex", 0);
    let state_path = state_path(&socket);
    wait_for_path(&state_path);
    assert_eq!(
        fs::metadata(&state_path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let mut subscriber = Command::new(binary)
        .args(["codex", "--follow"])
        .env("HARNESS_STATUS_SOCKET", &socket)
        .env("HARNESS_STATUS_NOTIFY", "0")
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
        .env("HARNESS_STATUS_NOTIFY", "0")
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    wait_for_state(binary, &socket, "codex", "completed");
    assert!(
        Command::new(binary)
            .args(["ack", "thread-smoke"])
            .env("HARNESS_STATUS_SOCKET", &socket)
            .env("HARNESS_STATUS_NOTIFY", "0")
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
        .env("HARNESS_STATUS_NOTIFY", "0")
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    wait_for_thread_count(binary, &socket, "codex", 0);
    final_owner.kill().unwrap();
    final_owner.wait().unwrap();

    // A running turn republishes as its duration advances, so the same state
    // legitimately repeats; what the stream must never do is lose a transition
    // or deliver one out of order.
    let mut states: Vec<Option<String>> = String::from_utf8(output.stdout)
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
    states.dedup();
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
        .env("HARNESS_STATUS_NOTIFY", "0")
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
    assert_eq!(working["threads"][0]["turn_id"], "prompt-smoke");
    let checkpoint = fs::read_to_string(state_path(&socket)).unwrap();
    assert!(!checkpoint.contains("sensitive transcript content"));
    assert!(!checkpoint.contains("not retained"));

    let mut transcript_file = fs::OpenOptions::new()
        .append(true)
        .open(&transcript)
        .unwrap();
    writeln!(
        transcript_file,
        "{}",
        json!({
            "type": "assistant",
            "promptId": "prompt-smoke",
            "message": {"role": "assistant", "model": "claude-smoke-5"}
        })
    )
    .unwrap();
    wait_for_model(binary, &socket, "claude", "claude-smoke-5");

    writeln!(
        transcript_file,
        "{}",
        json!({
            "type": "user",
            "promptId": "prompt-smoke",
            "interruptedMessageId": "msg-smoke",
            "message": {"role": "user", "content": "[Request interrupted by user]"}
        })
    )
    .unwrap();
    wait_for_thread_count(binary, &socket, "claude", 0);
    send_hook(
        binary,
        &socket,
        "claude",
        "UserPromptSubmit",
        Some(&transcript),
    );
    wait_for_state(binary, &socket, "claude", "working");

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
    wait_for_state(binary, &socket, "claude", "attention");
    send_hook_with_notification_type(
        binary,
        &socket,
        "claude",
        "Notification",
        Some("permission_prompt"),
    );
    wait_for_state(binary, &socket, "claude", "attention");
    send_hook_with_notification_type(
        binary,
        &socket,
        "claude",
        "Notification",
        Some("elicitation_complete"),
    );
    wait_for_state(binary, &socket, "claude", "working");
    send_hook(binary, &socket, "claude", "SessionEnd", Some(&transcript));
    wait_for_thread_count(binary, &socket, "claude", 0);

    send_hook(
        binary,
        &socket,
        "claude",
        "UserPromptSubmit",
        Some(&transcript),
    );
    wait_for_state(binary, &socket, "claude", "working");
    assert!(
        Command::new(binary)
            .args(["ack", "claude-thread-smoke"])
            .env("HARNESS_STATUS_SOCKET", &socket)
            .env("HARNESS_STATUS_NOTIFY", "0")
            .status()
            .unwrap()
            .success()
    );
    wait_for_thread_count(binary, &socket, "claude", 0);
}

#[test]
fn desktop_notifications_are_generic_and_transition_only() {
    let binary = env!("CARGO_BIN_EXE_harness-status");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("harness-status-notify-test-{nonce}"));
    fs::create_dir(&dir).unwrap();
    let socket = dir.join("status.sock");
    let output = dir.join("notifications");
    let notifier = dir.join("notifier");
    fs::write(
        &notifier,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\n",
            output.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&notifier, fs::Permissions::from_mode(0o700)).unwrap();

    let child = Command::new(binary)
        .args(["claude", "--follow"])
        .env("HARNESS_STATUS_SOCKET", &socket)
        .env("HARNESS_STATUS_NOTIFY_COMMAND", &notifier)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let _follower = Follower {
        child,
        dir: dir.clone(),
    };
    wait_for_socket(&socket);

    send_notification_hook(binary, &socket, "UserPromptSubmit", None);
    wait_for_state(binary, &socket, "claude", "working");
    send_notification_hook(binary, &socket, "Stop", None);
    wait_for_state(binary, &socket, "claude", "completed");
    send_notification_hook(binary, &socket, "Notification", Some("permission_prompt"));
    wait_for_state(binary, &socket, "claude", "attention");
    let lines = wait_for_lines(&output, 8);
    assert_eq!(
        lines,
        [
            "--app-name",
            "harness-status-claude",
            "Claude Code",
            "example finished",
            "--app-name",
            "harness-status-claude",
            "Claude Code",
            "example needs attention: permission"
        ]
    );
    assert!(
        !fs::read_to_string(&output)
            .unwrap()
            .contains("sensitive-secret")
    );
}

#[test]
fn notifier_failures_do_not_prevent_publication() {
    let binary = env!("CARGO_BIN_EXE_harness-status");
    for (index, command) in ["/missing/harness-status-notifier", "/bin/false"]
        .into_iter()
        .enumerate()
    {
        let dir = std::env::temp_dir().join(format!(
            "harness-status-notifier-failure-{}-{index}",
            std::process::id()
        ));
        fs::create_dir(&dir).unwrap();
        let socket = dir.join("status.sock");
        let mut child = Command::new(binary)
            .args(["codex", "--follow"])
            .env("HARNESS_STATUS_SOCKET", &socket)
            .env("HARNESS_STATUS_NOTIFY_COMMAND", command)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        wait_for_socket(&socket);
        send_notification_hook_for(binary, &socket, "codex", "Stop", None);
        wait_for_state(binary, &socket, "codex", "completed");
        child.kill().unwrap();
        child.wait().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn slow_notifier_is_killed_and_reaped_without_blocking_publication() {
    let binary = env!("CARGO_BIN_EXE_harness-status");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("harness-status-slow-notifier-{nonce}"));
    fs::create_dir(&dir).unwrap();
    let socket = dir.join("status.sock");
    let pid_file = dir.join("notifier.pid");
    let notifier = dir.join("notifier");
    fs::write(
        &notifier,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$$\" > '{}'\nexec sleep 30\n",
            pid_file.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&notifier, fs::Permissions::from_mode(0o700)).unwrap();

    let child = Command::new(binary)
        .args(["codex", "--follow"])
        .env("HARNESS_STATUS_SOCKET", &socket)
        .env("HARNESS_STATUS_NOTIFY_COMMAND", &notifier)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let _follower = Follower {
        child,
        dir: dir.clone(),
    };
    wait_for_socket(&socket);

    let started = Instant::now();
    let snapshot = send_ordered_hook_and_snapshot(&dir, &socket, "codex", "Stop", None);
    assert_eq!(snapshot["threads"][0]["state"], "completed");
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "notifier delayed state publication for {:?}",
        started.elapsed()
    );

    wait_for_path(&pid_file);
    let pid = fs::read_to_string(&pid_file).unwrap();
    let process_path = PathBuf::from(format!("/proc/{}", pid.trim()));
    wait_for_path_absent(&process_path);
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
        .env("HARNESS_STATUS_NOTIFY", "0")
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
    send_hook(binary, &socket, "pi", "Attention", None);
    wait_for_state(binary, &socket, "pi", "attention");
    send_hook(binary, &socket, "pi", "ResumeCompleted", None);
    wait_for_state(binary, &socket, "pi", "completed");
    send_hook(binary, &socket, "pi", "Attention", None);
    wait_for_state(binary, &socket, "pi", "attention");
    send_hook(binary, &socket, "pi", "ResumeWorking", None);
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

    verify_uninstall_leaves_no_empty_hooks(binary, dir);
    verify_uninstall_keeps_a_config_holding_nothing_of_ours(binary, dir);
}

/// A config that held only our handlers keeps no hollow `hooks` object once
/// they are gone.
fn verify_uninstall_leaves_no_empty_hooks(binary: &str, dir: &Path) {
    let codex_home = dir.join("codex-home-only-ours");
    fs::create_dir(&codex_home).unwrap();
    let hooks_path = codex_home.join("hooks.json");

    let install = Command::new(binary)
        .args(["install", "codex"])
        .env("CODEX_HOME", &codex_home)
        .output()
        .unwrap();
    assert!(install.status.success());

    let uninstall = Command::new(binary)
        .args(["uninstall", "codex"])
        .env("CODEX_HOME", &codex_home)
        .output()
        .unwrap();
    assert!(uninstall.status.success());
    let document: Value = serde_json::from_slice(&fs::read(&hooks_path).unwrap()).unwrap();
    assert!(document.get("hooks").is_none());
    assert_eq!(document.as_object().unwrap().len(), 0);
}

/// Uninstall reports nothing to do without rewriting a file it owns no part
/// of, including the empty `hooks` object it did not put there.
fn verify_uninstall_keeps_a_config_holding_nothing_of_ours(binary: &str, dir: &Path) {
    let codex_home = dir.join("codex-home-foreign");
    fs::create_dir(&codex_home).unwrap();
    let hooks_path = codex_home.join("hooks.json");
    let original = "{\n  \"model\": \"keep-me\",\n  \"hooks\": {}\n}\n";
    fs::write(&hooks_path, original).unwrap();

    let uninstall = Command::new(binary)
        .args(["uninstall", "codex"])
        .env("CODEX_HOME", &codex_home)
        .output()
        .unwrap();
    assert!(uninstall.status.success());
    assert_eq!(fs::read_to_string(&hooks_path).unwrap(), original);
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
          },
          {
            "type": "command",
            "command": "harness-status hook claude",
            "timeout": 5
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
        installed["hooks"]["SessionStart"][0]["hooks"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        installed["hooks"]["SessionStart"][0]["hooks"][0]["command"],
        "also-keep-me"
    );
    for event in [
        "UserPromptSubmit",
        "Stop",
        "StopFailure",
        "Notification",
        "SessionEnd",
    ] {
        let commands: Vec<&str> = installed["hooks"][event]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["hooks"].as_array().unwrap())
            .filter_map(|handler| handler["command"].as_str())
            .filter(|command| command.ends_with("' hook claude"))
            .collect();
        assert_eq!(commands.len(), 1);
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
    assert!(uninstalled["hooks"].get("Notification").is_none());
    assert!(uninstalled["hooks"].get("SessionEnd").is_none());

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
    assert!(installed.contains("session_start"));
    assert!(installed.contains("harness-status:attention"));
    assert!(installed.contains("harness-status:resume"));
    assert!(installed.contains("harness-status:background"));
    assert!(installed.contains("HARNESS_STATUS_DELEGATED"));
    assert!(installed.contains("Attention"));
    assert!(installed.contains("ResumeWorking"));
    assert!(installed.contains("ResumeCompleted"));
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

fn send_internal_codex_hook(binary: &str, socket: &Path, codex_home: &Path) {
    let mut child = Command::new(binary)
        .args(["hook", "codex"])
        .env("CODEX_HOME", codex_home)
        .env("HARNESS_STATUS_SOCKET", socket)
        .env("HARNESS_STATUS_NOTIFY", "0")
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    serde_json::to_writer(
        child.stdin.take().unwrap(),
        &json!({
            "hook_event_name": "UserPromptSubmit",
            "session_id": "internal-memory-thread",
            "turn_id": "internal-memory-turn",
            "cwd": codex_home.join("memories"),
            "model": "gpt-memory"
        }),
    )
    .unwrap();
    assert!(child.wait().unwrap().success());
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
        .env("HARNESS_STATUS_NOTIFY", "0")
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
    if harness == "claude" {
        input["prompt_id"] = json!("prompt-smoke");
    }
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

fn send_ordered_hook_and_snapshot(
    dir: &Path,
    socket: &Path,
    harness: &str,
    event: &str,
    notification_type: Option<&str>,
) -> Value {
    let client_path = dir.join("ordered-client.sock");
    let client = UnixDatagram::bind(&client_path).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let hook = json!({
        "op": "hook",
        "harness": harness,
        "event": {
            "event": event,
            "session_id": format!("{harness}-ordered-smoke"),
            "turn_id": null,
            "transcript": null,
            "cwd": "/tmp/example",
            "model": "",
            "notification_type": notification_type
        },
        "owner": null
    });
    client
        .send_to(&serde_json::to_vec(&hook).unwrap(), socket)
        .unwrap();
    let request = json!({"op": "snapshot", "harness": harness});
    client
        .send_to(&serde_json::to_vec(&request).unwrap(), socket)
        .unwrap();
    let mut buffer = vec![0_u8; 64 * 1024];
    let length = client.recv(&mut buffer).unwrap();
    serde_json::from_slice(&buffer[..length]).unwrap()
}

fn send_hook_with_notification_type(
    binary: &str,
    socket: &Path,
    harness: &str,
    event: &str,
    notification_type: Option<&str>,
) {
    let mut child = Command::new(binary)
        .args(["hook", harness])
        .env("HARNESS_STATUS_SOCKET", socket)
        .env("HARNESS_STATUS_NOTIFY", "0")
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    serde_json::to_writer(
        child.stdin.take().unwrap(),
        &json!({
            "hook_event_name": event,
            "session_id": "claude-thread-smoke",
            "cwd": "/tmp/example",
            "notification_type": notification_type
        }),
    )
    .unwrap();
    assert!(child.wait().unwrap().success());
}

fn send_notification_hook(
    binary: &str,
    socket: &Path,
    event: &str,
    notification_type: Option<&str>,
) {
    send_notification_hook_for(binary, socket, "claude", event, notification_type);
}

fn send_notification_hook_for(
    binary: &str,
    socket: &Path,
    harness: &str,
    event: &str,
    notification_type: Option<&str>,
) {
    let mut child = Command::new(binary)
        .args(["hook", harness])
        .env("HARNESS_STATUS_SOCKET", socket)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    serde_json::to_writer(
        child.stdin.take().unwrap(),
        &json!({
            "hook_event_name": event,
            "session_id": format!("{harness}-notification-smoke"),
            "cwd": "/tmp/example",
            "owner_pid": std::process::id(),
            "notification_type": notification_type,
            "prompt": "sensitive-secret"
        }),
    )
    .unwrap();
    assert!(child.wait().unwrap().success());
}

fn wait_for_lines(path: &Path, count: usize) -> Vec<String> {
    for _ in 0..500 {
        if let Ok(contents) = fs::read_to_string(path) {
            let lines: Vec<String> = contents.lines().map(str::to_owned).collect();
            if lines.len() >= count {
                return lines;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("file never reached {count} lines: {}", path.display());
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

fn wait_for_path_absent(path: &Path) {
    for _ in 0..300 {
        if !path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("path did not disappear: {}", path.display());
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

fn wait_for_model(binary: &str, socket: &Path, harness: &str, model: &str) -> Value {
    for _ in 0..500 {
        let snapshot = snapshot(binary, socket, harness);
        if snapshot["threads"]
            .as_array()
            .is_some_and(|threads| threads.iter().any(|thread| thread["model"] == model))
        {
            return snapshot;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("status never reported model {model}");
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
        .env("HARNESS_STATUS_NOTIFY", "0")
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
