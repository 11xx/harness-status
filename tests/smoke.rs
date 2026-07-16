use serde_json::{Value, json};
use std::fs;
use std::io::Write;
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
    let _follower = Follower {
        child,
        dir: dir.clone(),
    };
    wait_for_socket(&socket);

    let mut subscriber = Command::new(binary)
        .args(["codex", "--follow"])
        .env("HARNESS_STATUS_SOCKET", &socket)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_follow_socket(&dir);

    let transcript = dir.join("transcript.jsonl");
    fs::write(&transcript, "{\"type\":\"session_meta\"}\n").unwrap();
    send_hook(binary, &socket, "UserPromptSubmit", Some(&transcript));
    wait_for_state(binary, &socket, "working");
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
    wait_for_thread_count(binary, &socket, 0);

    send_hook(binary, &socket, "UserPromptSubmit", None);
    let working = wait_for_state(binary, &socket, "working");
    assert_eq!(working["threads"][0]["id"], "thread-smoke");
    assert_eq!(working["threads"][0]["name"], "example");

    send_hook(binary, &socket, "Stop", None);
    wait_for_state(binary, &socket, "completed");

    assert!(
        Command::new(binary)
            .args(["ack", "thread-smoke"])
            .env("HARNESS_STATUS_SOCKET", &socket)
            .status()
            .unwrap()
            .success()
    );
    let empty = wait_for_thread_count(binary, &socket, 0);
    assert_eq!(empty["threads"].as_array().unwrap().len(), 0);

    subscriber.kill().unwrap();
    let output = subscriber.wait_with_output().unwrap();
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
            Some("completed".to_owned()),
            None
        ]
    );
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

fn send_hook(binary: &str, socket: &Path, event: &str, transcript_path: Option<&Path>) {
    let mut child = Command::new(binary)
        .args(["hook", "codex"])
        .env("HARNESS_STATUS_SOCKET", socket)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    serde_json::to_writer(
        child.stdin.take().unwrap(),
        &json!({
            "hook_event_name": event,
            "session_id": "thread-smoke",
            "turn_id": "turn-smoke",
            "transcript_path": transcript_path,
            "cwd": "/tmp/example",
            "model": "gpt-smoke",
            "prompt": "not retained"
        }),
    )
    .unwrap();
    assert!(child.wait().unwrap().success());
}

fn wait_for_socket(socket: &Path) {
    for _ in 0..100 {
        if socket.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("follower did not create {}", socket.display());
}

fn wait_for_follow_socket(dir: &Path) {
    for _ in 0..100 {
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

fn wait_for_state(binary: &str, socket: &Path, state: &str) -> Value {
    for _ in 0..100 {
        let snapshot = snapshot(binary, socket);
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

fn wait_for_thread_count(binary: &str, socket: &Path, count: usize) -> Value {
    for _ in 0..100 {
        let snapshot = snapshot(binary, socket);
        if snapshot["threads"].as_array().map(Vec::len) == Some(count) {
            return snapshot;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("status never reached {count} threads");
}

fn snapshot(binary: &str, socket: &Path) -> Value {
    let output = Command::new(binary)
        .arg("codex")
        .env("HARNESS_STATUS_SOCKET", socket)
        .output()
        .unwrap();
    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).unwrap()
}
