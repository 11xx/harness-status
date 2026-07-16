use serde_json::Value;
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

    send_hook(binary, &socket, "UserPromptSubmit");
    let working = wait_for_state(binary, &socket, "working");
    assert_eq!(working["threads"][0]["id"], "thread-smoke");
    assert_eq!(working["threads"][0]["name"], "example");

    send_hook(binary, &socket, "Stop");
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
}

fn send_hook(binary: &str, socket: &Path, event: &str) {
    let mut child = Command::new(binary)
        .args(["hook", "codex"])
        .env("HARNESS_STATUS_SOCKET", socket)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    write!(
        child.stdin.take().unwrap(),
        "{{\"hook_event_name\":\"{event}\",\"session_id\":\"thread-smoke\",\"turn_id\":\"turn-smoke\",\"cwd\":\"/tmp/example\",\"model\":\"gpt-smoke\",\"prompt\":\"not retained\"}}"
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
