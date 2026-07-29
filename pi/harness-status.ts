/**
 * Pi lifecycle adapter for harness-status.
 *
 * Copy this file to ~/.pi/agent/extensions/ for a PATH-based manual install,
 * or run `harness-status install pi` to install it with an absolute binary path.
 */

import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { spawn } from "node:child_process";

const HARNESS_STATUS = "harness-status";

type PiStatusEvent = "AgentStart" | "AgentSettled" | "Attention" | "ResumeWorking" | "ResumeCompleted" | "SessionShutdown";

function report(event: PiStatusEvent, ctx: ExtensionContext): Promise<void> {
	return new Promise((resolve) => {
		const child = spawn(HARNESS_STATUS, ["hook", "pi"], {
			stdio: ["pipe", "ignore", "ignore"],
		});
		let finished = false;
		const finish = () => {
			if (finished) return;
			finished = true;
			clearTimeout(timeout);
			resolve();
		};
		const timeout = setTimeout(() => {
			child.kill();
			finish();
		}, 1000);

		child.once("error", finish);
		child.once("close", finish);
		child.stdin.on("error", () => {});
		child.stdin.end(
			JSON.stringify({
				hook_event_name: event,
				session_id: ctx.sessionManager.getSessionId(),
				cwd: ctx.cwd,
				model: ctx.model?.id ?? "",
				owner_pid: process.pid,
			}),
		);
	});
}

export default function (pi: ExtensionAPI) {
	let context: ExtensionContext | undefined;

	pi.on("session_start", (_event, ctx) => {
		context = ctx;
	});
	pi.on("agent_start", (_event, ctx) => report("AgentStart", ctx));
	pi.on("agent_settled", (_event, ctx) => report("AgentSettled", ctx));
	pi.events.on("harness-status:attention", () => {
		if (context) void report("Attention", context);
	});
	pi.events.on("harness-status:resume", (data) => {
		if (!context) return;
		const state = (data as { state?: unknown } | undefined)?.state;
		if (state === "working") void report("ResumeWorking", context);
		else if (state === "completed") void report("ResumeCompleted", context);
	});
	pi.on("session_shutdown", async (_event, ctx) => {
		await report("SessionShutdown", ctx);
		context = undefined;
	});
}
