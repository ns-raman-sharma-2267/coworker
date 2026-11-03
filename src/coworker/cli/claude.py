import json
import os
import shutil
import subprocess
import time
from collections.abc import Callable
from queue import Empty, Queue
from threading import Thread
from typing import Any

from coworker.config import CFG


class ClaudeError(RuntimeError):
    pass


class Claude:
    def ask_claude(
        self,
        prompt: str,
        model: str | None = None,
        allowed_tools=("Read", "Grep", "Glob"),
        timeout=180,
        stream=False,
        on_event: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        claude_exe = shutil.which("claude")

        if not claude_exe:
            raise ClaudeError(
                "Claude executable not found. Please ensure it is installed and in your PATH."
            )

        command = [claude_exe, "-p", "--model", model or CFG["claude"]["model"]]

        command.extend(["--output-format", "stream-json" if stream else "json"])

        if allowed_tools:
            command.append("--allowed-tools")
            command.append(",".join(allowed_tools))

        if stream:
            command.append("--include-partial-messages")
            command.append("--verbose")

        env = {k: v for k, v in os.environ.items() if k != "ANTHROPIC_API_KEY"}

        if stream:
            return self._ask_claude_streaming(command, prompt, timeout, env, on_event)

        process = subprocess.run(
            command,
            input=prompt,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
            env=env,
        )

        if process.returncode != 0:
            raise ClaudeError(
                f"Claude command failed with return code {process.returncode}: {process.stderr}"
            )

        try:
            response = json.loads(process.stdout)
        except json.JSONDecodeError as e:
            raise ClaudeError(f"Failed to parse Claude response as JSON: {e}")

        return response

    def _ask_claude_streaming(
        self,
        command: list[str],
        prompt: str,
        timeout: int,
        env: dict[str, str],
        on_event: Callable[[dict[str, Any]], None] | None,
    ) -> dict[str, Any]:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            env=env,
            bufsize=1,
        )
        if process.stdin is None or process.stdout is None or process.stderr is None:
            process.kill()
            process.wait()
            raise ClaudeError("Failed to open Claude process streams.")

        stdout_queue: Queue[str | None] = Queue()
        stderr_lines: list[str] = []

        def read_stdout() -> None:
            try:
                for line in process.stdout:
                    stdout_queue.put(line)
            finally:
                stdout_queue.put(None)

        stdout_thread = Thread(target=read_stdout, daemon=True)
        stderr_thread = Thread(
            target=lambda: stderr_lines.extend(process.stderr), daemon=True
        )
        stdout_thread.start()
        stderr_thread.start()

        deadline = time.monotonic() + timeout
        response: dict[str, Any] | None = None
        try:
            process.stdin.write(prompt)
            process.stdin.close()

            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, timeout)
                try:
                    line = stdout_queue.get(timeout=remaining)
                except Empty as e:
                    raise subprocess.TimeoutExpired(command, timeout) from e
                if line is None:
                    break

                try:
                    event = json.loads(line)
                except json.JSONDecodeError as e:
                    raise ClaudeError(
                        f"Failed to parse Claude stream event as JSON: {e}"
                    ) from e
                if not isinstance(event, dict):
                    raise ClaudeError("Claude stream event must be a JSON object.")

                if on_event is not None:
                    on_event(event)
                if event.get("type") == "result":
                    response = event

            returncode = process.wait(timeout=max(0, deadline - time.monotonic()))
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            stdout_thread.join()
            stderr_thread.join()

        if returncode != 0:
            raise ClaudeError(
                f"Claude command failed with return code {returncode}: {''.join(stderr_lines)}"
            )
        if response is None:
            raise ClaudeError("Claude stream ended without a result event.")

        return response

def addEventsInFile(event):
    with open("events.log", "a") as f:
        f.write(f"Event: {event}\n")

# if __name__ == "__main__":
#     claude = Claude()
#     prompt = "Say hello in one sentence."
#     try:
#         response = claude.ask_claude(prompt, allowed_tools=(), stream=True, on_event=lambda event: addEventsInFile(event))
#         print("Claude's response:", response)
#     except ClaudeError as e:
#         print("Error:", e)
