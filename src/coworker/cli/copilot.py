import json
import shutil
import subprocess
import time
from collections.abc import Callable
from queue import Empty, Queue
from threading import Thread
from typing import Any


class CopilotError(RuntimeError):
    pass


class Copilot:
    def ask_copilot(
        self,
        prompt: str,
        model: str | None = None,
        timeout: int = 180,
        stream: bool = False,
        on_event: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[dict[str, Any]]:
        """Run Copilot CLI in prompt mode and return its JSONL events."""
        copilot_exe = shutil.which("copilot")
        if not copilot_exe:
            raise CopilotError(
                "Copilot CLI executable not found. Please ensure it is installed and in your PATH."
            )

        command = [
            copilot_exe,
            "-p",
            prompt,
            "--model",
            model or "auto",
            "--allow-all-tools",
            "--output-format",
            "json",
            "--stream",
            "on" if stream else "off",
        ]
        if stream:
            return self._ask_copilot_streaming(command, timeout, on_event)

        process = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
        )
        if process.returncode != 0:
            raise CopilotError(
                f"Copilot command failed with return code {process.returncode}: {process.stderr}"
            )

        return self._parse_events(process.stdout)

    def _ask_copilot_streaming(
        self,
        command: list[str],
        timeout: int,
        on_event: Callable[[dict[str, Any]], None] | None,
    ) -> list[dict[str, Any]]:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        if process.stdout is None or process.stderr is None:
            process.kill()
            process.wait()
            raise CopilotError("Failed to open Copilot process streams.")

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

        events: list[dict[str, Any]] = []
        deadline = time.monotonic() + timeout
        try:
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

                event = self._parse_event(line)
                events.append(event)
                if on_event is not None:
                    on_event(event)

            returncode = process.wait(timeout=max(0, deadline - time.monotonic()))
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            stdout_thread.join()
            stderr_thread.join()

        if returncode != 0:
            raise CopilotError(
                f"Copilot command failed with return code {returncode}: {''.join(stderr_lines)}"
            )
        if not events:
            raise CopilotError("Copilot CLI returned no JSON events.")
        return events

    def _parse_events(self, output: str) -> list[dict[str, Any]]:
        events = [
            self._parse_event(line) for line in output.splitlines() if line.strip()
        ]
        if not events:
            raise CopilotError("Copilot CLI returned no JSON events.")
        return events

    @staticmethod
    def _parse_event(line: str) -> dict[str, Any]:
        try:
            event = json.loads(line)
        except json.JSONDecodeError as e:
            raise CopilotError(f"Failed to parse Copilot JSON event: {e}") from e
        if not isinstance(event, dict):
            raise CopilotError("Copilot JSON event must be an object.")
        return event


# if __name__ == "__main__":
#     copilot = Copilot()
#     prompt = "Write a Python function to calculate the factorial of a number."
#     try:
#         events = copilot.ask_copilot(prompt, stream=True, on_event=lambda event: print("Event:", event))
#         print("Copilot's events:", events)
#     except CopilotError as e:
#         print("Error:", e)