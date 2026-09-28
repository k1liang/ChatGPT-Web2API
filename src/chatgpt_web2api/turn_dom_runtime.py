"""Async consumer for the event-driven DOM completion secondary."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

_MAX_QUEUE = 128


@dataclass
class DomTurnResult:
    verdict: str
    assistant_text: str = ""
    conversation_id: str | None = None
    assistant_message_id: str | None = None
    turn_id: str | None = None
    diagnostic: dict = field(default_factory=dict)

    @property
    def matched(self) -> bool:
        return self.verdict == "matched"


class TurnDomRuntime:
    def __init__(self, driver) -> None:
        self._driver = driver
        self._hook = None
        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=_MAX_QUEUE)
        self._consumer_task: asyncio.Task | None = None
        self._terminal_by_turn: dict[str, dict] = {}
        self._changed = asyncio.Event()
        self._signal_seq = 0
        self.queue_overflow_count = 0
        self.bad_event_count = 0
        self.event_count = 0
        self.turns_started = 0
        self.turns_matched = 0
        self.turns_failed = 0

    async def start(self, hook) -> None:
        if self._hook is not hook:
            if self._hook is not None:
                self._hook.remove_event_listener(self._on_event)
            self._hook = hook
            hook.add_event_listener(self._on_event)
        if self._consumer_task is None or self._consumer_task.done():
            self._consumer_task = asyncio.create_task(self._consume())

    async def stop(self) -> None:
        if self._hook is not None:
            self._hook.remove_event_listener(self._on_event)
            try:
                await self._hook.disarm()
            except Exception:
                pass
        if self._consumer_task is not None and not self._consumer_task.done():
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        self._consumer_task = None
        self.reset_session()

    def reset_session(self) -> None:
        self._terminal_by_turn.clear()
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self._signal_seq += 1
        self._changed.set()

    def _on_event(self, event: dict) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self.queue_overflow_count += 1
            self._signal_seq += 1
            self._changed.set()

    async def _consume(self) -> None:
        while True:
            event = await self._queue.get()
            try:
                self._apply_event(event)
            finally:
                self._signal_seq += 1
                self._changed.set()

    def _apply_event(self, event: dict) -> None:
        if not isinstance(event, dict):
            self.bad_event_count += 1
            return
        self.event_count += 1
        kind = event.get("t")
        turn_id = event.get("turn_id")
        if not isinstance(turn_id, str) or not turn_id:
            self.bad_event_count += 1
            return
        if kind == "terminal":
            self._terminal_by_turn[turn_id] = dict(event)
            if len(self._terminal_by_turn) > 16:
                self._terminal_by_turn.pop(next(iter(self._terminal_by_turn)))
        elif kind == "hook_error":
            self._terminal_by_turn[turn_id] = dict(event)
        else:
            self.bad_event_count += 1

    async def wait_for_turn(self, turn_id: str, *, timeout: float) -> DomTurnResult:
        if not turn_id or self._hook is None:
            return DomTurnResult(verdict="no_user_anchor", turn_id=turn_id or None)
        self.turns_started += 1
        overflow_at_start = self.queue_overflow_count
        if not await self._hook.arm(turn_id):
            self.turns_failed += 1
            return DomTurnResult(verdict="arm_failed", turn_id=turn_id)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        try:
            while True:
                observed_seq = self._signal_seq
                if self.queue_overflow_count > overflow_at_start:
                    self.turns_failed += 1
                    return DomTurnResult(
                        verdict="queue_overflow", turn_id=turn_id,
                        diagnostic=self._diagnostic(turn_id),
                    )
                event = self._terminal_by_turn.pop(turn_id, None)
                if event is not None:
                    if event.get("t") == "hook_error":
                        self.turns_failed += 1
                        return DomTurnResult(
                            verdict="hook_error", turn_id=turn_id,
                            conversation_id=event.get("conversation_id"),
                            diagnostic={"error": event.get("error")},
                        )
                    result = self._result_from_terminal(turn_id, event)
                    if result.matched:
                        self.turns_matched += 1
                    else:
                        self.turns_failed += 1
                    return result
                remaining = deadline - loop.time()
                if remaining <= 0:
                    self.turns_failed += 1
                    return DomTurnResult(
                        verdict="no_terminal", turn_id=turn_id,
                        diagnostic=self._diagnostic(turn_id),
                    )
                self._changed.clear()
                if self._signal_seq != observed_seq:
                    continue
                try:
                    await asyncio.wait_for(self._changed.wait(), timeout=remaining)
                except TimeoutError:
                    self.turns_failed += 1
                    return DomTurnResult(
                        verdict="no_terminal", turn_id=turn_id,
                        diagnostic=self._diagnostic(turn_id),
                    )
        finally:
            try:
                await self._hook.disarm()
            except Exception:
                pass
            self._terminal_by_turn.pop(turn_id, None)

    def _result_from_terminal(self, turn_id: str, event: dict) -> DomTurnResult:
        common = {
            "turn_id": turn_id,
            "conversation_id": event.get("conversation_id"),
            "assistant_message_id": event.get("assistant_message_id"),
            "diagnostic": {
                "assistant_key": event.get("assistant_key"),
                "html_len": event.get("html_len"),
                "text_truncated": bool(event.get("text_truncated")),
                "non_text": bool(event.get("non_text")),
            },
        }
        if event.get("text_truncated"):
            return DomTurnResult(verdict="text_truncated", **common)
        text = event.get("text")
        if isinstance(text, str) and text.strip():
            return DomTurnResult(verdict="matched", assistant_text=text, **common)
        if event.get("non_text"):
            return DomTurnResult(verdict="non_text", **common)
        return DomTurnResult(verdict="empty_terminal", **common)

    def _diagnostic(self, turn_id: str) -> dict:
        return {
            "turn_id": turn_id,
            "events": self.event_count,
            "bad_events": self.bad_event_count,
            "queue_overflows": self.queue_overflow_count,
        }

    @property
    def stats(self) -> dict:
        return {
            "running": self._consumer_task is not None and not self._consumer_task.done(),
            "events": self.event_count,
            "bad_events": self.bad_event_count,
            "queue_overflows": self.queue_overflow_count,
            "pending_turns": len(self._terminal_by_turn),
            "turns_started": self.turns_started,
            "turns_matched": self.turns_matched,
            "turns_failed": self.turns_failed,
        }
