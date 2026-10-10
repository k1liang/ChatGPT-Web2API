"""Production event-driven turn stream runtime.

Owns the live logical-turn lifecycle on top of PageStreamHook events:
- CDP handler only enqueues decoded events (no SSE/JSON-Patch work there);
- one async consumer incrementally feeds SendStreamParser per attempt;
- current-turn ownership is exact IdentityListener UUID correlation;
- no raw response bytes or prompt body are retained in production.

The first production version deliberately emits only after terminal.  Offline
prefix-check found a real S6 counterexample where an intermediate assistant
candidate was not a prefix of the terminal answer, so token-level emit is not
safe yet.
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import time
from dataclasses import dataclass, field

from .response_stream import (
    SendStreamParser,
    TurnParserCandidate,
    select_from_parser_candidates,
)

_MAX_ATTEMPTS = 32
_MAX_EVENT_QUEUE = 512
_FAIL_CLOSED_OUTCOMES = {
    "text_overflow", "unrouted_delta", "line_overflow",
    "invalid_frame", "invalid_utf8", "orphan_op",
}
@dataclass(frozen=True)
class TurnStreamContext:
    """Snapshot taken immediately before one user send."""

    event_seq: int
    queue_overflows: int
    started_at: float


@dataclass
class TurnStreamResult:
    verdict: str
    assistant_text: str = ""
    conversation_id: str | None = None
    user_message_id: str | None = None
    assistant_message_id: str | None = None
    attempt_id: str | None = None
    diagnostic: dict = field(default_factory=dict)

    @property
    def matched(self) -> bool:
        return self.verdict == "matched"


@dataclass
class _AttemptState:
    attempt_id: str
    first_event_seq: int
    request_user_message_id: str | None = None
    response_status: int | None = None
    content_type: str | None = None
    parser: SendStreamParser | None = None
    transport_error: str | None = None
    bytes_seen: int = 0
class TurnStreamRuntime:
    """Incremental production consumer for page-stream binding events."""

    def __init__(self, driver) -> None:
        self._driver = driver
        self._hook = None
        self._queue: asyncio.Queue[tuple[int, dict]] = asyncio.Queue(
            maxsize=_MAX_EVENT_QUEUE
        )
        self._consumer_task: asyncio.Task | None = None
        self._attempts: dict[str, _AttemptState] = {}
        self._order: list[str] = []
        self._changed = asyncio.Event()
        self._event_seq = 0
        # Monotonic state-change generation closes the asyncio.Event
        # check/clear/wait race: a result that arrives between evaluation and
        # clear must force an immediate re-evaluation instead of sleeping.
        self._signal_seq = 0
        self.event_count = 0
        self.chunk_count = 0
        self.bytes_seen = 0
        self.queue_overflow_count = 0
        self.bad_event_count = 0
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
            self._hook = None
        task = self._consumer_task
        self._consumer_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.reset_session()

    def reset_session(self) -> None:
        self._attempts.clear()
        self._order.clear()
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self._signal_seq += 1
        self._changed.set()

    def begin_turn(self) -> TurnStreamContext:
        self.turns_started += 1
        return TurnStreamContext(
            event_seq=self._event_seq,
            queue_overflows=self.queue_overflow_count,
            started_at=time.monotonic(),
        )

    def _on_event(self, event: dict) -> None:
        """Fast CDP callback: enqueue only; never parse response bytes here."""
        self._event_seq += 1
        seq = self._event_seq
        try:
            self._queue.put_nowait((seq, event))
        except asyncio.QueueFull:
            self.queue_overflow_count += 1
            self._signal_seq += 1
            self._changed.set()

    async def _consume(self) -> None:
        while True:
            seq, event = await self._queue.get()
            try:
                self._apply_event(seq, event)
            except Exception:
                self.bad_event_count += 1
            finally:
                self._signal_seq += 1
                self._changed.set()

    def _remember(self, state: _AttemptState) -> None:
        self._attempts[state.attempt_id] = state
        self._order.append(state.attempt_id)
        while len(self._order) > _MAX_ATTEMPTS:
            old = self._order.pop(0)
            self._attempts.pop(old, None)

    def _apply_event(self, seq: int, event: dict) -> None:
        if not isinstance(event, dict):
            self.bad_event_count += 1
            return
        kind = event.get("t")
        aid = event.get("aid")
        if not isinstance(aid, str) or not aid:
            self.bad_event_count += 1
            return
        self.event_count += 1
        state = self._attempts.get(aid)

        if kind == "fetch_seen":
            anchor = event.get("req_user_msg_id")
            state = _AttemptState(
                attempt_id=aid,
                first_event_seq=seq,
                request_user_message_id=(
                    anchor if isinstance(anchor, str) and anchor else None
                ),
            )
            self._remember(state)
            return
        if state is None:
            self.bad_event_count += 1
            return
        if kind == "response":
            state.response_status = event.get("status")
            state.content_type = str(event.get("content_type") or "")
            state.parser = SendStreamParser(content_type=state.content_type)
            return
        if kind == "chunk":
            self.chunk_count += 1
            if state.parser is None:
                state.transport_error = "chunk_before_response"
                return
            encoded = event.get("b64")
            if not isinstance(encoded, str):
                state.transport_error = "missing_chunk_body"
                state.parser.mark_error(state.transport_error)
                return
            try:
                data = base64.b64decode(encoded, validate=True)
            except (binascii.Error, ValueError):
                state.transport_error = "invalid_base64"
                state.parser.mark_error(state.transport_error)
                return
            declared_len = event.get("len")
            if (
                isinstance(declared_len, int)
                and not isinstance(declared_len, bool)
                and declared_len != len(data)
            ):
                state.transport_error = "chunk_length_mismatch"
                state.parser.mark_error(state.transport_error)
                return
            state.bytes_seen += len(data)
            self.bytes_seen += len(data)
            # prepare/finalize application/json bodies are irrelevant to
            # textual completion. Feeding them would make SendStreamParser keep
            # up to 64 KiB of raw non-SSE bytes, violating production's zero
            # raw-retention contract.
            if state.parser.is_event_stream:
                state.parser.feed(data)
            return
        if kind == "eof":
            if state.parser is not None:
                state.parser.mark_eof()
            else:
                state.transport_error = "eof_before_response"
            return
        if kind in ("no_reader", "copy_error", "hook_error"):
            state.transport_error = (
                f"{kind}: {event.get('error') or event.get('status')}"
            )
            if state.parser is not None:
                state.parser.mark_error(state.transport_error)
            return
        self.bad_event_count += 1

    def _states_for_turn(
        self, expected_user_message_id: str, context: TurnStreamContext
    ) -> list[_AttemptState]:
        out = []
        for aid in self._order:
            state = self._attempts.get(aid)
            if state is None or state.first_event_seq <= context.event_seq:
                continue
            parser_user_id = (
                state.parser.user_message_id if state.parser is not None else None
            )
            if (
                state.request_user_message_id == expected_user_message_id
                or parser_user_id == expected_user_message_id
            ):
                out.append(state)
        return out

    def _selection(
        self, states: list[_AttemptState], expected_user_message_id: str
    ):
        candidates = [
            TurnParserCandidate(
                attempt_id=s.attempt_id,
                parser=s.parser,
                request_user_message_id=s.request_user_message_id,
                attempt=s,
            )
            for s in states
            if s.parser is not None
        ]
        return select_from_parser_candidates(
            candidates, expected_user_message_id=expected_user_message_id
        )

    @staticmethod
    def _failure_state(
        states: list[_AttemptState],
    ) -> tuple[str, _AttemptState] | None:
        """Return a completed fail-close attempt; aborted attempts may retry."""
        for state in reversed(states):
            parser = state.parser
            if parser is None:
                continue
            outcome = parser.outcome()
            summary = parser.summary()
            if outcome in _FAIL_CLOSED_OUTCOMES and (
                parser.saw_done
                or parser.saw_message_stream_complete
                or summary["eof"]
                or summary["error"] is not None
            ):
                return outcome, state
        return None

    async def wait_for_turn(
        self,
        expected_user_message_id: str,
        *,
        timeout: float,
        context: TurnStreamContext,
    ) -> TurnStreamResult:
        if not expected_user_message_id:
            return TurnStreamResult(verdict="no_user_anchor")
        deadline = time.monotonic() + timeout
        while True:
            observed_signal_seq = self._signal_seq
            if self.queue_overflow_count > context.queue_overflows:
                self.turns_failed += 1
                result = TurnStreamResult(
                    verdict="queue_overflow",
                    diagnostic=self._diagnostic([], context),
                )
                self._drop_turn(expected_user_message_id)
                return result
            states = self._states_for_turn(expected_user_message_id, context)
            selection = self._selection(states, expected_user_message_id)
            if selection.matched and selection.parser is not None:
                parser = selection.parser
                self.turns_matched += 1
                result = TurnStreamResult(
                    verdict="matched",
                    assistant_text=parser.assistant_text,
                    conversation_id=parser.conversation_id,
                    user_message_id=parser.user_message_id,
                    assistant_message_id=parser.assistant_message_id,
                    attempt_id=getattr(selection.attempt, "attempt_id", None),
                    diagnostic=self._diagnostic(states, context),
                )
                self._drop_turn(expected_user_message_id)
                return result
            failure = self._failure_state(states)
            if failure is not None:
                failure_outcome, failure_state = failure
                parser = failure_state.parser
                self.turns_failed += 1
                result = TurnStreamResult(
                    verdict=failure_outcome,
                    conversation_id=parser.conversation_id if parser else None,
                    user_message_id=parser.user_message_id if parser else None,
                    assistant_message_id=parser.assistant_message_id if parser else None,
                    attempt_id=failure_state.attempt_id,
                    diagnostic=self._diagnostic(states, context),
                )
                self._drop_turn(expected_user_message_id)
                return result

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.turns_failed += 1
                result = TurnStreamResult(
                    verdict="no_terminal",
                    diagnostic=self._diagnostic(states, context),
                )
                self._drop_turn(expected_user_message_id)
                return result
            self._changed.clear()
            if self._signal_seq != observed_signal_seq:
                continue
            try:
                await asyncio.wait_for(self._changed.wait(), timeout=remaining)
            except TimeoutError:
                self.turns_failed += 1
                result = TurnStreamResult(
                    verdict="no_terminal",
                    diagnostic=self._diagnostic(states, context),
                )
                self._drop_turn(expected_user_message_id)
                return result

    def discard_turn(self, user_message_id: str) -> None:
        """Drop buffered attempts for a cancelled/externally-resolved turn."""
        self._drop_turn(user_message_id)

    def _drop_turn(self, user_message_id: str) -> None:
        remove = set()
        for aid, state in self._attempts.items():
            parser_id = (
                state.parser.user_message_id if state.parser is not None else None
            )
            if (
                state.request_user_message_id == user_message_id
                or parser_id == user_message_id
            ):
                remove.add(aid)
        if remove:
            self._order = [aid for aid in self._order if aid not in remove]
            for aid in remove:
                self._attempts.pop(aid, None)

    def _diagnostic(
        self, states: list[_AttemptState], context: TurnStreamContext
    ) -> dict:
        return {
            "attempt_count": len(states),
            "retry_count": max(0, len(states) - 1),
            "attempts": [
                {
                    "attempt_id": s.attempt_id,
                    "request_user_message_id": s.request_user_message_id,
                    "response_status": s.response_status,
                    "content_type": s.content_type,
                    "outcome": s.parser.outcome() if s.parser else None,
                    "protocol_drift": (
                        s.parser.protocol_drift if s.parser else False
                    ),
                    "bytes_seen": s.bytes_seen,
                    "transport_error": s.transport_error,
                }
                for s in states
            ],
            "queue_overflows_since_turn": (
                self.queue_overflow_count - context.queue_overflows
            ),
            "raw_bytes_retained": 0,
        }

    @property
    def stats(self) -> dict:
        return {
            "running": self._consumer_task is not None
            and not self._consumer_task.done(),
            "events": self.event_count,
            "chunks": self.chunk_count,
            "bytes_seen": self.bytes_seen,
            "queue_overflows": self.queue_overflow_count,
            "bad_events": self.bad_event_count,
            "attempts_live": len(self._attempts),
            "turns_started": self.turns_started,
            "turns_matched": self.turns_matched,
            "turns_failed": self.turns_failed,
            "raw_bytes_retained": 0,
        }
