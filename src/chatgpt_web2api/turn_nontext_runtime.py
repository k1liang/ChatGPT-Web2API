"""Production exact-turn non-text/image runtime.

Consumes passive WebSocket frames already exposed by CDP Network events.  The
runtime keeps only parsed structural fields; raw WebSocket payloads are never
retained.  Ownership is the IdentityListener-captured user UUID.  A successful
image turn requires the A2.21 contract:

- exactly one qualifying final tool asset message parented by the user UUID;
- a qualifying assistant end-turn message in the same conversation/turn exchange;
- every image_asset_pointer resolves through the authenticated file metadata
  endpoint and its returned bytes match the declared size.

DOM is intentionally not part of the production contract.  A2.21 used DOM only
as an independent validation oracle.
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import time
from dataclasses import dataclass, field
from urllib.parse import quote, urlencode

_WS_METHOD = "Network.webSocketFrameReceived"
_MAX_QUEUE = 256
_MAX_LIVE_MESSAGES = 64
_MAX_ASSET_BYTES = 20 * 1024 * 1024


@dataclass(frozen=True)
class ImageAssetPointer:
    file_id: str
    asset_pointer: str
    mime_type: str
    size_bytes: int
    width: int
    height: int

    def to_dict(self) -> dict:
        return {
            "file_id": self.file_id,
            "asset_pointer": self.asset_pointer,
            "mime_type": self.mime_type,
            "size_bytes": self.size_bytes,
            "width": self.width,
            "height": self.height,
        }


@dataclass(frozen=True)
class AssetMessage:
    message_id: str
    conversation_id: str
    parent_id: str
    turn_exchange_id: str
    working_turn_id: str
    pointers: tuple[ImageAssetPointer, ...]


@dataclass(frozen=True)
class EndTurnMessage:
    message_id: str
    conversation_id: str
    turn_exchange_id: str
    working_turn_id: str


@dataclass(frozen=True)
class NonTextTurnContext:
    event_seq: int
    queue_overflows: int
    started_at: float


@dataclass
class NonTextTurnResult:
    verdict: str
    conversation_id: str | None = None
    user_message_id: str | None = None
    asset: AssetMessage | None = None
    terminal: EndTurnMessage | None = None
    diagnostic: dict = field(default_factory=dict)

    @property
    def matched(self) -> bool:
        return self.verdict == "matched"


@dataclass
class ResolvedNonTextResult:
    verdict: str
    payload: dict | None = None
    diagnostic: dict = field(default_factory=dict)

    @property
    def matched(self) -> bool:
        return self.verdict == "matched" and self.payload is not None


@dataclass(frozen=True)
class _ObservedAsset:
    seq: int
    asset: AssetMessage


@dataclass(frozen=True)
class _ObservedTerminal:
    seq: int
    terminal: EndTurnMessage


def _walk_dicts(obj):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _walk_dicts(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _walk_dicts(value)


def _message_dicts(obj):
    seen: set[int] = set()
    for node in _walk_dicts(obj):
        mid = node.get("id")
        author = node.get("author")
        if not isinstance(mid, str) or not mid or not isinstance(author, dict):
            continue
        ident = id(node)
        if ident in seen:
            continue
        seen.add(ident)
        yield node


def _parse_pointer(part: dict) -> ImageAssetPointer | None:
    if not isinstance(part, dict) or part.get("content_type") != "image_asset_pointer":
        return None
    asset_pointer = part.get("asset_pointer")
    mime_type = part.get("mime_type")
    size_bytes = part.get("size_bytes")
    width = part.get("width")
    height = part.get("height")
    if not isinstance(asset_pointer, str) or not asset_pointer.startswith("sediment://file_"):
        return None
    if not isinstance(mime_type, str) or not mime_type.startswith("image/"):
        return None
    if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes <= 0:
        return None
    if not isinstance(width, int) or isinstance(width, bool) or width <= 0:
        return None
    if not isinstance(height, int) or isinstance(height, bool) or height <= 0:
        return None
    return ImageAssetPointer(
        file_id=asset_pointer.removeprefix("sediment://"),
        asset_pointer=asset_pointer,
        mime_type=mime_type,
        size_bytes=size_bytes,
        width=width,
        height=height,
    )


def _parse_asset_message(message: dict, *, conversation_id: str) -> AssetMessage | None:
    if (message.get("author") or {}).get("role") != "tool":
        return None
    if message.get("status") != "finished_successfully":
        return None
    if message.get("recipient") != "all" or message.get("channel") != "final":
        return None
    mid = message.get("id")
    metadata = message.get("metadata")
    content = message.get("content")
    if not isinstance(mid, str) or not mid:
        return None
    if not isinstance(metadata, dict) or not isinstance(content, dict):
        return None
    parent_id = metadata.get("parent_id")
    turn_exchange_id = metadata.get("turn_exchange_id")
    working_turn_id = metadata.get("working_turn_id")
    if not all(
        isinstance(x, str) and x
        for x in (conversation_id, parent_id, turn_exchange_id, working_turn_id)
    ):
        return None
    if working_turn_id != turn_exchange_id:
        return None
    parts = content.get("parts")
    if not isinstance(parts, list):
        return None
    pointers = tuple(
        pointer
        for part in parts
        if (pointer := _parse_pointer(part)) is not None
    )
    if not pointers:
        return None
    return AssetMessage(
        message_id=mid,
        conversation_id=conversation_id,
        parent_id=parent_id,
        turn_exchange_id=turn_exchange_id,
        working_turn_id=working_turn_id,
        pointers=pointers,
    )


def _parse_terminal_message(
    message: dict, *, conversation_id: str
) -> EndTurnMessage | None:
    if (message.get("author") or {}).get("role") != "assistant":
        return None
    if message.get("status") != "finished_successfully" or message.get("end_turn") is not True:
        return None
    if message.get("recipient") != "all" or message.get("channel") != "final":
        return None
    mid = message.get("id")
    metadata = message.get("metadata")
    if not isinstance(mid, str) or not mid or not isinstance(metadata, dict):
        return None
    turn_exchange_id = metadata.get("turn_exchange_id")
    working_turn_id = metadata.get("working_turn_id")
    if not all(
        isinstance(x, str) and x
        for x in (conversation_id, turn_exchange_id, working_turn_id)
    ):
        return None
    if working_turn_id != turn_exchange_id:
        return None
    return EndTurnMessage(
        message_id=mid,
        conversation_id=conversation_id,
        turn_exchange_id=turn_exchange_id,
        working_turn_id=working_turn_id,
    )


def parse_ws_payload(payload_data: str) -> tuple[list[AssetMessage], list[EndTurnMessage]]:
    if not isinstance(payload_data, str) or not payload_data:
        return [], []
    if (
        "image_asset_pointer" not in payload_data
        and '"end_turn":true' not in payload_data
        and '"end_turn": true' not in payload_data
    ):
        return [], []
    try:
        obj = json.loads(payload_data)
    except Exception:
        return [], []
    payload = obj.get("payload") if isinstance(obj, dict) else None
    conversation_id = payload.get("conversation_id") if isinstance(payload, dict) else None
    if not isinstance(conversation_id, str) or not conversation_id:
        return [], []
    assets: list[AssetMessage] = []
    terminals: list[EndTurnMessage] = []
    for message in _message_dicts(obj):
        asset = _parse_asset_message(message, conversation_id=conversation_id)
        if asset is not None:
            assets.append(asset)
        terminal = _parse_terminal_message(message, conversation_id=conversation_id)
        if terminal is not None:
            terminals.append(terminal)
    return assets, terminals


def _download_metadata_url(file_id: str, conversation_id: str) -> str:
    query = urlencode(
        {
            "conversation_id": conversation_id,
            "download_intent": "false",
            "inline": "true",
            "include_library_file_state": "true",
        }
    )
    return f"/backend-api/files/download/{quote(file_id, safe='')}?{query}"


class TurnNonTextRuntime:
    def __init__(self, driver) -> None:
        self._driver = driver
        self._queue: asyncio.Queue[tuple[int, list[AssetMessage], list[EndTurnMessage]]] = (
            asyncio.Queue(maxsize=_MAX_QUEUE)
        )
        self._consumer_task: asyncio.Task | None = None
        self._assets: dict[str, _ObservedAsset] = {}
        self._terminals: dict[str, _ObservedTerminal] = {}
        self._changed = asyncio.Event()
        self._signal_seq = 0
        self._event_seq = 0
        self._started = False
        self.frames_seen = 0
        self.json_frames = 0
        self.bad_frames = 0
        self.queue_overflow_count = 0
        self.turns_started = 0
        self.turns_matched = 0
        self.turns_failed = 0

    async def start(self) -> None:
        if self._started:
            return
        existing = self._driver._cdp_event_handlers.get(_WS_METHOD)
        if existing is not None and existing != self._on_ws_frame:
            raise RuntimeError(f"{_WS_METHOD} handler already occupied")
        # IdentityListener owns Network.enable on connect/reconnect.  This
        # runtime is deliberately passive: only register the WS-frame handler.
        self._driver._cdp_event_handlers[_WS_METHOD] = self._on_ws_frame
        self._started = True
        if self._consumer_task is None or self._consumer_task.done():
            self._consumer_task = asyncio.create_task(self._consume())

    async def stop(self) -> None:
        if self._driver._cdp_event_handlers.get(_WS_METHOD) == self._on_ws_frame:
            self._driver._cdp_event_handlers.pop(_WS_METHOD, None)
        self._started = False
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
        self._assets.clear()
        self._terminals.clear()
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self._signal_seq += 1
        self._changed.set()

    def begin_turn(self) -> NonTextTurnContext:
        self.turns_started += 1
        return NonTextTurnContext(
            event_seq=self._event_seq,
            queue_overflows=self.queue_overflow_count,
            started_at=time.monotonic(),
        )

    def _on_ws_frame(self, message: dict) -> None:
        params = message.get("params") or {}
        frame = params.get("response") or {}
        if frame.get("opcode") != 1:
            return
        self.frames_seen += 1
        payload_data = frame.get("payloadData")
        if not isinstance(payload_data, str):
            return
        self._event_seq += 1
        seq = self._event_seq
        assets, terminals = parse_ws_payload(payload_data)
        if not assets and not terminals:
            return
        self.json_frames += 1
        try:
            self._queue.put_nowait((seq, assets, terminals))
        except asyncio.QueueFull:
            self.queue_overflow_count += 1
            self._signal_seq += 1
            self._changed.set()

    async def _consume(self) -> None:
        while True:
            seq, assets, terminals = await self._queue.get()
            try:
                for asset in assets:
                    self._assets[asset.message_id] = _ObservedAsset(seq, asset)
                for terminal in terminals:
                    self._terminals[terminal.message_id] = _ObservedTerminal(seq, terminal)
                self._trim()
            except Exception:
                self.bad_frames += 1
            finally:
                self._signal_seq += 1
                self._changed.set()

    def _trim(self) -> None:
        while len(self._assets) > _MAX_LIVE_MESSAGES:
            self._assets.pop(next(iter(self._assets)))
        while len(self._terminals) > _MAX_LIVE_MESSAGES:
            self._terminals.pop(next(iter(self._terminals)))

    def _asset_candidates(
        self, user_message_id: str, context: NonTextTurnContext
    ) -> list[_ObservedAsset]:
        return [
            observed
            for observed in self._assets.values()
            if observed.seq > context.event_seq
            and observed.asset.parent_id == user_message_id
        ]

    def _matching_terminal(
        self, observed_asset: _ObservedAsset, context: NonTextTurnContext
    ) -> EndTurnMessage | None:
        asset = observed_asset.asset
        for observed in self._terminals.values():
            terminal = observed.terminal
            if observed.seq <= context.event_seq:
                continue
            if (
                terminal.conversation_id == asset.conversation_id
                and terminal.turn_exchange_id == asset.turn_exchange_id
                and terminal.working_turn_id == asset.working_turn_id
            ):
                return terminal
        return None

    async def wait_for_turn(
        self,
        user_message_id: str,
        *,
        timeout: float,
        context: NonTextTurnContext,
    ) -> NonTextTurnResult:
        if not user_message_id:
            return NonTextTurnResult(verdict="no_user_anchor")
        deadline = time.monotonic() + timeout
        try:
            while True:
                observed_signal_seq = self._signal_seq
                if self.queue_overflow_count > context.queue_overflows:
                    self.turns_failed += 1
                    return NonTextTurnResult(
                        verdict="queue_overflow",
                        user_message_id=user_message_id,
                        diagnostic=self._diagnostic(user_message_id, context),
                    )
                candidates = self._asset_candidates(user_message_id, context)
                matches = [
                    (observed, self._matching_terminal(observed, context))
                    for observed in candidates
                ]
                matches = [(asset, terminal) for asset, terminal in matches if terminal]
                if matches:
                    if len(candidates) != 1 or len(matches) != 1:
                        self.turns_failed += 1
                        return NonTextTurnResult(
                            verdict="ambiguous_assets",
                            user_message_id=user_message_id,
                            diagnostic=self._diagnostic(user_message_id, context),
                        )
                    observed_asset, terminal = matches[0]
                    asset = observed_asset.asset
                    self.turns_matched += 1
                    return NonTextTurnResult(
                        verdict="matched",
                        conversation_id=asset.conversation_id,
                        user_message_id=user_message_id,
                        asset=asset,
                        terminal=terminal,
                        diagnostic=self._diagnostic(user_message_id, context),
                    )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.turns_failed += 1
                    return NonTextTurnResult(
                        verdict="no_terminal",
                        user_message_id=user_message_id,
                        diagnostic=self._diagnostic(user_message_id, context),
                    )
                self._changed.clear()
                if self._signal_seq != observed_signal_seq:
                    continue
                try:
                    await asyncio.wait_for(self._changed.wait(), timeout=remaining)
                except TimeoutError:
                    self.turns_failed += 1
                    return NonTextTurnResult(
                        verdict="no_terminal",
                        user_message_id=user_message_id,
                        diagnostic=self._diagnostic(user_message_id, context),
                    )
        finally:
            self._drop_turn(user_message_id)

    def _drop_turn(self, user_message_id: str) -> None:
        turn_keys = {
            (
                observed.asset.conversation_id,
                observed.asset.turn_exchange_id,
                observed.asset.working_turn_id,
            )
            for observed in self._assets.values()
            if observed.asset.parent_id == user_message_id
        }
        self._assets = {
            mid: observed
            for mid, observed in self._assets.items()
            if observed.asset.parent_id != user_message_id
        }
        if turn_keys:
            self._terminals = {
                mid: observed
                for mid, observed in self._terminals.items()
                if (
                    observed.terminal.conversation_id,
                    observed.terminal.turn_exchange_id,
                    observed.terminal.working_turn_id,
                )
                not in turn_keys
            }

    def _diagnostic(self, user_message_id: str, context: NonTextTurnContext) -> dict:
        candidates = self._asset_candidates(user_message_id, context)
        return {
            "asset_candidates": len(candidates),
            "terminal_candidates": sum(
                1 for observed in self._terminals.values()
                if observed.seq > context.event_seq
            ),
            "queue_overflows_since_turn": (
                self.queue_overflow_count - context.queue_overflows
            ),
            "raw_ws_payload_retained": 0,
        }

    async def resolve_result(
        self, result: NonTextTurnResult
    ) -> ResolvedNonTextResult:
        asset = result.asset
        terminal = result.terminal
        if not result.matched or asset is None or terminal is None:
            return ResolvedNonTextResult(
                verdict="not_matched",
                diagnostic={"source_verdict": result.verdict},
            )
        resolved_assets: list[dict] = []
        for pointer in asset.pointers:
            if pointer.size_bytes > _MAX_ASSET_BYTES:
                return ResolvedNonTextResult(
                    verdict="asset_too_large",
                    diagnostic={
                        "file_id": pointer.file_id,
                        "size_bytes": pointer.size_bytes,
                        "limit": _MAX_ASSET_BYTES,
                    },
                )
            resolved = await self._resolve_pointer(pointer, asset.conversation_id)
            if resolved is None:
                return ResolvedNonTextResult(
                    verdict="asset_resolution_failed",
                    diagnostic={"file_id": pointer.file_id},
                )
            resolved_assets.append(resolved)
        payload = {
            "type": "image",
            "source": "websocket_asset_terminal",
            "conversation_id": asset.conversation_id,
            "turn_exchange_id": asset.turn_exchange_id,
            "asset_message_id": asset.message_id,
            "terminal_message_id": terminal.message_id,
            "assets": resolved_assets,
        }
        return ResolvedNonTextResult(verdict="matched", payload=payload)

    async def _resolve_pointer(
        self, pointer: ImageAssetPointer, conversation_id: str
    ) -> dict | None:
        d = self._driver
        metadata_url = _download_metadata_url(pointer.file_id, conversation_id)
        await d.ensure_token()
        raw = await d._js_with_data_strict(
            "(async () => {"
            "  var headers = {'Authorization': 'Bearer ' + __D.token};"
            "  var metaResp = await fetch(__D.metadata_url, {credentials:'include', headers:headers});"
            "  var metaText = await metaResp.text();"
            "  var meta = null; try { meta = JSON.parse(metaText); } catch(e) {}"
            "  var out = {meta_http_status:metaResp.status,status:meta&&meta.status||null,"
            "    download_path:null,download_id:null,asset_http_status:null,"
            "    content_type:null,byte_len:null,sha256:null,data_b64:null};"
            "  if (!meta || typeof meta.download_url !== 'string') return JSON.stringify(out);"
            "  var u = new URL(meta.download_url, location.origin);"
            "  out.download_path = u.pathname; out.download_id = u.searchParams.get('id');"
            "  var assetResp = await fetch(meta.download_url, {credentials:'include', headers:headers});"
            "  var buf = await assetResp.arrayBuffer(); var bytes = new Uint8Array(buf);"
            "  out.asset_http_status = assetResp.status;"
            "  out.content_type = assetResp.headers.get('content-type');"
            "  out.byte_len = bytes.byteLength;"
            "  var digest = await crypto.subtle.digest('SHA-256', buf);"
            "  out.sha256 = Array.from(new Uint8Array(digest))"
            "    .map(function(x){return x.toString(16).padStart(2,'0');}).join('');"
            "  var chunks=[]; for(var i=0;i<bytes.length;i+=0x8000){"
            "    chunks.push(String.fromCharCode.apply(null, bytes.subarray(i,i+0x8000)));"
            "  }"
            "  out.data_b64 = btoa(chunks.join(''));"
            "  return JSON.stringify(out);"
            "})()",
            {"metadata_url": metadata_url, "token": d._access_token},
            timeout=45,
        )
        if not isinstance(raw, str):
            return None
        try:
            resolved = json.loads(raw)
        except json.JSONDecodeError:
            return None
        data_b64 = resolved.get("data_b64")
        if not isinstance(data_b64, str) or not data_b64:
            return None
        try:
            data = base64.b64decode(data_b64, validate=True)
        except (binascii.Error, ValueError):
            return None
        content_type = resolved.get("content_type")
        valid = (
            resolved.get("meta_http_status") == 200
            and resolved.get("status") == "success"
            and resolved.get("download_path") == "/backend-api/estuary/content"
            and resolved.get("download_id") == pointer.file_id
            and resolved.get("asset_http_status") == 200
            and isinstance(content_type, str)
            and content_type.startswith("image/")
            and resolved.get("byte_len") == pointer.size_bytes
            and len(data) == pointer.size_bytes
            and resolved.get("sha256") == hashlib.sha256(data).hexdigest()
        )
        if not valid:
            return None
        return {
            **pointer.to_dict(),
            "content_type": content_type,
            "sha256": resolved["sha256"],
            "data_url": f"data:{content_type};base64,{data_b64}",
        }

    @property
    def stats(self) -> dict:
        return {
            "running": self._started
            and self._consumer_task is not None
            and not self._consumer_task.done(),
            "frames_seen": self.frames_seen,
            "json_frames": self.json_frames,
            "bad_frames": self.bad_frames,
            "queue_overflows": self.queue_overflow_count,
            "turns_started": self.turns_started,
            "turns_matched": self.turns_matched,
            "turns_failed": self.turns_failed,
            "raw_ws_payload_retained": 0,
        }
