"""A2.21: validate the exact-turn WebSocket image-result contract.

The confirmatory matrix is frozen before execution in:
  D:/Study/workspace/gaifan/.gaifan/temp/nontext-ws-contract-matrix-20261010.md

This driver never retains raw WebSocket payloads. It extracts only the
predeclared structural fields needed by the contract.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, urlencode

import websockets

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from nontext_channel_discovery import (  # noqa: E402
    BINDING_NAME,
    DOM_HOOK,
    _extract_user_message_id,
    _target_ws_url,
)

GAIFAN_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_ROOT = GAIFAN_ROOT / ".gaifan" / "temp" / "nontext-ws-contract"
SEND_SUFFIX = "/backend-api/f/conversation"

IMAGE_TURNS = (
    ("I1", "Generate an image of a red cube on white background."),
    ("I2", "Generate an image of a blue sphere on white background."),
    ("I3", "Generate an image of a green pyramid on white background."),
)
TEXT_TURNS = (
    ("T1", "Reply with exactly: MARKER-A221-1", "MARKER-A221-1"),
    ("T2", "Reply with exactly: MARKER-A221-2", "MARKER-A221-2"),
    ("T3", "Reply with exactly: MARKER-A221-3", "MARKER-A221-3"),
)
IMAGE_TIMEOUT = 180.0
TEXT_TIMEOUT = 90.0
STALE_GUARD = 5.0
ANCHOR_TIMEOUT = 12.0


def _download_metadata_url(file_id: str, conversation_id: str) -> str:
    if not file_id or not conversation_id:
        raise ValueError("file_id and conversation_id are required")
    query = urlencode(
        {
            "conversation_id": conversation_id,
            "download_intent": "false",
            "inline": "true",
            "include_library_file_state": "true",
        }
    )
    return f"/backend-api/files/download/{quote(file_id, safe='')}?{query}"


@dataclass(frozen=True)
class ImagePointer:
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
    pointers: tuple[ImagePointer, ...]

    def to_dict(self) -> dict:
        return {
            "message_id": self.message_id,
            "conversation_id": self.conversation_id,
            "parent_id": self.parent_id,
            "turn_exchange_id": self.turn_exchange_id,
            "working_turn_id": self.working_turn_id,
            "pointers": [p.to_dict() for p in self.pointers],
        }


@dataclass(frozen=True)
class EndTurnMessage:
    message_id: str
    conversation_id: str
    turn_exchange_id: str
    working_turn_id: str

    def to_dict(self) -> dict:
        return {
            "message_id": self.message_id,
            "conversation_id": self.conversation_id,
            "turn_exchange_id": self.turn_exchange_id,
            "working_turn_id": self.working_turn_id,
        }


@dataclass
class TurnState:
    label: str
    anchor: str | None = None
    assets: dict[str, AssetMessage] = field(default_factory=dict)
    terminals: dict[str, EndTurnMessage] = field(default_factory=dict)
    dom_events: list[dict] = field(default_factory=list)
    ws_frames_seen: int = 0
    ws_json_frames: int = 0
    ws_bad_json_frames: int = 0
    raw_ws_payload_retained: int = 0
    anchor_event: asyncio.Event = field(default_factory=asyncio.Event)
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    dom_changed: asyncio.Event = field(default_factory=asyncio.Event)


def _walk_dicts(obj):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _walk_dicts(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _walk_dicts(value)


def _message_dicts(obj):
    seen = set()
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


def _parse_pointer_part(part: dict) -> ImagePointer | None:
    if not isinstance(part, dict) or part.get("content_type") != "image_asset_pointer":
        return None
    asset_pointer = part.get("asset_pointer")
    mime_type = part.get("mime_type")
    size_bytes = part.get("size_bytes")
    width = part.get("width")
    height = part.get("height")
    if not isinstance(asset_pointer, str) or not asset_pointer.startswith("sediment://file_"):
        return None
    file_id = asset_pointer.removeprefix("sediment://")
    if not isinstance(mime_type, str) or not mime_type.startswith("image/"):
        return None
    if not isinstance(size_bytes, int) or size_bytes <= 0:
        return None
    if not isinstance(width, int) or width <= 0:
        return None
    if not isinstance(height, int) or height <= 0:
        return None
    return ImagePointer(
        file_id=file_id,
        asset_pointer=asset_pointer,
        mime_type=mime_type,
        size_bytes=size_bytes,
        width=width,
        height=height,
    )


def _parse_asset_message(
    message: dict, *, conversation_id: str
) -> AssetMessage | None:
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
    if not all(isinstance(x, str) and x for x in (parent_id, turn_exchange_id, working_turn_id)):
        return None
    if working_turn_id != turn_exchange_id:
        return None
    parts = content.get("parts")
    if not isinstance(parts, list):
        return None
    pointers = tuple(
        pointer
        for part in parts
        if (pointer := _parse_pointer_part(part)) is not None
    )
    if not pointers:
        return None
    if not isinstance(conversation_id, str) or not conversation_id:
        return None
    return AssetMessage(
        message_id=mid,
        conversation_id=conversation_id,
        parent_id=parent_id,
        turn_exchange_id=turn_exchange_id,
        working_turn_id=working_turn_id,
        pointers=pointers,
    )


def _parse_end_turn_message(
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
    if not all(isinstance(x, str) and x for x in (turn_exchange_id, working_turn_id)):
        return None
    if working_turn_id != turn_exchange_id:
        return None
    if not isinstance(conversation_id, str) or not conversation_id:
        return None
    return EndTurnMessage(
        message_id=mid,
        conversation_id=conversation_id,
        turn_exchange_id=turn_exchange_id,
        working_turn_id=working_turn_id,
    )


def parse_ws_payload(payload_data: str) -> tuple[list[AssetMessage], list[EndTurnMessage]]:
    """Extract only contract structures; never return or retain arbitrary text."""
    if not isinstance(payload_data, str) or not payload_data:
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
        terminal = _parse_end_turn_message(message, conversation_id=conversation_id)
        if terminal is not None:
            terminals.append(terminal)
    return assets, terminals


class ContractTap:
    """Second passive CDP connection for exact-turn WS contract validation."""

    def __init__(self, *, port: int, target_id: str):
        self.port = port
        self.target_id = target_id
        self.ws = None
        self.reader_task: asyncio.Task | None = None
        self.next_id = 0
        self.pending: dict[int, asyncio.Future] = {}
        self.active: TurnState | None = None
        self.completed: list[TurnState] = []
        self._script_id: str | None = None
        self.error: str | None = None

    async def start(self) -> None:
        self.ws = await websockets.connect(
            _target_ws_url(self.port, self.target_id),
            max_size=128 * 1024 * 1024,
            close_timeout=5,
        )
        self.reader_task = asyncio.create_task(self._reader())
        await self._cmd(
            "Network.enable",
            {
                "maxPostDataSize": 64 * 1024 * 1024,
                "maxResourceBufferSize": 8 * 1024 * 1024,
                "maxTotalBufferSize": 16 * 1024 * 1024,
            },
        )
        await self._cmd("Runtime.enable", {})
        await self._cmd("Page.enable", {})
        await self._cmd("Runtime.addBinding", {"name": BINDING_NAME})
        response = await self._cmd("Page.addScriptToEvaluateOnNewDocument", {"source": DOM_HOOK})
        self._script_id = ((response.get("result") or {}).get("identifier"))
        await self._cmd("Runtime.evaluate", {"expression": DOM_HOOK, "returnByValue": True})

    async def stop(self) -> None:
        if self.ws is not None:
            try:
                await self._cmd(
                    "Runtime.evaluate",
                    {
                        "expression": (
                            "window.__w2aNonTextDiscoveryApi && "
                            "window.__w2aNonTextDiscoveryApi.disarm()"
                        ),
                        "returnByValue": True,
                    },
                    timeout=3,
                )
            except Exception:
                pass
            if self._script_id:
                try:
                    await self._cmd(
                        "Page.removeScriptToEvaluateOnNewDocument",
                        {"identifier": self._script_id},
                        timeout=3,
                    )
                except Exception:
                    pass
            try:
                await self._cmd("Runtime.removeBinding", {"name": BINDING_NAME}, timeout=3)
            except Exception:
                pass
        if self.reader_task is not None:
            self.reader_task.cancel()
            try:
                await self.reader_task
            except asyncio.CancelledError:
                pass
        if self.ws is not None:
            await self.ws.close()
        self.ws = None

    async def _cmd(self, method: str, params: dict, timeout: float = 10.0) -> dict:
        if self.ws is None:
            raise RuntimeError("tap not started")
        self.next_id += 1
        mid = self.next_id
        future = asyncio.get_running_loop().create_future()
        self.pending[mid] = future
        await self.ws.send(json.dumps({"id": mid, "method": method, "params": params}))
        try:
            result = await asyncio.wait_for(future, timeout=timeout)
        finally:
            self.pending.pop(mid, None)
        if result.get("error"):
            raise RuntimeError(f"{method}: {result['error']}")
        return result

    async def _reader(self) -> None:
        assert self.ws is not None
        try:
            async for raw in self.ws:
                message = json.loads(raw)
                mid = message.get("id")
                if isinstance(mid, int) and mid in self.pending:
                    future = self.pending[mid]
                    if not future.done():
                        future.set_result(message)
                    continue
                self._event(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"
            for future in list(self.pending.values()):
                if not future.done():
                    future.set_exception(RuntimeError(self.error))

    def _event(self, message: dict) -> None:
        state = self.active
        if state is None:
            return
        method = message.get("method")
        params = message.get("params") or {}

        if method == "Network.requestWillBeSent":
            request = params.get("request") or {}
            url = str(request.get("url") or "")
            if (
                state.anchor is None
                and str(request.get("method") or "").upper() == "POST"
                and SEND_SUFFIX in url
            ):
                user_id = _extract_user_message_id(request.get("postData"))
                if user_id:
                    state.anchor = user_id
                    state.anchor_event.set()
                    asyncio.create_task(self._arm_dom(user_id, state.label))
            return

        if method == "Network.webSocketFrameReceived":
            frame = params.get("response") or {}
            if frame.get("opcode") != 1:
                return
            state.ws_frames_seen += 1
            payload_data = frame.get("payloadData")
            if not isinstance(payload_data, str):
                return
            # Parse in-place, then discard payload_data. No raw text is copied
            # into state or artifacts.
            try:
                obj = json.loads(payload_data)
            except Exception:
                state.ws_bad_json_frames += 1
                return
            state.ws_json_frames += 1
            payload = obj.get("payload") if isinstance(obj, dict) else None
            conversation_id = payload.get("conversation_id") if isinstance(payload, dict) else None
            if not isinstance(conversation_id, str) or not conversation_id:
                return
            for message_obj in _message_dicts(obj):
                asset = _parse_asset_message(message_obj, conversation_id=conversation_id)
                if asset is not None:
                    state.assets.setdefault(asset.message_id, asset)
                terminal = _parse_end_turn_message(message_obj, conversation_id=conversation_id)
                if terminal is not None:
                    state.terminals.setdefault(terminal.message_id, terminal)
            state.changed.set()
            return

        if method == "Runtime.bindingCalled" and params.get("name") == BINDING_NAME:
            payload = params.get("payload")
            if not isinstance(payload, str):
                return
            try:
                event = json.loads(payload)
            except Exception:
                return
            if not isinstance(event, dict) or event.get("label") != state.label:
                return
            # DOM oracle events contain only exact-turn structural state from
            # our hook; no assistant/model hidden text is included.
            state.dom_events.append(event)
            state.dom_changed.set()

    async def _arm_dom(self, turn_id: str, label: str) -> None:
        expression = (
            "window.__w2aNonTextDiscoveryApi && "
            f"window.__w2aNonTextDiscoveryApi.arm({json.dumps(turn_id)}, {json.dumps(label)})"
        )
        try:
            await self._cmd(
                "Runtime.evaluate",
                {"expression": expression, "returnByValue": True},
                timeout=5,
            )
        except Exception:
            return

    def begin_turn(self, label: str) -> TurnState:
        if self.active is not None:
            raise RuntimeError(f"turn already active: {self.active.label}")
        state = TurnState(label=label)
        self.active = state
        return state

    async def end_turn(self) -> TurnState:
        state = self.active
        if state is None:
            raise RuntimeError("no active turn")
        try:
            await self._cmd(
                "Runtime.evaluate",
                {
                    "expression": (
                        "window.__w2aNonTextDiscoveryApi && "
                        "window.__w2aNonTextDiscoveryApi.disarm()"
                    ),
                    "returnByValue": True,
                },
                timeout=5,
            )
        except Exception:
            pass
        self.active = None
        self.completed.append(state)
        return state

    async def wait_anchor(self, state: TurnState, timeout: float = ANCHOR_TIMEOUT) -> str | None:
        if state.anchor:
            return state.anchor
        try:
            await asyncio.wait_for(state.anchor_event.wait(), timeout=timeout)
        except TimeoutError:
            return None
        return state.anchor

    async def wait_contract(
        self, state: TurnState, *, expected_user_id: str, timeout: float
    ) -> tuple[AssetMessage, EndTurnMessage] | None:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            candidates = [
                asset for asset in state.assets.values()
                if asset.parent_id == expected_user_id
            ]
            for asset in candidates:
                terminal = next(
                    (
                        terminal for terminal in state.terminals.values()
                        if terminal.conversation_id == asset.conversation_id
                        and terminal.turn_exchange_id == asset.turn_exchange_id
                        and terminal.working_turn_id == asset.working_turn_id
                    ),
                    None,
                )
                if terminal is not None:
                    return asset, terminal
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return None
            state.changed.clear()
            try:
                await asyncio.wait_for(state.changed.wait(), timeout=remaining)
            except TimeoutError:
                return None

    async def wait_dom_milestone(
        self, state: TurnState, *, kind: str, timeout: float
    ) -> dict | None:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            for event in state.dom_events:
                if event.get("t") == "milestone" and event.get("kind") == kind:
                    return event
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return None
            state.dom_changed.clear()
            try:
                await asyncio.wait_for(state.dom_changed.wait(), timeout=remaining)
            except TimeoutError:
                return None

    async def read_exact_turn_text(self, turn_id: str) -> str:
        expression = r"""
        (function(turnId) {
          var roots = document.querySelectorAll('[data-turn-key]');
          var root = null;
          for (var i = 0; i < roots.length; i++) {
            if (roots[i].getAttribute('data-turn-key') === turnId) {
              root = roots[i]; break;
            }
          }
          if (!root) { return ''; }
          var a = root.querySelector(
            '[data-message-author-role="assistant"], [data-content-search-unit-key$=":assistant"]'
          );
          if (!a) { return ''; }
          var md = a.querySelector('[data-markdown-text-style="assistant-message"]')
                || a.querySelector('.markdown');
          return md ? (md.textContent || '') : '';
        })
        """ + "(" + json.dumps(turn_id) + ")"
        response = await self._cmd(
            "Runtime.evaluate",
            {"expression": expression, "returnByValue": True},
            timeout=5,
        )
        return str((((response.get("result") or {}).get("result") or {}).get("value")) or "")

    async def resolve_pointer(
        self, driver, pointer: ImagePointer, conversation_id: str
    ) -> dict:
        metadata_url = _download_metadata_url(pointer.file_id, conversation_id)
        try:
            await driver.ensure_token()
            raw = await driver._js_with_data_strict(
                "(async () => {"
                "  var headers = {'Authorization': 'Bearer ' + __D.token};"
                "  var metaResp = await fetch(__D.metadata_url, {credentials:'include', headers:headers});"
                "  var meta = await metaResp.json();"
                "  var out = {"
                "    meta_http_status: metaResp.status,"
                "    status: meta && meta.status || null,"
                "    file_name: meta && meta.file_name || null,"
                "    file_size_bytes: meta && meta.file_size_bytes || null,"
                "    download_path: null, download_id: null, content_type: null,"
                "    byte_len: null, sha256: null"
                "  };"
                "  if (!meta || typeof meta.download_url !== 'string') {"
                "    out.error = 'missing_download_url'; return JSON.stringify(out);"
                "  }"
                "  var u = new URL(meta.download_url, location.origin);"
                "  out.download_path = u.pathname;"
                "  out.download_id = u.searchParams.get('id');"
                "  var assetResp = await fetch(meta.download_url, {credentials:'include', headers:headers});"
                "  var bytes = await assetResp.arrayBuffer();"
                "  out.asset_http_status = assetResp.status;"
                "  out.content_type = assetResp.headers.get('content-type');"
                "  out.byte_len = bytes.byteLength;"
                "  var digest = await crypto.subtle.digest('SHA-256', bytes);"
                "  out.sha256 = Array.from(new Uint8Array(digest))"
                "    .map(function(x){return x.toString(16).padStart(2,'0');}).join('');"
                "  return JSON.stringify(out);"
                "})()",
                {"metadata_url": metadata_url, "token": driver._access_token},
                timeout=30,
            )
            return json.loads(raw) if isinstance(raw, str) else {"error": "no_value"}
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}


def _dom_image_oracle(dom_event: dict | None, asset: AssetMessage) -> dict:
    state = (dom_event or {}).get("state") or {}
    images = state.get("images") or []
    message_ids = set()
    dimensions = set()
    for image in images:
        ids = str(image.get("message_ids") or "").split()
        message_ids.update(x for x in ids if x)
        dimensions.add((int(image.get("natural_width") or 0), int(image.get("natural_height") or 0)))
    pointer_dimensions = {(p.width, p.height) for p in asset.pointers}
    return {
        "gallery_count": int(state.get("gallery_count") or 0),
        "image_count": len(images),
        "message_id_match": asset.message_id in message_ids,
        "dimension_match": bool(pointer_dimensions) and pointer_dimensions.issubset(dimensions),
        "loaded": bool(images) and all(
            bool(i.get("complete"))
            and int(i.get("natural_width") or 0) > 0
            and int(i.get("natural_height") or 0) > 0
            for i in images
        ),
    }


def _asset_resolution_ok(pointer: ImagePointer, resolved: dict) -> bool:
    return (
        resolved.get("meta_http_status") == 200
        and resolved.get("status") == "success"
        and resolved.get("download_path") == "/backend-api/estuary/content"
        and resolved.get("download_id") == pointer.file_id
        and resolved.get("asset_http_status") == 200
        and isinstance(resolved.get("content_type"), str)
        and resolved["content_type"].startswith("image/")
        and resolved.get("byte_len") == pointer.size_bytes
        and isinstance(resolved.get("sha256"), str)
        and len(resolved["sha256"]) == 64
    )


async def run_image_turn(d, tap: ContractTap, *, label: str, prompt: str) -> dict:
    d._reset_projection_stats()
    state = tap.begin_turn(label)
    started = time.monotonic()
    row = {
        "label": label,
        "kind": "image",
        "anchor": None,
        "conversation_id": None,
        "asset": None,
        "terminal": None,
        "resolved": [],
        "dom_oracle": None,
        "error": None,
    }
    deadline = asyncio.get_running_loop().time() + IMAGE_TIMEOUT
    try:
        await d.type_message(prompt)
        await d.click_send()
        anchor = await tap.wait_anchor(state)
        row["anchor"] = anchor
        if not anchor:
            row["error"] = "missing_user_uuid"
            return row

        remaining = max(0.1, deadline - asyncio.get_running_loop().time())
        contract = await tap.wait_contract(state, expected_user_id=anchor, timeout=remaining)
        if contract is None:
            row["error"] = "ws_contract_timeout"
            return row
        asset, terminal = contract
        row["asset"] = asset.to_dict()
        row["terminal"] = terminal.to_dict()

        remaining = max(0.1, deadline - asyncio.get_running_loop().time())
        dom_event = await tap.wait_dom_milestone(state, kind="gallery_loaded", timeout=remaining)
        row["dom_oracle"] = _dom_image_oracle(dom_event, asset)
        if dom_event is None:
            row["error"] = "dom_gallery_timeout"
            return row

        row["conversation_id"] = asset.conversation_id

        for pointer in asset.pointers:
            resolved = await tap.resolve_pointer(d, pointer, asset.conversation_id)
            resolved["contract_ok"] = _asset_resolution_ok(pointer, resolved)
            resolved["file_id"] = pointer.file_id
            row["resolved"].append(resolved)
        if not all(item.get("contract_ok") for item in row["resolved"]):
            row["error"] = "asset_resolution_failed"
            return row
        return row
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {exc}"
        return row
    finally:
        row["matching_asset_count"] = sum(
            1 for asset in state.assets.values()
            if state.anchor and asset.parent_id == state.anchor
        )
        row["asset_messages_seen"] = len(state.assets)
        row["terminal_messages_seen"] = len(state.terminals)
        row["ws_frames_seen"] = state.ws_frames_seen
        row["ws_json_frames"] = state.ws_json_frames
        row["ws_bad_json_frames"] = state.ws_bad_json_frames
        row["raw_ws_payload_retained"] = state.raw_ws_payload_retained
        row["projection_stats"] = d.projection_stats
        row["wall_ms"] = int((time.monotonic() - started) * 1000)
        await tap.end_turn()
        await asyncio.sleep(2.0)


async def run_text_turn(
    d, tap: ContractTap, *, label: str, prompt: str, marker: str
) -> dict:
    d._reset_projection_stats()
    state = tap.begin_turn(label)
    started = time.monotonic()
    row = {
        "label": label,
        "kind": "text",
        "anchor": None,
        "marker": marker,
        "text_match": False,
        "error": None,
    }
    try:
        await d.type_message(prompt)
        await d.click_send()
        anchor = await tap.wait_anchor(state)
        row["anchor"] = anchor
        if not anchor:
            row["error"] = "missing_user_uuid"
            return row
        dom_event = await tap.wait_dom_milestone(state, kind="text_terminal", timeout=TEXT_TIMEOUT)
        if dom_event is None:
            row["error"] = "text_terminal_timeout"
            return row
        text = await tap.read_exact_turn_text(anchor)
        row["text_match"] = text.strip() == marker
        row["text_len"] = len(text)
        if not row["text_match"]:
            row["error"] = "wrong_text_output"
            return row
        await asyncio.sleep(STALE_GUARD)
        return row
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {exc}"
        return row
    finally:
        row["matching_asset_count"] = sum(
            1 for asset in state.assets.values()
            if state.anchor and asset.parent_id == state.anchor
        )
        row["mismatched_asset_count"] = sum(
            1 for asset in state.assets.values()
            if state.anchor and asset.parent_id != state.anchor
        )
        row["asset_messages_seen"] = len(state.assets)
        row["ws_frames_seen"] = state.ws_frames_seen
        row["ws_json_frames"] = state.ws_json_frames
        row["ws_bad_json_frames"] = state.ws_bad_json_frames
        row["raw_ws_payload_retained"] = state.raw_ws_payload_retained
        row["projection_stats"] = d.projection_stats
        row["wall_ms"] = int((time.monotonic() - started) * 1000)
        await tap.end_turn()
        await asyncio.sleep(2.0)


def evaluate_gate(rows: list[dict]) -> dict:
    failures = []
    images = [r for r in rows if r.get("kind") == "image"]
    texts = [r for r in rows if r.get("kind") == "text"]
    if len(images) != 3:
        failures.append({"reason": "wrong_image_count", "actual": len(images)})
    if len(texts) != 3:
        failures.append({"reason": "wrong_text_count", "actual": len(texts)})

    for row in images:
        checks = [
            (row.get("error") is None, "turn_error", row.get("error")),
            (bool(row.get("anchor")), "missing_anchor", row.get("anchor")),
            (row.get("matching_asset_count") == 1, "matching_asset_count", row.get("matching_asset_count")),
            (bool(row.get("asset")), "missing_asset", row.get("asset")),
            (bool(row.get("terminal")), "missing_terminal", row.get("terminal")),
            (bool(row.get("resolved")) and all(x.get("contract_ok") for x in row.get("resolved") or []), "asset_resolution", row.get("resolved")),
            ((row.get("dom_oracle") or {}).get("loaded") is True, "dom_loaded", row.get("dom_oracle")),
            ((row.get("dom_oracle") or {}).get("message_id_match") is True, "dom_message_id", row.get("dom_oracle")),
            ((row.get("dom_oracle") or {}).get("dimension_match") is True, "dom_dimensions", row.get("dom_oracle")),
            ((row.get("projection_stats") or {}).get("count", 0) == 0, "projection_gets", row.get("projection_stats")),
            (row.get("raw_ws_payload_retained") == 0, "raw_ws_retained", row.get("raw_ws_payload_retained")),
        ]
        for ok, reason, detail in checks:
            if not ok:
                failures.append({"label": row.get("label"), "reason": reason, "detail": detail})

    for row in texts:
        checks = [
            (row.get("error") is None, "turn_error", row.get("error")),
            (bool(row.get("anchor")), "missing_anchor", row.get("anchor")),
            (row.get("text_match") is True, "text_match", row.get("text_match")),
            (row.get("matching_asset_count") == 0, "stale_asset_accepted", row.get("matching_asset_count")),
            ((row.get("projection_stats") or {}).get("count", 0) == 0, "projection_gets", row.get("projection_stats")),
            (row.get("raw_ws_payload_retained") == 0, "raw_ws_retained", row.get("raw_ws_payload_retained")),
        ]
        for ok, reason, detail in checks:
            if not ok:
                failures.append({"label": row.get("label"), "reason": reason, "detail": detail})

    return {
        "pass": not failures,
        "image_turns": len(images),
        "text_turns": len(texts),
        "failure_count": len(failures),
        "failures": failures,
        "rows": rows,
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cdp-port", type=int, required=True)
    args = parser.parse_args()

    if os.getenv("W2A_NONTEXT_CONTRACT") != "1":
        print("REFUSED: set W2A_NONTEXT_CONTRACT=1 for the live A2.21 matrix")
        return 2

    from chatgpt_web2api.cdp_driver import CDPDriver
    from chatgpt_web2api.chrome import ChromeProcess
    from chatgpt_web2api.config import Config

    run_id = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    out_dir = OUTPUT_ROOT / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    config = Config()
    config.chrome.cdp_port = args.cdp_port
    chrome = ChromeProcess(config)
    await chrome.ensure_running()
    d = CDPDriver(cdp_port=args.cdp_port, tab_mode="owned", parallel_tabs=True)
    tap = None
    rows: list[dict] = []

    try:
        await d.connect()
        if not d.target_id:
            raise RuntimeError("CDPDriver has no target_id")
        tap = ContractTap(port=args.cdp_port, target_id=d.target_id)
        await tap.start()

        for idx in range(3):
            image_label, image_prompt = IMAGE_TURNS[idx]
            text_label, text_prompt, marker = TEXT_TURNS[idx]
            await d.navigate_new_chat()
            await asyncio.sleep(1.0)

            print(f"[a2.21] {image_label} ...", flush=True)
            image_row = await run_image_turn(
                d, tap, label=image_label, prompt=image_prompt
            )
            rows.append(image_row)
            print(
                f"[a2.21] {image_label} anchor={bool(image_row.get('anchor'))} "
                f"asset={bool(image_row.get('asset'))} "
                f"terminal={bool(image_row.get('terminal'))} "
                f"resolved={all(x.get('contract_ok') for x in image_row.get('resolved') or [])} "
                f"dom={image_row.get('dom_oracle')} "
                f"error={image_row.get('error')} wall_ms={image_row.get('wall_ms')}",
                flush=True,
            )

            print(f"[a2.21] {text_label} ...", flush=True)
            text_row = await run_text_turn(
                d, tap, label=text_label, prompt=text_prompt, marker=marker
            )
            rows.append(text_row)
            print(
                f"[a2.21] {text_label} anchor={bool(text_row.get('anchor'))} "
                f"text={text_row.get('text_match')} "
                f"matching_assets={text_row.get('matching_asset_count')} "
                f"mismatched_assets={text_row.get('mismatched_asset_count')} "
                f"error={text_row.get('error')} wall_ms={text_row.get('wall_ms')}",
                flush=True,
            )

        gate = evaluate_gate(rows)
        artifact = {
            "run_id": run_id,
            "target_id": d.target_id,
            "tap_error": tap.error,
            "gate": gate,
        }
        with (out_dir / "contract-gate.json").open("w", encoding="utf-8") as f:
            json.dump(artifact, f, ensure_ascii=False, indent=2)
        print(
            f"[a2.21] pass={gate['pass']} failures={gate['failure_count']} "
            f"artifact={out_dir / 'contract-gate.json'}"
        )
        for failure in gate["failures"]:
            print(f"[a2.21] FAIL {failure}")
        return 0 if gate["pass"] else 1
    finally:
        if tap is not None:
            await tap.stop()
        try:
            await d.close()
        except Exception:
            pass
        if chrome.owns_chrome:
            await chrome.stop()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
