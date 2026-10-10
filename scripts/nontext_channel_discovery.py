"""A2.20: passive full-channel discovery for ChatGPT non-text/image turns.

Formal matrix is frozen in the parent Gaifan repo's ignored area:
  D:/Study/workspace/gaifan/.gaifan/temp/nontext-channel-discovery-matrix-20261010.md

This is diagnostic-only. It does not change production completion semantics.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import time
import urllib.request
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

import websockets

TEXT_PROMPT = "Reply with exactly: MARKER-NONTEXT-CONTROL"
IMAGE_PROMPT = "Generate an image of a red cube on white background."
FORMAL_TIMEOUT = 180.0
POST_MILESTONE_HOLD = 5.0
ANCHOR_TIMEOUT = 12.0
MAX_EVENT_PAYLOAD = 256 * 1024
MAX_RESPONSE_BODY = 512 * 1024
GAIFAN_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_ROOT = GAIFAN_ROOT / ".gaifan" / "temp" / "nontext-channel-discovery"
BINDING_NAME = "__w2aNonTextDiscoveryEvent"
SEND_SUFFIX = "/backend-api/f/conversation"

KEYWORDS = (
    "image",
    "asset",
    "file",
    "generation",
    "generated",
    "download",
    "dalle",
    "sandbox",
)

DOM_HOOK = r"""
(function () {
  if (window.__w2aNonTextDiscoveryApi) { return; }
  var bindingName = '__w2aNonTextDiscoveryEvent';
  var observer = null;
  var activeTurnId = null;
  var activeLabel = null;
  var lastSig = null;
  var milestoneSent = false;

  function emit(ev) {
    try {
      var fn = window[bindingName];
      if (typeof fn === 'function') { fn(JSON.stringify(ev)); }
    } catch (e) {}
  }

  function visible(el) {
    try { return !!(el && (el.offsetParent !== null || el.getClientRects().length)); }
    catch (e) { return false; }
  }

  function exactTurnRoot(turnId) {
    try {
      var roots = document.querySelectorAll('[data-turn-key]');
      for (var i = 0; i < roots.length; i++) {
        if (roots[i].getAttribute('data-turn-key') === turnId) { return roots[i]; }
      }
    } catch (e) {}
    return null;
  }

  function assistantNode(root) {
    if (!root) { return null; }
    return root.querySelector(
      '[data-message-author-role="assistant"], [data-content-search-unit-key$=":assistant"]'
    );
  }

  function completionControl(root, assistant) {
    if (!root || !assistant) { return null; }
    try {
      var buttons = root.querySelectorAll('button');
      for (var i = 0; i < buttons.length; i++) {
        var b = buttons[i];
        if (!visible(b) || assistant.contains(b)) { continue; }
        if (assistant.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING) {
          return b;
        }
      }
    } catch (e) {}
    return null;
  }

  function imageInfo(img) {
    var msg = null;
    try {
      var holder = img.closest('[data-chatgpt-search-message-ids]');
      msg = holder ? holder.getAttribute('data-chatgpt-search-message-ids') : null;
    } catch (e) {}
    var anchor = null;
    try {
      var a = img.closest('a[href]');
      anchor = a ? {href: a.href || null, download: a.getAttribute('download')} : null;
    } catch (e) {}
    return {
      src: img.getAttribute('src') || null,
      current_src: img.currentSrc || null,
      complete: !!img.complete,
      natural_width: Number(img.naturalWidth || 0),
      natural_height: Number(img.naturalHeight || 0),
      alt: img.getAttribute('alt') || null,
      message_ids: msg,
      anchor: anchor
    };
  }

  function snapshot() {
    var root = exactTurnRoot(activeTurnId);
    if (!root) {
      return {
        root_present: false, assistant_count: 0, text_len: 0,
        gallery_count: 0, images: [], visible_button_count: 0
      };
    }
    var assistants = root.querySelectorAll(
      '[data-message-author-role="assistant"], [data-content-search-unit-key$=":assistant"]'
    );
    var assistant = assistantNode(root);
    var text = '';
    if (assistant) {
      var md = assistant.querySelector('[data-markdown-text-style="assistant-message"]')
            || assistant.querySelector('.markdown');
      text = md ? (md.textContent || '') : '';
    }
    var galleries = root.querySelectorAll('[data-testid="generated-image-gallery"]');
    var images = [];
    for (var i = 0; i < galleries.length; i++) {
      var imgs = galleries[i].querySelectorAll('img');
      for (var j = 0; j < imgs.length; j++) { images.push(imageInfo(imgs[j])); }
    }
    var buttons = root.querySelectorAll('button');
    var visibleButtons = 0;
    for (var k = 0; k < buttons.length; k++) {
      if (visible(buttons[k])) { visibleButtons++; }
    }
    return {
      root_present: true,
      assistant_count: assistants.length,
      assistant_key: assistant ? assistant.getAttribute('data-content-search-unit-key') : null,
      assistant_message_ids: assistant ? assistant.getAttribute('data-chatgpt-search-message-ids') : null,
      text_len: text.length,
      gallery_count: galleries.length,
      images: images,
      visible_button_count: visibleButtons,
      has_following_completion_control: !!completionControl(root, assistant),
      root_html_len: (root.innerHTML || '').length
    };
  }

  function inspect(reason) {
    if (!activeTurnId) { return false; }
    try {
      var s = snapshot();
      var sig = JSON.stringify(s);
      if (sig !== lastSig) {
        lastSig = sig;
        emit({
          t: 'dom_state', turn_id: activeTurnId, label: activeLabel,
          reason: reason || 'mutation', state: s, ts: Date.now()
        });
      }
      if (!milestoneSent && s.root_present) {
        var loaded = s.images.length > 0 && s.images.every(function (img) {
          return img.complete && img.natural_width > 0 && img.natural_height > 0;
        });
        if (s.gallery_count > 0 && loaded) {
          milestoneSent = true;
          emit({
            t: 'milestone', kind: 'gallery_loaded',
            turn_id: activeTurnId, label: activeLabel, state: s, ts: Date.now()
          });
        } else if (s.text_len > 0 && s.has_following_completion_control) {
          milestoneSent = true;
          emit({
            t: 'milestone', kind: 'text_terminal',
            turn_id: activeTurnId, label: activeLabel, state: s, ts: Date.now()
          });
        }
      }
      return true;
    } catch (e) {
      emit({
        t: 'hook_error', turn_id: activeTurnId, label: activeLabel,
        error: String(e), ts: Date.now()
      });
      return false;
    }
  }

  function onLoad(ev) {
    try {
      if (activeTurnId && ev && ev.target && ev.target.tagName === 'IMG') {
        inspect('image_load');
      }
    } catch (e) {}
  }

  function disarm() {
    if (observer) {
      try { observer.disconnect(); } catch (e) {}
      observer = null;
    }
    try { document.removeEventListener('load', onLoad, true); } catch (e) {}
    activeTurnId = null;
    activeLabel = null;
    lastSig = null;
    milestoneSent = false;
  }

  function arm(turnId, label) {
    disarm();
    if (typeof turnId !== 'string' || !turnId) { return false; }
    activeTurnId = turnId;
    activeLabel = label || null;
    try {
      observer = new MutationObserver(function () { inspect('mutation'); });
      observer.observe(document.documentElement || document, {
        childList: true, subtree: true, characterData: true, attributes: true,
        attributeFilter: [
          'data-turn-key', 'data-content-search-unit-key',
          'data-chatgpt-search-message-ids', 'class', 'aria-label', 'src'
        ]
      });
      document.addEventListener('load', onLoad, true);
      inspect('arm');
      return true;
    } catch (e) {
      emit({
        t: 'hook_error', turn_id: activeTurnId, label: activeLabel,
        error: String(e), ts: Date.now()
      });
      disarm();
      return false;
    }
  }

  window.__w2aNonTextDiscoveryApi = {
    arm: arm,
    disarm: disarm,
    inspect: inspect,
    snapshot: snapshot
  };
})();
"""


def _extract_user_message_id(post_data: str | None) -> str | None:
    if not post_data:
        return None
    try:
        obj = json.loads(post_data)
    except Exception:
        return None
    messages = obj.get("messages") if isinstance(obj, dict) else None
    if not isinstance(messages, list):
        return None
    for message in reversed(messages):
        if not isinstance(message, dict):
            continue
        author = message.get("author")
        if not isinstance(author, dict) or author.get("role") != "user":
            continue
        value = message.get("id")
        return value if isinstance(value, str) and value else None
    return None


def _normalize_endpoint(method: str | None, url: str | None) -> str | None:
    if not url:
        return None
    try:
        parts = urlsplit(url)
    except Exception:
        return None
    if parts.scheme not in {"http", "https", "ws", "wss"}:
        return None
    return f"{(method or '?').upper()} {parts.scheme}://{parts.netloc}{parts.path}"


def _payload_excerpt(value: str | None, limit: int = MAX_EVENT_PAYLOAD) -> dict:
    text = value or ""
    raw = text.encode("utf-8", errors="replace")
    return {
        "len": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "text": text[:limit],
        "truncated": len(raw) > limit,
    }


def _keyword_hits(text: str | None) -> list[str]:
    lower = (text or "").lower()
    return [word for word in KEYWORDS if word in lower]


def _body_probe_candidate(meta: dict) -> bool:
    mime = str(meta.get("mime_type") or "").lower()
    resource_type = str(meta.get("resource_type") or "")
    encoded = int(meta.get("encoded_data_length") or 0)
    url = str(meta.get("url") or "")
    if encoded > 2 * 1024 * 1024:
        return False
    if SEND_SUFFIX in url:
        return False
    if resource_type not in {"Fetch", "XHR", "Other"}:
        return False
    return (
        "json" in mime
        or mime.startswith("text/")
        or "javascript" in mime
        or "ndjson" in mime
    )


def _target_ws_url(port: int, target_id: str) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/json/list", timeout=5) as response:
        targets = json.loads(response.read())
    for target in targets:
        if target.get("id") == target_id:
            value = target.get("webSocketDebuggerUrl")
            if value:
                return value
    raise RuntimeError(f"target {target_id!r} not present in /json/list")


class PassiveTap:
    def __init__(self, *, port: int, target_id: str):
        self.port = port
        self.target_id = target_id
        self.ws = None
        self.reader_task: asyncio.Task | None = None
        self.next_id = 0
        self.pending: dict[int, asyncio.Future] = {}
        self.events: list[dict] = []
        self.requests: dict[str, dict] = {}
        self.active_label: str | None = None
        self.active_phase = "idle"
        self.anchor_by_label: dict[str, str] = {}
        self.anchor_events: defaultdict[str, asyncio.Event] = defaultdict(asyncio.Event)
        self.milestone_by_label: dict[str, dict] = {}
        self.milestone_events: defaultdict[str, asyncio.Event] = defaultdict(asyncio.Event)
        self.dom_states: defaultdict[str, list[dict]] = defaultdict(list)
        self._body_tasks: set[asyncio.Task] = set()
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
                "maxResourceBufferSize": 16 * 1024 * 1024,
                "maxTotalBufferSize": 32 * 1024 * 1024,
            },
        )
        await self._cmd("Runtime.enable", {})
        await self._cmd("Page.enable", {})
        await self._cmd("Runtime.addBinding", {"name": BINDING_NAME})
        response = await self._cmd("Page.addScriptToEvaluateOnNewDocument", {"source": DOM_HOOK})
        self._script_id = ((response.get("result") or {}).get("identifier"))
        await self._cmd(
            "Runtime.evaluate",
            {"expression": DOM_HOOK, "returnByValue": True},
        )

    async def stop(self) -> None:
        if self._body_tasks:
            await asyncio.gather(*list(self._body_tasks), return_exceptions=True)
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
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending[mid] = future
        await self.ws.send(json.dumps({"id": mid, "method": method, "params": params}))
        try:
            response = await asyncio.wait_for(future, timeout=timeout)
        finally:
            self.pending.pop(mid, None)
        if response.get("error"):
            raise RuntimeError(f"{method}: {response['error']}")
        return response

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
                await self._event(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"
            for future in list(self.pending.values()):
                if not future.done():
                    future.set_exception(RuntimeError(self.error))

    async def _event(self, message: dict) -> None:
        method = message.get("method")
        params = message.get("params") or {}
        if method == "Runtime.bindingCalled" and params.get("name") == BINDING_NAME:
            self._dom_event(params.get("payload"))
            return
        if not isinstance(method, str) or not method.startswith("Network."):
            return
        if self.active_label is None:
            return

        event = {
            "event": method,
            "label": self.active_label,
            "phase": self.active_phase,
            "observed_ms": int(time.time() * 1000),
        }

        if method == "Network.requestWillBeSent":
            request = params.get("request") or {}
            request_id = str(params.get("requestId") or "")
            url = str(request.get("url") or "")
            http_method = str(request.get("method") or "")
            meta = self.requests.setdefault(request_id, {})
            meta.update(
                {
                    "request_id": request_id,
                    "label": self.active_label,
                    "phase": self.active_phase,
                    "url": url,
                    "method": http_method,
                    "endpoint": _normalize_endpoint(http_method, url),
                    "resource_type": params.get("type"),
                    "initiator_type": (params.get("initiator") or {}).get("type"),
                }
            )
            user_id = _extract_user_message_id(request.get("postData"))
            if (
                self.active_phase == "send"
                and http_method.upper() == "POST"
                and SEND_SUFFIX in url
                and user_id
                and self.active_label not in self.anchor_by_label
            ):
                self.anchor_by_label[self.active_label] = user_id
                self.anchor_events[self.active_label].set()
                asyncio.create_task(self._arm_dom(user_id, self.active_label))
            event.update(
                {
                    "request_id": request_id,
                    "url": url,
                    "method_http": http_method,
                    "endpoint": meta.get("endpoint"),
                    "resource_type": params.get("type"),
                    "initiator_type": meta.get("initiator_type"),
                    "user_message_id": user_id,
                }
            )

        elif method == "Network.responseReceived":
            request_id = str(params.get("requestId") or "")
            response = params.get("response") or {}
            meta = self.requests.setdefault(request_id, {})
            meta.update(
                {
                    "request_id": request_id,
                    "label": meta.get("label") or self.active_label,
                    "phase": meta.get("phase") or self.active_phase,
                    "url": response.get("url") or meta.get("url"),
                    "status": response.get("status"),
                    "mime_type": response.get("mimeType"),
                    "resource_type": params.get("type") or meta.get("resource_type"),
                }
            )
            meta["endpoint"] = meta.get("endpoint") or _normalize_endpoint(
                meta.get("method"), meta.get("url")
            )
            event.update(
                {
                    "request_id": request_id,
                    "url": meta.get("url"),
                    "status": meta.get("status"),
                    "mime_type": meta.get("mime_type"),
                    "resource_type": meta.get("resource_type"),
                    "endpoint": meta.get("endpoint"),
                }
            )

        elif method == "Network.loadingFinished":
            request_id = str(params.get("requestId") or "")
            meta = self.requests.setdefault(request_id, {})
            meta["encoded_data_length"] = params.get("encodedDataLength")
            event.update(
                {
                    "request_id": request_id,
                    "encoded_data_length": params.get("encodedDataLength"),
                    "endpoint": meta.get("endpoint"),
                }
            )
            if _body_probe_candidate(meta):
                task = asyncio.create_task(self._capture_body(request_id))
                self._body_tasks.add(task)
                task.add_done_callback(self._body_tasks.discard)

        elif method == "Network.loadingFailed":
            request_id = str(params.get("requestId") or "")
            meta = self.requests.setdefault(request_id, {})
            meta["loading_failed"] = params.get("errorText")
            event.update(
                {
                    "request_id": request_id,
                    "error": params.get("errorText"),
                    "canceled": params.get("canceled"),
                    "endpoint": meta.get("endpoint"),
                }
            )

        elif method == "Network.eventSourceMessageReceived":
            request_id = str(params.get("requestId") or "")
            meta = self.requests.get(request_id) or {}
            excerpt = _payload_excerpt(params.get("data"))
            event.update(
                {
                    "request_id": request_id,
                    "event_name": params.get("eventName"),
                    "event_id": params.get("eventId"),
                    "endpoint": meta.get("endpoint"),
                    "payload": excerpt,
                    "keyword_hits": _keyword_hits(excerpt.get("text")),
                }
            )

        elif method == "Network.webSocketCreated":
            request_id = str(params.get("requestId") or "")
            url = str(params.get("url") or "")
            meta = self.requests.setdefault(request_id, {})
            meta.update(
                {
                    "request_id": request_id,
                    "label": self.active_label,
                    "phase": self.active_phase,
                    "url": url,
                    "method": "WS",
                    "endpoint": _normalize_endpoint("WS", url),
                    "resource_type": "WebSocket",
                }
            )
            event.update(
                {
                    "request_id": request_id,
                    "url": url,
                    "endpoint": meta.get("endpoint"),
                }
            )

        elif method in {
            "Network.webSocketFrameSent",
            "Network.webSocketFrameReceived",
        }:
            request_id = str(params.get("requestId") or "")
            meta = self.requests.get(request_id) or {}
            frame = params.get("response") or {}
            excerpt = _payload_excerpt(frame.get("payloadData"))
            event.update(
                {
                    "request_id": request_id,
                    "endpoint": meta.get("endpoint"),
                    "opcode": frame.get("opcode"),
                    "payload": excerpt,
                    "keyword_hits": _keyword_hits(excerpt.get("text")),
                }
            )

        elif method in {
            "Network.webSocketWillSendHandshakeRequest",
            "Network.webSocketHandshakeResponseReceived",
            "Network.webSocketClosed",
        }:
            request_id = str(params.get("requestId") or "")
            meta = self.requests.get(request_id) or {}
            event.update(
                {
                    "request_id": request_id,
                    "endpoint": meta.get("endpoint"),
                }
            )
        else:
            return

        self.events.append(event)

    async def _capture_body(self, request_id: str) -> None:
        meta = self.requests.get(request_id)
        if not meta:
            return
        try:
            response = await self._cmd(
                "Network.getResponseBody", {"requestId": request_id}, timeout=5
            )
            result = response.get("result") or {}
            body = result.get("body")
            if not isinstance(body, str):
                meta["body_outcome"] = "missing"
                return
            if result.get("base64Encoded"):
                decoded = base64.b64decode(body)
                text = decoded.decode("utf-8", errors="replace")
            else:
                text = body
            raw = text.encode("utf-8", errors="replace")
            meta["body_outcome"] = "captured"
            meta["body_len"] = len(raw)
            meta["body_sha256"] = hashlib.sha256(raw).hexdigest()
            meta["body_text"] = text[:MAX_RESPONSE_BODY]
            meta["body_truncated"] = len(raw) > MAX_RESPONSE_BODY
            meta["body_keyword_hits"] = _keyword_hits(meta["body_text"])
        except Exception as exc:
            meta["body_outcome"] = f"error:{type(exc).__name__}"

    def _dom_event(self, payload: object) -> None:
        if not isinstance(payload, str):
            return
        try:
            event = json.loads(payload)
        except Exception:
            return
        if not isinstance(event, dict):
            return
        label = event.get("label")
        if not isinstance(label, str) or not label:
            label = self.active_label
        if not label:
            return
        event["phase"] = self.active_phase
        self.dom_states[label].append(event)
        if event.get("t") == "milestone":
            self.milestone_by_label[label] = event
            self.milestone_events[label].set()

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
        except Exception as exc:
            self.dom_states[label].append(
                {"t": "arm_error", "turn_id": turn_id, "label": label, "error": str(exc)}
            )

    def begin_turn(self, label: str) -> None:
        self.active_label = label
        self.active_phase = "send"
        self.anchor_by_label.pop(label, None)
        self.milestone_by_label.pop(label, None)
        self.anchor_events[label] = asyncio.Event()
        self.milestone_events[label] = asyncio.Event()
        self.dom_states[label].clear()

    def end_turn(self) -> None:
        self.active_phase = "idle"
        self.active_label = None

    async def wait_anchor(self, label: str, timeout: float = ANCHOR_TIMEOUT) -> str | None:
        if label in self.anchor_by_label:
            return self.anchor_by_label[label]
        try:
            await asyncio.wait_for(self.anchor_events[label].wait(), timeout=timeout)
        except TimeoutError:
            return None
        return self.anchor_by_label.get(label)

    async def wait_milestone(self, label: str, timeout: float) -> dict | None:
        if label in self.milestone_by_label:
            return self.milestone_by_label[label]
        try:
            await asyncio.wait_for(self.milestone_events[label].wait(), timeout=timeout)
        except TimeoutError:
            return None
        return self.milestone_by_label.get(label)

    async def asset_probe(self, label: str, turn_id: str) -> dict:
        self.active_phase = "asset_probe"
        expression = r"""
        (async function(turnId) {
          function exactTurnRoot(id) {
            var roots = document.querySelectorAll('[data-turn-key]');
            for (var i = 0; i < roots.length; i++) {
              if (roots[i].getAttribute('data-turn-key') === id) { return roots[i]; }
            }
            return null;
          }
          function hex(buffer) {
            var a = Array.from(new Uint8Array(buffer));
            return a.map(function (x) { return x.toString(16).padStart(2, '0'); }).join('');
          }
          var root = exactTurnRoot(turnId);
          if (!root) { return JSON.stringify({turn_id: turnId, error: 'no_root', assets: []}); }
          var imgs = root.querySelectorAll('[data-testid="generated-image-gallery"] img');
          var seen = {};
          var out = [];
          for (var i = 0; i < imgs.length; i++) {
            var src = imgs[i].currentSrc || imgs[i].src || '';
            if (!src || seen[src]) { continue; }
            seen[src] = true;
            var row = {src: src};
            try {
              var r = await fetch(src);
              var b = await r.arrayBuffer();
              row.ok = !!r.ok;
              row.status = r.status;
              row.final_url = r.url || null;
              row.content_type = r.headers.get('content-type');
              row.byte_len = b.byteLength;
              try { row.sha256 = hex(await crypto.subtle.digest('SHA-256', b)); }
              catch (e) { row.sha256_error = String(e); }
            } catch (e) {
              row.ok = false;
              row.error = String(e);
            }
            out.push(row);
          }
          return JSON.stringify({turn_id: turnId, assets: out});
        })(
        """ + json.dumps(turn_id) + ")"
        try:
            response = await self._cmd(
                "Runtime.evaluate",
                {
                    "expression": expression,
                    "awaitPromise": True,
                    "returnByValue": True,
                },
                timeout=30,
            )
            value = (((response.get("result") or {}).get("result") or {}).get("value"))
            if not isinstance(value, str):
                return {"turn_id": turn_id, "assets": [], "error": "no_value"}
            return json.loads(value)
        except Exception as exc:
            return {"turn_id": turn_id, "assets": [], "error": f"{type(exc).__name__}: {exc}"}
        finally:
            self.active_phase = "send"


def _scenario_endpoint_sets(events: list[dict], turns: list[dict]) -> dict:
    label_to_scenario = {turn["label"]: turn["scenario"] for turn in turns}
    per_label: defaultdict[str, set[str]] = defaultdict(set)
    for event in events:
        if event.get("phase") == "asset_probe":
            continue
        if event.get("event") != "Network.requestWillBeSent":
            continue
        endpoint = event.get("endpoint")
        label = event.get("label")
        if endpoint and label in label_to_scenario:
            per_label[label].add(endpoint)
    out = {"S1": {}, "S8": {}}
    for scenario in ("S1", "S8"):
        labels = [t["label"] for t in turns if t["scenario"] == scenario]
        keys = set().union(*(per_label[label] for label in labels)) if labels else set()
        out[scenario] = {
            endpoint: sum(endpoint in per_label[label] for label in labels)
            for endpoint in sorted(keys)
        }
    return out


def summarize_capture(turns: list[dict], events: list[dict], requests: dict[str, dict]) -> dict:
    endpoints = _scenario_endpoint_sets(events, turns)
    s1_total = sum(1 for t in turns if t["scenario"] == "S1")
    s8_total = sum(1 for t in turns if t["scenario"] == "S8")
    image_specific = []
    for endpoint, hits in endpoints["S8"].items():
        if hits >= max(1, min(2, s8_total)) and endpoints["S1"].get(endpoint, 0) == 0:
            image_specific.append({"endpoint": endpoint, "s8_turns": hits, "s1_turns": 0})

    realtime = []
    for event in events:
        if event["event"] == "Network.eventSourceMessageReceived" or "webSocket" in event["event"]:
            realtime.append(
                {
                    key: event.get(key)
                    for key in (
                        "event",
                        "label",
                        "phase",
                        "request_id",
                        "endpoint",
                        "event_name",
                        "opcode",
                        "keyword_hits",
                    )
                }
            )

    body_hits = []
    for meta in requests.values():
        hits = meta.get("body_keyword_hits") or []
        if hits:
            body_hits.append(
                {
                    "label": meta.get("label"),
                    "phase": meta.get("phase"),
                    "endpoint": meta.get("endpoint"),
                    "hits": hits,
                    "body_len": meta.get("body_len"),
                }
            )

    return {
        "matrix": {"S1_turns": s1_total, "S8_turns": s8_total},
        "turns": turns,
        "endpoint_presence": endpoints,
        "image_specific_endpoint_candidates": image_specific,
        "realtime_events": realtime,
        "response_body_keyword_hits": body_hits,
    }


async def run_turn(d, tap: PassiveTap, *, scenario: str, repeat: int, timeout: float) -> dict:
    label = f"{scenario}-r{repeat}"
    prompt = TEXT_PROMPT if scenario == "S1" else IMAGE_PROMPT
    await d.navigate_new_chat()
    await asyncio.sleep(1.0)
    tap.begin_turn(label)
    started = time.monotonic()
    row = {
        "label": label,
        "scenario": scenario,
        "repeat": repeat,
        "prompt": prompt,
        "anchor": None,
        "milestone": None,
        "asset_probe": None,
        "error": None,
    }
    try:
        await d.type_message(prompt)
        await d.click_send()
        anchor = await tap.wait_anchor(label)
        row["anchor"] = anchor
        if not anchor:
            row["error"] = "missing_send_user_uuid"
            return row

        milestone = await tap.wait_milestone(label, timeout=timeout)
        row["milestone"] = milestone
        if milestone is None:
            row["error"] = "milestone_timeout"
            return row

        expected_kind = "text_terminal" if scenario == "S1" else "gallery_loaded"
        if milestone.get("kind") != expected_kind:
            row["error"] = f"unexpected_milestone:{milestone.get('kind')}"
            return row

        await asyncio.sleep(POST_MILESTONE_HOLD)
        if scenario == "S8":
            row["asset_probe"] = await tap.asset_probe(label, anchor)
        return row
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {exc}"
        return row
    finally:
        row["wall_ms"] = int((time.monotonic() - started) * 1000)
        row["dom_events"] = list(tap.dom_states.get(label) or [])
        tap.end_turn()
        await asyncio.sleep(2.0)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--smoke", action="store_true", help="single S1 integration smoke only")
    mode.add_argument("--matrix", action="store_true", help="frozen S1x3 + S8x3 formal matrix")
    parser.add_argument("--cdp-port", type=int, default=9222)
    args = parser.parse_args()

    if os.getenv("W2A_NONTEXT_DISCOVERY") != "1":
        print("REFUSED: set W2A_NONTEXT_DISCOVERY=1 for live-account discovery")
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
    turns: list[dict] = []

    try:
        await d.connect()
        if not d.target_id:
            raise RuntimeError("CDPDriver has no target_id after connect")
        tap = PassiveTap(port=args.cdp_port, target_id=d.target_id)
        await tap.start()

        plan = [("S1", 1)] if args.smoke else [
            ("S1", 1), ("S8", 1),
            ("S1", 2), ("S8", 2),
            ("S1", 3), ("S8", 3),
        ]
        for scenario, repeat in plan:
            print(f"[nontext-discovery] {scenario} r{repeat} ...", flush=True)
            row = await run_turn(
                d,
                tap,
                scenario=scenario,
                repeat=repeat,
                timeout=FORMAL_TIMEOUT,
            )
            turns.append(row)
            print(
                f"[nontext-discovery] {row['label']} anchor={bool(row['anchor'])} "
                f"milestone={((row.get('milestone') or {}).get('kind'))} "
                f"error={row['error']} wall_ms={row['wall_ms']}",
                flush=True,
            )
            with (out_dir / f"{row['label']}.json").open("w", encoding="utf-8") as f:
                json.dump(row, f, ensure_ascii=False, indent=2)

        summary = summarize_capture(turns, tap.events, tap.requests)
        payload = {
            "run_id": run_id,
            "mode": "smoke" if args.smoke else "matrix",
            "target_id": d.target_id,
            "tap_error": tap.error,
            "summary": summary,
            "events": tap.events,
            "requests": tap.requests,
        }
        with (out_dir / "capture.json").open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        with (out_dir / "summary.json").open("w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        print(f"[nontext-discovery] output={out_dir}")
        print(json.dumps(summary, ensure_ascii=False, indent=2))

        if args.smoke:
            return 0 if turns and turns[0].get("anchor") else 1
        expected = 6
        if len(turns) != expected:
            return 1
        if any(not turn.get("anchor") for turn in turns):
            return 1
        return 0
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
