"""Event-driven DOM secondary for textual turn completion.

The hook is dormant during the normal response-stream path.  A caller arms it
with the captured current-turn user UUID only after stream primary is
unavailable or has failed.  Current ChatGPT DOM exposes that UUID on the
turn root as ``data-turn-key``; the observer therefore correlates a turn
exactly instead of guessing from the last assistant message.

Completion is push-only: a MutationObserver inspects the exact turn root and
emits once when meaningful assistant content exists and a visible control
appears *after* the assistant node inside the same turn root.  No timers,
polling, localized labels, or drifting action-button testids are required.
"""
from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)

BINDING_NAME = "__w2aDomStateEvent"
_MAX_TEXT_CHARS = 256 * 1024

_HOOK_SOURCE = r"""
(function () {
  if (window.__w2aDomObserverApi) {
    try { window.__w2aDomObserverApi.disarm(); } catch (e) {}
    return;
  }
  var bindingName = '__w2aDomStateEvent';
  var observer = null;
  var activeTurnId = null;
  var maxTextChars = 262144;
  var assistantSelector = '[data-message-author-role="assistant"], [data-content-search-unit-key$=":assistant"]';

  function emit(ev) {
    try {
      var fn = window[bindingName];
      if (typeof fn === 'function') { fn(JSON.stringify(ev)); }
    } catch (e) { /* observer must never affect the page */ }
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
  function conversationId() {
    try {
      var m = location.pathname.match(/^\/c\/([^/?#]+)/);
      return m ? m[1] : null;
    } catch (e) { return null; }
  }
  function completionControl(root, assistant) {
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
  function inspect() {
    if (!activeTurnId) { return false; }
    try {
      var root = exactTurnRoot(activeTurnId);
      if (!root) { return false; }
      var assistant = root.querySelector(assistantSelector);
      if (!assistant) { return false; }
      var md = assistant.querySelector('[data-markdown-text-style="assistant-message"]')
            || assistant.querySelector('.markdown');
      var text = md ? (md.textContent || '') : '';
      var htmlLen = (assistant.innerHTML || '').length;
      var nonText = !text.trim() && htmlLen > 50;
      if (!text.trim() && !nonText) { return false; }
      if (!completionControl(root, assistant)) { return false; }
      var truncated = text.length > maxTextChars;
      var ids = assistant.getAttribute('data-chatgpt-search-message-ids') || '';
      var assistantId = ids.trim() ? ids.trim().split(/\s+/)[0] : null;
      emit({
        t: 'terminal', turn_id: activeTurnId,
        text: truncated ? text.slice(0, maxTextChars) : text,
        text_truncated: truncated, non_text: nonText, html_len: htmlLen,
        assistant_key: assistant.getAttribute('data-content-search-unit-key'),
        assistant_message_id: assistantId,
        conversation_id: conversationId(), ts: Date.now()
      });
      if (observer) { observer.disconnect(); observer = null; }
      activeTurnId = null;
      return true;
    } catch (e) {
      emit({t: 'hook_error', turn_id: activeTurnId, error: String(e), ts: Date.now()});
      return false;
    }
  }
  function disarm() {
    if (observer) { try { observer.disconnect(); } catch (e) {} observer = null; }
    activeTurnId = null;
  }
  function arm(turnId) {
    disarm();
    if (typeof turnId !== 'string' || !turnId) { return false; }
    activeTurnId = turnId;
    if (inspect()) { return true; }
    try {
      observer = new MutationObserver(function () { inspect(); });
      observer.observe(document.documentElement || document, {
        childList: true, subtree: true, characterData: true,
        attributes: true, attributeFilter: ['data-turn-key', 'data-content-search-unit-key', 'class', 'aria-label']
      });
      inspect();
      return true;
    } catch (e) {
      emit({t: 'hook_error', turn_id: activeTurnId, error: String(e), ts: Date.now()});
      disarm();
      return false;
    }
  }
  window.__w2aDomObserverApi = {arm: arm, disarm: disarm, inspect: inspect};
})();
"""


class PageDomHook:
    def __init__(self, driver) -> None:
        self._driver = driver
        self._installed = False
        self._script_id: str | None = None
        self._binding_added = False
        self._listeners: list = []
        self.event_count = 0
        self.terminal_count = 0
        self.error_count = 0
        self.bad_payload_count = 0

    def add_event_listener(self, listener) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_event_listener(self, listener) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    async def install(self) -> bool:
        if self._installed:
            return True
        d = self._driver
        try:
            await d._cdp("Runtime.enable", {}, timeout=10)
            await d._cdp("Page.enable", {}, timeout=10)
            resp = await d._cdp("Runtime.addBinding", {"name": BINDING_NAME}, timeout=10)
            err = _err_text(resp)
            if err:
                raise RuntimeError(f"addBinding: {err}")
            self._binding_added = True
            resp = await d._cdp(
                "Page.addScriptToEvaluateOnNewDocument", {"source": _HOOK_SOURCE}, timeout=10
            )
            err = _err_text(resp)
            if err:
                raise RuntimeError(f"addScriptToEvaluateOnNewDocument: {err}")
            self._script_id = _result(resp).get("identifier")
            d._binding_router.add(BINDING_NAME, self._on_binding_called)
            # Unlike fetch wrapping, this observer can be installed safely into
            # the current document and therefore never requires a reload.
            resp = await d._cdp(
                "Runtime.evaluate",
                {"expression": _HOOK_SOURCE, "returnByValue": True},
                timeout=10,
            )
            err = _err_text(resp)
            if err:
                raise RuntimeError(f"Runtime.evaluate: {err}")
            self._installed = True
            return True
        except Exception as e:
            logger.warning("page_dom_hook_install_failed: %s", e)
            await self._teardown()
            return False

    async def uninstall(self) -> None:
        await self._teardown()

    async def _teardown(self) -> None:
        d = self._driver
        try:
            if self._installed:
                await self.disarm()
        except Exception:
            pass
        d._binding_router.remove(BINDING_NAME, self._on_binding_called)
        if self._script_id:
            try:
                await d._cdp(
                    "Page.removeScriptToEvaluateOnNewDocument",
                    {"identifier": self._script_id}, timeout=10,
                )
            except Exception:
                pass
            self._script_id = None
        if self._binding_added:
            try:
                await d._cdp("Runtime.removeBinding", {"name": BINDING_NAME}, timeout=10)
            except Exception:
                pass
            self._binding_added = False
        self._installed = False

    async def arm(self, turn_id: str) -> bool:
        if not self._installed or not turn_id:
            return False
        expr = "window.__w2aDomObserverApi && window.__w2aDomObserverApi.arm(" + json.dumps(turn_id) + ")"
        try:
            resp = await self._driver._cdp(
                "Runtime.evaluate", {"expression": expr, "returnByValue": True}, timeout=10
            )
            if _err_text(resp):
                return False
            value = (_result(resp).get("result") or {}).get("value")
            return value is True
        except Exception:
            return False

    async def disarm(self) -> None:
        if not self._installed:
            return
        try:
            await self._driver._cdp(
                "Runtime.evaluate",
                {"expression": "window.__w2aDomObserverApi && window.__w2aDomObserverApi.disarm()", "returnByValue": True},
                timeout=5,
            )
        except Exception:
            pass

    def is_installed(self) -> bool:
        return self._installed

    def mark_session_lost(self) -> None:
        self._installed = False
        self._script_id = None
        self._binding_added = False

    def _on_binding_called(self, msg: dict) -> None:
        params = (msg or {}).get("params") or {}
        if params.get("name") != BINDING_NAME:
            return
        payload = params.get("payload")
        if not isinstance(payload, str):
            self.bad_payload_count += 1
            return
        try:
            event = json.loads(payload)
        except Exception:
            self.bad_payload_count += 1
            return
        if not isinstance(event, dict):
            self.bad_payload_count += 1
            return
        self.event_count += 1
        if event.get("t") == "terminal":
            self.terminal_count += 1
        elif event.get("t") == "hook_error":
            self.error_count += 1
        for listener in list(self._listeners):
            try:
                listener(event)
            except Exception:
                self.error_count += 1

    @property
    def stats(self) -> dict:
        return {
            "installed": self._installed,
            "script_id": self._script_id,
            "binding_added": self._binding_added,
            "events": self.event_count,
            "terminals": self.terminal_count,
            "errors": self.error_count,
            "bad_payloads": self.bad_payload_count,
            "binding_router": self._driver._binding_router.stats,
        }


def _result(msg: dict | None) -> dict:
    from .cdp_transport import cdp_result
    return cdp_result(msg)


def _err_text(msg: dict | None) -> str | None:
    from .cdp_transport import cdp_error_text
    return cdp_error_text(msg)
