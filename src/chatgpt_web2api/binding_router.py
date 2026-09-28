"""``Runtime.bindingCalled`` 的多路分发器（评审 P1：binding 单 consumer）。

``driver._cdp_event_handlers`` 的既有约定是「一个 CDP method 一个 handler」
（见 ``turn_network_listener`` 的说明），对 ``Runtime.bindingCalled`` 不成立：
**所有** binding 共用这一个 method，谁后注册谁生效。Method E 的流 hook 与
计划中的 DOM secondary hook（MutationObserver → 另一个 binding）会互相覆盖，
uninstall 时的 ``pop()`` 还会顺手摘掉别人的 handler。

本模块把该 method 收敛成唯一 handler，再按 ``params.name`` 路由到 N 个
listener：

    Runtime.bindingCalled
        -> name == __w2aTurnStreamEvent -> 流 hook
        -> name == __w2aDomStateEvent   -> DOM hook（后续）
        -> 其它                          -> 计数丢弃

约束与既有 handler 契约一致：``handle`` 必须快进快出、不得 await、异常
一律吞掉并计数（reader loop 是所有 CDP 命令 future 的唯一 resolver）。
"""

from __future__ import annotations

import logging
from collections.abc import Callable

logger = logging.getLogger(__name__)

# 本 router 独占的 CDP method（所有 binding 事件都走它）。
BINDING_CALLED_METHOD = "Runtime.bindingCalled"


class BindingRouter:
    """按 binding 名分发 ``Runtime.bindingCalled`` 的多路复用器。"""

    def __init__(self, driver) -> None:
        self._driver = driver
        self._listeners: dict[str, list[Callable[[dict], None]]] = {}
        # 挂到事件表上的稳定引用。bound method 每次属性访问都会新建对象，
        # ``self.handle is self.handle`` 为 False——身份比较必须用这个字段。
        self._registered: Callable[[dict], None] | None = None
        # Metrics（诊断读出）。
        self.routed_count = 0
        self.unrouted_count = 0
        self.error_count = 0

    # ── 注册 ──────────────────────────────────────────────────

    def add(self, name: str, handler: Callable[[dict], None]) -> None:
        """注册一个 binding listener（幂等：同一个 callable 重复注册无效）。

        首个 listener 注册时才把 router 自己挂到 driver 的事件表上。
        """
        listeners = self._listeners.setdefault(name, [])
        if handler not in listeners:
            listeners.append(handler)
        self._attach()

    def remove(self, name: str, handler: Callable[[dict], None]) -> None:
        """摘掉一个 listener；最后一个 listener 摘掉后 router 自身也退出。"""
        listeners = self._listeners.get(name)
        if listeners and handler in listeners:
            listeners.remove(handler)
        if not listeners:
            self._listeners.pop(name, None)
        if not self._listeners:
            self._detach()

    def clear(self) -> None:
        """清空全部 listener（driver close / 会话重建时用）。"""
        self._listeners = {}
        self._detach()

    def names(self) -> list[str]:
        return sorted(self._listeners)

    @property
    def is_attached(self) -> bool:
        return (
            self._registered is not None
            and self._driver._cdp_event_handlers.get(BINDING_CALLED_METHOD) is self._registered
        )

    def _attach(self) -> None:
        d = self._driver
        if self._registered is None:
            self._registered = self.handle
        existing = d._cdp_event_handlers.get(BINDING_CALLED_METHOD)
        if existing is not None and existing is not self._registered:
            # 不该发生：router 是该 method 的唯一合法注册者。覆盖前留痕。
            logger.warning(
                "binding_router_overrides_existing_handler: %r", existing
            )
        d._cdp_event_handlers[BINDING_CALLED_METHOD] = self._registered

    def _detach(self) -> None:
        d = self._driver
        if d._cdp_event_handlers.get(BINDING_CALLED_METHOD) is self._registered:
            d._cdp_event_handlers.pop(BINDING_CALLED_METHOD, None)

    # ── 分发（CDP event handler：快、不阻塞、不抛）──────────────

    def handle(self, msg: dict) -> None:
        params = (msg or {}).get("params") or {}
        name = params.get("name")
        listeners = self._listeners.get(name) if isinstance(name, str) else None
        if not listeners:
            self.unrouted_count += 1
            logger.debug("binding_router: no listener for binding %r", name)
            return
        self.routed_count += 1
        # 复制一份再遍历：listener 可能在回调里摘自己。
        for fn in list(listeners):
            try:
                fn(msg)
            except Exception:
                self.error_count += 1
                logger.exception("binding_router listener failed for %r", name)

    @property
    def stats(self) -> dict:
        return {
            "bindings": self.names(),
            "routed": self.routed_count,
            "unrouted": self.unrouted_count,
            "errors": self.error_count,
            "attached": self.is_attached,
        }
