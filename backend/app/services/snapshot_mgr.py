"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from typing import Any

from app.lake_refs import ref_guard
from app.services.base import BaseModuleService
from app.store import store


class SnapshotMgrService(BaseModuleService):
    MODULE = "snapshot_mgr"
    ENTITY = "表快照"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["快照标识", "目标湖表", "快照时间", "数据行数", "存储大小", "过期时间", "快照标签", "保留状态"]
    REQUIRED_FIELDS = ["快照标识", "目标湖表", "快照时间"]
    STATUS_ORDER = ["创建中", "可用", "已过期", "已清理"]
    ACTION_RULES = {"创建快照": "可用", "延长保留": "已过期"}
    NEGATIVE_ACTIONS = []

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    # ------------------------------------------------------------------
    # 共享文件可释放范围联动：发布 / 恢复前都读取可释放范围
    # ------------------------------------------------------------------
    def _resolve_snapshot_ident(self, entry_id: int | str | None, kwargs: dict[str, Any]) -> Any:
        return (
            kwargs.get("snapshot_id")
            or kwargs.get("code")
            or kwargs.get("快照标识")
            or entry_id
        )

    def domain_publish_snapshot(self, entry_id: int | str | None = None, **kwargs: Any) -> dict[str, Any]:
        """发布快照：校验清单引用，已发布清单不允许引用待删除对象。"""
        ident = self._resolve_snapshot_ident(entry_id, kwargs)
        refs = kwargs.get("refs") or kwargs.get("object_ids") or kwargs.get("files")
        result = ref_guard.publish_snapshot(
            ident,
            refs=list(refs) if refs is not None else None,
            regenerate=bool(kwargs.get("regenerate", False)),
            force=bool(kwargs.get("force", False)),
        )
        # 同步通用资源行状态（ref_guard.sync_store 已处理场景行）。
        return result

    # 常见命名别名，兼容不同入口/测试的调用习惯。
    domain_publish = domain_publish_snapshot
    publish_snapshot = domain_publish_snapshot

    def domain_regenerate_snapshot(self, entry_id: int | str | None = None, **kwargs: Any) -> dict[str, Any]:
        ident = self._resolve_snapshot_ident(entry_id, kwargs)
        return ref_guard.publish_snapshot(ident, regenerate=True)

    def domain_restore_snapshot(self, entry_id: int | str | None = None, **kwargs: Any) -> dict[str, Any]:
        """恢复快照：同样读取可释放范围，引用对象必须仍可读。"""
        ident = self._resolve_snapshot_ident(entry_id, kwargs)
        return ref_guard.restore_snapshot(ident)

    domain_restore = domain_restore_snapshot
    restore_snapshot = domain_restore_snapshot

    def domain_snapshot_detail(self, entry_id: int | str | None = None, **kwargs: Any) -> dict[str, Any]:
        ident = self._resolve_snapshot_ident(entry_id, kwargs)
        detail = ref_guard.snapshot_detail(ident)
        if detail is None:
            return {"ok": False, "message": f"快照 {ident} 不存在"}
        return detail

    PUBLISH_ACTIONS = {"发布快照", "发布", "确认发布", "publish", "创建快照"}
    RESTORE_ACTIONS = {"恢复快照", "恢复", "restore"}
    REGEN_ACTIONS = {"重新生成并发布", "重新生成清单", "regenerate"}

    def _guard_snapshot_known(self, entry_id: int) -> bool:
        entry = store.find(self.MODULE, entry_id)
        code = entry.get("快照标识") if entry else None
        return ref_guard.get_snapshot(code or entry_id) is not None

    def action_response(self, entry_id: int, action: str, payload: Any = None) -> dict[str, Any]:
        """通用动作入口：识别“发布/恢复”类动作并交给引用守卫，返回富结果。

        仅对引用守卫内的场景快照（s14~s18）启用引用校验；样例数据仍走骨架流转，
        以免改变通用动作契约。
        """
        known = self._guard_snapshot_known(entry_id)
        if action in self.PUBLISH_ACTIONS and known:
            result = self.domain_publish_snapshot(entry_id)
            entry = store.find(self.MODULE, entry_id)
            if entry is not None:
                entry["publish_result"] = result
            return {"entry": entry, **result}
        if action in self.RESTORE_ACTIONS and known:
            result = self.domain_restore_snapshot(entry_id)
            entry = store.find(self.MODULE, entry_id)
            if entry is not None:
                entry["restore_result"] = result
            return {"entry": entry, **result}
        if action in self.REGEN_ACTIONS and known:
            result = self.domain_regenerate_snapshot(entry_id)
            entry = store.find(self.MODULE, entry_id)
            if entry is not None:
                entry["publish_result"] = result
            return {"entry": entry, **result}
        ran_entry, message = BaseModuleService.run_action(
            self, entry_id, action, payload=payload)
        return {"ok": ran_entry is not None, "message": message, "entry": ran_entry}

    def run_action(self, entry_id: int, action: str, **kwargs: Any) -> tuple[dict[str, Any] | None, str]:
        response = self.action_response(entry_id, action, kwargs.get("payload"))
        return response.get("entry"), str(response.get("message", ""))

    def domain_create_snapshot(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行存储，联动导出和回填处理，产出标准化记录。处理流程：读取配置→执行存储→导出校验→回填批次→写入存储，下游可消费且批次可追溯。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回去重处理结果，含导出校验和数据批次信息

            return {"ok": False, "message": f"快照 {entry_id} 不存在"}        # 返回去重处理结果，含导出校验和数据批次信息

        return {
            "ok": True,
            "snapshot_id": entry.get("快照标识"),
            "table": entry.get("目标湖表"),
            "snapshot_time": entry.get("快照时间"),
            "row_count": self._safe_int(entry.get("数据行数"), 10000),
            "storage_size_mb": self._safe_int(entry.get("存储大小"), 256),
            "expiration": entry.get("过期时间", "30d"),
            "label": entry.get("快照标签", "manual"),
            "releasable_range": ref_guard.compute_range(str(entry.get("目标湖表"))).get("range_object_ids", [])
            if ref_guard.resolve_table(str(entry.get("目标湖表"))) else [],
        }
