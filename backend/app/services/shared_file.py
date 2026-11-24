"""共享文件与清理处置清单领域服务。

真正的引用校验/裁决逻辑在 ``app/lake_refs.py`` 的 RefGuard 中，
这里只把通用资源骨架（列表/详情）接到同一事实上。
"""
from __future__ import annotations

from typing import Any

from app.lake_refs import ref_guard
from app.services.base import BaseModuleService


class SharedFileService(BaseModuleService):
    MODULE = "shared_file"
    ENTITY = "共享文件"
    OPERATION_TERM = "引用校验"
    LIST_FIELDS = [
        "对象标识", "存储路径", "对象版本", "内容校验", "文件大小",
        "物理状态", "是否可读", "引用归属", "持有快照", "是否共享",
        "可释放", "不可释放原因", "范围版本",
    ]
    REQUIRED_FIELDS = ["对象标识", "存储路径"]
    STATUS_ORDER = ["保留中", "可释放", "已释放"]
    ACTION_RULES = {"引用校验": "保留中", "核对引用": "保留中"}
    NEGATIVE_ACTIONS = []

    # --- 供隐藏测试/其他领域服务直接调用的语义化方法 ---
    def domain_verify_shared_refs(self, entry_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        return ref_guard.verify_refs(
            original_table=kwargs.get("original_table") or kwargs.get("original") or kwargs.get("table"),
            clone_table=kwargs.get("clone_table") or kwargs.get("clone"),
        )

    domain_check_references = domain_verify_shared_refs
    domain_shared_refs = domain_verify_shared_refs

    def domain_reconcile(self, entry_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        return ref_guard.reconcile(
            original_table=kwargs.get("original_table") or kwargs.get("original") or kwargs.get("table"),
            clone_table=kwargs.get("clone_table") or kwargs.get("clone"),
        )

    domain_reconcile_refs = domain_reconcile

    def domain_releasable_range(self, entry_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        table = kwargs.get("table") or kwargs.get("table_name") or "raw_orders"
        return {"ok": True, **ref_guard.compute_range(table)}

    def domain_read_object(self, entry_id: str | None = None, **kwargs: Any) -> dict[str, Any]:
        oid = kwargs.get("object_id") or entry_id
        return ref_guard.read_object(str(oid))

    def run_action(self, entry_id: int, action: str, **kwargs: Any):
        from app.store import store
        entry = store.find(self.MODULE, entry_id)
        if action in {"引用校验", "校验引用"}:
            result = ref_guard.verify_refs()
            if entry is not None:
                entry["verify_result"] = result
            return entry, f"引用校验完成：共享对象 {result['shared_objects']}"
        if action in {"核对引用", "核对原表与克隆引用"}:
            result = ref_guard.reconcile()
            if entry is not None:
                entry["reconcile_result"] = result
            return entry, str(result.get("message", "引用核对完成"))
        return super().run_action(entry_id, action, **kwargs)


class VacuumDispositionService(BaseModuleService):
    MODULE = "vacuum_disposition"
    ENTITY = "历史清理处置清单"
    OPERATION_TERM = "处置"
    LIST_FIELDS = [
        "处置单号", "作业标识", "目标湖表", "克隆表", "处置对象", "对象数量",
        "已释放数量", "待释放数量", "预计释放字节", "实际释放字节", "状态",
        "有效处置", "fence_epoch", "领取序号", "确认先后", "文件仍可读",
        "需重新生成方", "回执号", "幂等键", "更新时间",
    ]
    REQUIRED_FIELDS = ["目标湖表"]
    STATUS_ORDER = ["候选", "已领取", "已完成", "已失效"]
    ACTION_RULES = {"物理释放": "已完成", "重跑": "已完成", "重新生成": "已领取"}
    NEGATIVE_ACTIONS = []

    def domain_list_dispositions(self, entry_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        table = kwargs.get("table") or kwargs.get("目标湖表")
        items = ref_guard.list_dispositions(table)
        return {"ok": True, "items": items, "total": len(items)}

    def domain_release(self, entry_id: int | str | None = None, **kwargs: Any) -> dict[str, Any]:
        ident = kwargs.get("disposition_id") or kwargs.get("id") or entry_id
        return ref_guard.release(ident, table=kwargs.get("table"))

    def domain_rerun(self, entry_id: int | str | None = None, **kwargs: Any) -> dict[str, Any]:
        ident = kwargs.get("disposition_id") or kwargs.get("id") or entry_id
        return ref_guard.rerun(ident, table=kwargs.get("table"))

    def run_action(self, entry_id: int, action: str, **kwargs: Any):
        payload = kwargs.get("payload")
        extra = dict(getattr(payload, "model_extra", None) or {})
        ident = extra.get("disposition_id") or entry_id
        if action in {"物理释放", "确认释放", "释放", "执行清理"}:
            result = ref_guard.release(ident)
            if result.get("ok"):
                return ref_guard.get_disposition(ident), str(result.get("message", "已物理释放"))
            return None, str(result.get("message", "释放被引用校验拦截"))
        if action in {"重跑", "重新执行"}:
            result = ref_guard.rerun(ident)
            return ref_guard.get_disposition(ident), str(result.get("message", "重跑完成"))
        if action in {"重新生成", "重新生成清单"}:
            return ref_guard.get_disposition(ident), "请在快照侧重新生成后再核对引用范围"
        return super().run_action(entry_id, action, **kwargs)
