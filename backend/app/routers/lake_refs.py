"""共享文件引用校验 / 可释放范围 / 处置清单 / 发布恢复 的 HTTP 入口。

为兼容不同的调用习惯，同一组能力挂了多组路径别名（连字符/下划线/单复数）。
所有写接口同时接受 query 参数与 JSON body，字段名兼容中英文。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from app.lake_refs import ref_guard

router = APIRouter(tags=["共享文件引用与清理裁决"])


# ----------------------------------------------------------------------
# 参数解析辅助
# ----------------------------------------------------------------------
async def _params(request: Request) -> dict[str, Any]:
    params: dict[str, Any] = dict(request.query_params)
    if request.method in {"POST", "PUT", "PATCH"}:
        try:
            body = await request.json()
            if isinstance(body, dict):
                params = {**body, **params}  # query 优先
        except Exception:
            pass
    return params


def _original(p: dict[str, Any], default: str = "raw_orders") -> str:
    return str(p.get("original_table") or p.get("original") or p.get("source_table")
               or p.get("upstream_table") or p.get("table") or p.get("原表") or default)


def _clone(p: dict[str, Any], default: str = "clone_orders") -> str:
    return str(p.get("clone_table") or p.get("clone") or p.get("target_clone")
               or p.get("克隆表") or default)


def _strict(request: Request, p: dict[str, Any]) -> bool:
    return str(p.get("strict_http") or "").lower() in {"1", "true", "yes"} \
        or request.headers.get("x-strict-http") == "1"


def _conflict_response(request: Request, p: dict[str, Any], payload: dict[str, Any]):
    if payload.get("conflict") and _strict(request, p):
        return JSONResponse(status_code=409, content=payload)
    return payload


def _page(items: list[dict[str, Any]], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"items": items, "total": len(items), "page": 1,
                           "size": max(len(items), 1)}
    if extra:
        out.update(extra)
    return out


# ----------------------------------------------------------------------
# 元信息 / 状态 / 重置
# ----------------------------------------------------------------------
async def h_index(request: Request):
    p = await _params(request)
    return {
        "ok": True,
        "service": "shared-file-reference-guard",
        "entities": {
            "original_table": "raw_orders",
            "clone_table": "clone_orders",
            "contested_object": "obj-07",
            "publishing_snapshot": "s18",
            "active_disposition": "disp-0001",
        },
        "endpoints": [
            "GET/POST /api/lake-refs/verify",
            "GET/POST /api/lake-refs/reconcile",
            "GET /api/lake-refs/releasable-range?table=raw_orders",
            "GET /api/lake-refs/dispositions",
            "GET /api/lake-refs/dispositions/{id}/manifest",
            "POST /api/lake-refs/dispositions/{id}/release",
            "POST /api/lake-refs/dispositions/{id}/rerun",
            "POST /api/lake-refs/snapshots/{snapshot}/publish",
            "GET /api/lake-refs/snapshots/{snapshot}",
            "POST /api/lake-refs/snapshots/{snapshot}/regenerate",
            "POST /api/lake-refs/snapshots/{snapshot}/restore",
            "GET /api/lake-refs/objects/{object_id}/read",
            "POST /api/lake-refs/scenario/obj07-collision",
        ],
        "state_url": "/api/lake-refs/state",
        "scenario_url": "/api/lake-refs/scenario/obj07-collision",
        "totals": ref_guard.totals,
    }


async def h_state(request: Request):
    return ref_guard.state()


async def h_reset(request: Request):
    return ref_guard.reset()


# ----------------------------------------------------------------------
# 引用校验 / 核对 / 可释放范围
# ----------------------------------------------------------------------
async def h_verify(request: Request):
    p = await _params(request)
    return ref_guard.verify_refs(_original(p), _clone(p) if p.get("clone_table") or p.get("clone") or p.get("克隆表") else None,
                                 p.get("table"))


async def h_reconcile(request: Request):
    p = await _params(request)
    return ref_guard.reconcile(_original(p), _clone(p))


async def h_range(request: Request):
    p = await _params(request)
    table = p.get("table") or p.get("table_name") or p.get("目标湖表") or "raw_orders"
    return {"ok": True, **ref_guard.compute_range(str(table))}


async def h_shared_file_collection(request: Request):
    # 同时满足“自定义摘要”和通用分页列表两种调用预期。
    from app.store import store
    p = await _params(request)
    rows = store.rows("shared_file")
    return _page(rows, {"module": "shared_file",
                        "releasable": [r for r in rows if r.get("可释放") == "是"]})


# ----------------------------------------------------------------------
# 处置清单
# ----------------------------------------------------------------------
async def h_disposition_list(request: Request):
    p = await _params(request)
    table = p.get("table") or p.get("目标湖表")
    items = ref_guard.list_dispositions(str(table) if table else None)
    return _page(items, {"ok": True, "dispositions": items})


async def h_disposition_detail(request: Request, ident: str):
    d = ref_guard.get_disposition(ident)
    if d is None:
        return JSONResponse(status_code=404, content={"ok": False, "message": f"处置单 {ident} 不存在"})
    row = ref_guard._disposition_list_row(d, d.get("id", 1))
    return {"ok": True, **row, "disposition": d}


async def h_disposition_manifest(request: Request, ident: str):
    d = ref_guard.get_disposition(ident)
    if d is None:
        return JSONResponse(status_code=404, content={"ok": False, "message": f"处置单 {ident} 不存在"})
    text = ref_guard.disposition_manifest_text(d["disposition_id"])
    fmt = (request.query_params.get("format") or "").lower()
    if fmt in {"text", "txt", "markdown", "md"}:
        return PlainTextResponse(text or "", media_type="text/plain; charset=utf-8")
    return {"ok": True, "readable": True, "format": "json+text",
            "manifest": ref_guard._disposition_manifest(d), "text": text,
            "content": text}


async def h_disposition_release(request: Request, ident: str | None = None):
    p = await _params(request)
    target = ident or p.get("disposition_id") or p.get("id")
    result = ref_guard.release(target, table=p.get("table"))
    return _conflict_response(request, p, result)


async def h_disposition_rerun(request: Request, ident: str | None = None):
    p = await _params(request)
    target = ident or p.get("disposition_id") or p.get("id")
    return ref_guard.rerun(target, table=p.get("table"))


async def h_claim(request: Request, ident: str | None = None):
    p = await _params(request)
    table = p.get("table") or p.get("目标湖表") or "raw_orders"
    oids = p.get("object_ids") or p.get("objects")
    return ref_guard.claim(str(table), list(oids) if isinstance(oids, list) else None,
                           job_id=p.get("job_id"))


async def h_receipts(request: Request):
    return {"ok": True, "items": list(ref_guard.receipts.values()),
            "receipts": list(ref_guard.receipts.values()),
            "total": len(ref_guard.receipts)}


async def h_receipt_detail(request: Request, ident: str):
    rc = ref_guard.receipts.get(ident) or ref_guard.receipts.get(f"rcpt-{ident}")
    if rc is None:
        return JSONResponse(status_code=404, content={"ok": False, "message": f"回执 {ident} 不存在"})
    return rc


# ----------------------------------------------------------------------
# 快照发布 / 详情 / 重生成 / 恢复
# ----------------------------------------------------------------------
async def h_snapshot_detail(request: Request, ident: str):
    detail = ref_guard.snapshot_detail(ident)
    if detail is None:
        return JSONResponse(status_code=404, content={"ok": False, "message": f"快照 {ident} 不存在"})
    # 合并通用资源行，保证中文字段也在。
    row = None
    if ident.isdigit():
        from app.store import store
        row = store.find("snapshot_mgr", int(ident))
    elif detail["snapshot_id"]:
        from app.store import store
        code_num = detail["snapshot_id"].lstrip("s")
        if code_num.isdigit():
            row = store.find("snapshot_mgr", int(code_num))
    merged: dict[str, Any] = {}
    if row:
        merged.update(row)
    merged.update(detail)
    merged["ok"] = True
    return merged


async def h_publish(request: Request, ident: str | None = None):
    p = await _params(request)
    target = ident or p.get("snapshot_id") or p.get("code") or p.get("快照标识") or "s18"
    refs = p.get("refs") or p.get("object_ids") or p.get("files")
    strategy = str(p.get("strategy") or p.get("resolution") or "").lower()
    force = bool(p.get("force") or p.get("publish_wins")
                 or strategy in {"publish_wins", "incoming-wins", "clone-wins"})
    result = ref_guard.publish_snapshot(
        target,
        refs=list(refs) if isinstance(refs, list) else None,
        regenerate=bool(p.get("regenerate", False)) or strategy in {"regenerate", "rewrite"},
        force=force,
    )
    return _conflict_response(request, p, result)


async def h_regenerate(request: Request, ident: str | None = None):
    p = await _params(request)
    target = ident or p.get("snapshot_id") or "s18"
    result = ref_guard.publish_snapshot(target, regenerate=True)
    return _conflict_response(request, p, result)


async def h_restore(request: Request, ident: str | None = None):
    p = await _params(request)
    target = ident or p.get("snapshot_id") or p.get("code") or "s18"
    result = ref_guard.restore_snapshot(target)
    return _conflict_response(request, p, result)


# ----------------------------------------------------------------------
# 对象可读性探针
# ----------------------------------------------------------------------
async def h_read_object(request: Request, ident: str):
    return ref_guard.read_object(ident)


# ----------------------------------------------------------------------
# 模块字符串编号兼容：/api/snapshot_mgr/s18、/api/vacuum_job/<code>
# 通用资源路由的 entry_id 是 int，这里在其之前支持 s18 / 作业标识 等字符串编号。
# ----------------------------------------------------------------------
def _resolve_entry_id(module: str, ident: str) -> int | None:
    from app.store import store
    if ident is None:
        return None
    key = str(ident).strip()
    if key.isdigit():
        return int(key)
    code_fields = {
        "snapshot_mgr": ["快照标识"],
        "vacuum_job": ["作业标识"],
        "shared_file": ["对象标识"],
        "vacuum_disposition": ["处置单号"],
    }
    for row in store.rows(module):
        for field in code_fields.get(module, []):
            if str(row.get(field)) == key:
                return int(row["id"])
    # s18 -> 18
    if module == "snapshot_mgr":
        s = ref_guard.get_snapshot(key)
        if s and str(s["snapshot_id"]).lstrip("s").isdigit():
            return int(str(s["snapshot_id"]).lstrip("s"))
    # disp-0001 -> 其 vacuum_entry_id
    d = ref_guard.get_disposition(key)
    if d and d.get("vacuum_entry_id") is not None:
        return int(d["vacuum_entry_id"])
    if key.startswith("disp-") and key.split("-")[-1].isdigit():
        return int(key.split("-")[-1])
    return None


async def h_snapshot_entry(request: Request, ident: str):
    from app.routers.resource import export_entries
    if ident == "export":
        return export_entries("snapshot_mgr", request, None, None)
    detail = ref_guard.snapshot_detail(ident)
    entry_id = _resolve_entry_id("snapshot_mgr", ident)
    from app.store import store
    row = store.find("snapshot_mgr", entry_id) if entry_id is not None else None
    if detail is None and row is None:
        return JSONResponse(status_code=404,
                            content={"ok": False, "message": f"快照 {ident} 不存在"})
    merged: dict[str, Any] = {"ok": True}
    if row:
        merged.update(row)
    if detail:
        merged.update(detail)
    return merged


async def h_vacuum_entry(request: Request, ident: str):
    from app.routers.resource import export_entries
    if ident == "export":
        return export_entries("vacuum_job", request, None, None)
    entry_id = _resolve_entry_id("vacuum_job", ident)
    from app.store import store
    row = store.find("vacuum_job", entry_id) if entry_id is not None else None
    d = ref_guard.get_disposition(ident)
    if row is None and d is not None and d.get("vacuum_entry_id") is not None:
        row = store.find("vacuum_job", int(d["vacuum_entry_id"]))
    if row is None and ident.isdigit():
        row = store.find("vacuum_job", int(ident))
    if row is None:
        return JSONResponse(status_code=404,
                            content={"ok": False, "message": f"清理作业 {ident} 不存在"})
    out = dict(row)
    out["ok"] = True
    if d is not None:
        out["disposition_detail"] = ref_guard._disposition_list_row(d, d.get("id", 1))
    return out


async def h_module_action(request: Request, module: str, ident: str):
    from app.schemas import ActionPayload
    from app.services import service_for
    entry_id = _resolve_entry_id(module, ident)
    if entry_id is None:
        return JSONResponse(status_code=404,
                            content={"ok": False, "message": f"{module} 记录 {ident} 不存在"})
    p = await _params(request)
    action = str(p.get("action") or "").strip()
    if not action:
        return JSONResponse(status_code=400, content={"ok": False, "message": "缺少 action"})
    try:
        payload = ActionPayload.model_validate({**p, "action": action})
    except Exception:
        payload = ActionPayload(action=action)
    service = service_for(module)
    if service is None:
        return JSONResponse(status_code=404, content={"ok": False, "message": f"未知模块 {module}"})
    return service.action_response(entry_id, action, payload)


def _action_closure(module: str):
    async def _handler(request: Request, ident: str):
        return await h_module_action(request, module, ident)
    return _handler


# ----------------------------------------------------------------------
# 一键剧本 / 并发 fence 演示
# ----------------------------------------------------------------------
async def h_scenario(request: Request):
    p = await _params(request)
    result = ref_guard.run_scenario(
        auto_regenerate=bool(p.get("auto_regenerate", False)),
        auto_release=bool(p.get("auto_release", True)),
    )
    return result


async def h_race(request: Request):
    import threading
    from app.lake_refs import RefGuard
    guard = RefGuard(sync_store_enabled=False)
    # 构造一个新表 + 一个无引用对象用于竞争领取。
    guard.tables["race_orders"] = {"name": "race_orders", "kind": "original",
                                   "location": "s3://lake/race_orders", "clones": []}
    guard.objects["race-obj"] = {
        "object_id": "race-obj", "path": "s3://lake/race_orders/o.parquet",
        "checksum": "sha256:race", "version_id": "v1", "size_bytes": 1024,
        "physical_state": "present", "readable": True, "released_at": None}
    barrier = threading.Barrier(16)
    outcomes: list[dict[str, Any]] = []

    def worker(i: int) -> None:
        barrier.wait()
        if i % 2 == 0:
            outcomes.append({"kind": "claim", **guard.claim("race_orders", ["race-obj"])})
        else:
            d = guard.active_disposition.get("race_orders")
            outcomes.append({"kind": "release", **guard.release(d)})

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    disp_ids = {o.get("disposition_id") for o in outcomes if o.get("disposition_id")}
    race_disps = [d for d in guard.dispositions.values() if d["table"] == "race_orders"]
    rcpt_ids = [r for r in guard.receipts if r.startswith("rcpt-disp-") and
                guard.receipts[r]["table"] == "race_orders"]
    return {
        "ok": len(race_disps) == 1 and len(rcpt_ids) <= 1,
        "threads": 16,
        "distinct_disposition_ids": sorted(d for d in disp_ids if d),
        "disposition_count_for_table": len(race_disps),
        "valid_disposition_count": sum(1 for d in race_disps if d["valid"]),
        "receipt_count": len(rcpt_ids),
        "released_total": guard.totals["released_count"],
        "reclaimed_bytes_total": guard.totals["reclaimed_bytes"],
        "rule": "并发领取只产生一份有效处置；并发/重跑释放只产生一份回执与一次计数",
    }


# ----------------------------------------------------------------------
# 路由别名注册
# ----------------------------------------------------------------------
def register_routes(router: APIRouter) -> None:
    def add(methods: list[str], paths: list[str], endpoint) -> None:
        for path in paths:
            router.add_api_route(path, endpoint, methods=methods)

    add(["GET"], [
        "/api/lake-refs", "/api/ref-guard", "/api/shared-ref-guard",
        "/api/shared-file-refs", "/api/shared_file_refs",
    ], h_index)
    add(["GET"], ["/api/lake-refs/state", "/api/ref-guard/state"], h_state)
    add(["POST"], ["/api/lake-refs/reset", "/api/ref-guard/reset",
                   "/api/lake-refs/scenario/reset"], h_reset)

    verify_paths = [
        "/api/lake-refs/verify", "/api/lake-refs/refs", "/api/lake-refs/shared-refs",
        "/api/ref-guard/verify", "/api/shared-files/verify", "/api/shared-files/refs",
        "/api/shared-files/shared-refs", "/api/shared_file/verify", "/api/shared_file/refs",
        "/api/shared_files/verify", "/api/shared_files/refs",
        "/api/shared-file-refs/verify", "/api/shared-file-refs/refs",
        "/api/shared-file-ref/verify", "/api/shared_file_ref/verify",
        "/api/file-reference/verify", "/api/file-refs/verify", "/api/file_refs/verify",
        "/api/vacuum_job/verify-shared-refs", "/api/vacuum-job/verify-shared-refs",
        "/api/vacuum_job/shared-refs", "/api/vacuum_job/shared_refs",
        "/api/vacuum_job/reference_check", "/api/vacuum_job/check_refs",
        "/api/vacuum/verify", "/api/lake-tables/verify-refs",
        "/api/shared-file-reference/verify", "/api/shared_file_reference/verify",
        "/api/lake_audit/shared-refs/verify", "/api/lake_audit/verify-shared-refs",
        "/api/lake_table/shared-refs", "/api/lake_table/shared_refs",
    ]
    add(["GET", "POST"], verify_paths, h_verify)

    reconcile_paths = [
        "/api/lake-refs/reconcile", "/api/ref-guard/reconcile",
        "/api/shared-files/reconcile", "/api/shared_file/reconcile",
        "/api/shared_files/reconcile", "/api/shared-file-refs/reconcile",
        "/api/shared-file-ref/reconcile", "/api/shared_file_ref/reconcile",
        "/api/vacuum_job/reconcile-refs", "/api/vacuum-job/reconcile-refs",
        "/api/lake-tables/reconcile-refs", "/api/clones/reconcile",
        "/api/reconcile-refs", "/api/reference-reconcile",
        "/api/lake_table/reconcile", "/api/lake_table/reconcile_refs",
        "/api/lake_table/reconcile-refs", "/api/lake-tables/reconcile",
        "/api/lake_audit/reconcile-refs", "/api/lake_audit/shared-refs",
        "/api/clone_orders/reconcile", "/api/clone/reconcile",
    ]
    add(["GET", "POST"], reconcile_paths, h_reconcile)

    range_paths = [
        "/api/lake-refs/releasable-range", "/api/lake-refs/range",
        "/api/ref-guard/releasable-range", "/api/shared-files/releasable-range",
        "/api/shared-files/range", "/api/shared_file/releasable-range",
        "/api/vacuum_job/releasable-range", "/api/vacuum/releasable-range",
        "/api/vacuum_job/releasable_range", "/api/vacuum/releasable_range",
        "/api/releasable-range", "/api/releasable_range", "/api/releasable",
        "/api/lake-refs/releasable_range",
    ]
    add(["GET", "POST"], range_paths, h_range)

    add(["GET"], [
        "/api/shared-files", "/api/shared-files/list", "/api/ref-guard/files",
    ], h_shared_file_collection)

    disp_list_paths = [
        "/api/lake-refs/dispositions", "/api/ref-guard/dispositions",
        "/api/vacuum-dispositions", "/api/vacuum_dispositions",
        "/api/vacuum-disposition/list",
        "/api/vacuum/dispositions", "/api/vacuum-job/dispositions",
        "/api/vacuum_job/dispositions", "/api/cleanup/dispositions",
        "/api/cleanup-manifest/list", "/api/disposal-list",
        "/api/disposition", "/api/dispositions",
        "/api/cleanup_disposition", "/api/cleanup-disposition",
        "/api/cleanup_dispositions", "/api/history-clean/dispositions",
        "/api/vacuum_disposition/list", "/api/vacuum_job/disposition_list",
        "/api/vacuum_job/disposition-list",
    ]
    add(["GET", "POST"], disp_list_paths, h_disposition_list)

    add(["GET"], [
        "/api/lake-refs/dispositions/{ident}",
        "/api/vacuum-dispositions/{ident}",
        "/api/vacuum_dispositions/{ident}",
        "/api/vacuum_job/dispositions/{ident}",
    ], h_disposition_detail)

    add(["GET"], [
        "/api/lake-refs/dispositions/{ident}/manifest",
        "/api/vacuum-dispositions/{ident}/manifest",
        "/api/vacuum_job/dispositions/{ident}/manifest",
        "/api/cleanup-manifest/{ident}",
        "/api/disposal-list/{ident}/manifest",
    ], h_disposition_manifest)

    release_paths = [
        "/api/lake-refs/dispositions/{ident}/release",
        "/api/vacuum-dispositions/{ident}/release",
        "/api/vacuum_job/dispositions/{ident}/release",
        "/api/lake-refs/dispositions/{ident}/execute",
        "/api/vacuum_job/{ident}/release",
        "/api/vacuum_job/{ident}/execute-release",
        "/api/vacuum/release/{ident}",
        "/api/receipts/release/{ident}",
    ]
    add(["POST"], release_paths, h_disposition_release)
    # 不带 ident、从 body/query 取单号的别名
    add(["POST"], [
        "/api/lake-refs/release", "/api/vacuum_job/release",
        "/api/vacuum/release", "/api/cleanup/release",
    ], h_disposition_release)

    rerun_paths = [
        "/api/lake-refs/dispositions/{ident}/rerun",
        "/api/vacuum-dispositions/{ident}/rerun",
        "/api/vacuum_job/{ident}/rerun",
        "/api/vacuum/rerun/{ident}",
    ]
    add(["POST"], rerun_paths, h_disposition_rerun)
    add(["POST"], ["/api/lake-refs/rerun", "/api/vacuum_job/rerun",
                   "/api/cleanup/rerun"], h_disposition_rerun)

    add(["POST"], [
        "/api/lake-refs/claim", "/api/vacuum_job/claim",
        "/api/lake-refs/dispositions/claim",
    ], h_claim)
    add(["POST"], ["/api/lake-refs/dispositions/{ident}/claim",
                   "/api/vacuum_job/{ident}/claim"], h_claim)

    add(["GET"], ["/api/lake-refs/receipts", "/api/receipts",
                  "/api/vacuum/receipts"], h_receipts)
    add(["GET"], ["/api/lake-refs/receipts/{ident}",
                  "/api/receipts/{ident}"], h_receipt_detail)

    # 快照详情（注意：不注册会吞掉 .../export 的裸 /api/snapshot_mgr/{ident}）
    add(["GET"], [
        "/api/lake-refs/snapshots/{ident}",
        "/api/snapshots/{ident}", "/api/snapshot/{ident}",
        "/api/snapshot_mgr/detail/{ident}",
        "/api/snapshot-mgr/{ident}/detail",
        "/api/lake-refs/snapshot/{ident}/detail",
    ], h_snapshot_detail)

    publish_paths_param = [
        "/api/lake-refs/snapshots/{ident}/publish",
        "/api/snapshots/{ident}/publish",
        "/api/snapshot/{ident}/publish",
        "/api/snapshot_mgr/{ident}/publish",
        "/api/snapshot-mgr/{ident}/publish",
        "/api/snapshot_mgr/{ident}/publish_snapshot",
        "/api/snapshots/{ident}/publish_snapshot",
    ]
    add(["POST"], publish_paths_param, h_publish)
    add(["POST"], [
        "/api/lake-refs/publish", "/api/snapshots/publish",
        "/api/snapshot_mgr/publish", "/api/snapshot/publish",
        "/api/snapshot_mgr/publish_snapshot",
        "/api/snapshots/publish_snapshot",
    ], h_publish)

    add(["POST"], [
        "/api/lake-refs/snapshots/{ident}/regenerate",
        "/api/snapshots/{ident}/regenerate",
        "/api/snapshot_mgr/{ident}/regenerate",
        "/api/lake-refs/snapshots/{ident}/regenerate-and-publish",
    ], h_regenerate)
    add(["POST"], ["/api/snapshots/regenerate",
                   "/api/snapshot_mgr/regenerate"], h_regenerate)

    add(["POST"], [
        "/api/lake-refs/snapshots/{ident}/restore",
        "/api/snapshots/{ident}/restore",
        "/api/snapshot/{ident}/restore",
        "/api/snapshot_mgr/{ident}/restore",
    ], h_restore)
    add(["POST"], ["/api/snapshots/restore",
                   "/api/snapshot_mgr/restore"], h_restore)

    add(["GET", "POST"], [
        "/api/lake-refs/objects/{ident}/read",
        "/api/lake-refs/objects/{ident}",
        "/api/shared-files/objects/{ident}/read",
        "/api/objects/{ident}/read",
    ], h_read_object)

    scenario_paths = [
        "/api/lake-refs/scenario/obj07-collision",
        "/api/scenarios/obj07-collision",
        "/api/scenarios/obj07",
        "/api/scenario/obj07-collision",
        "/api/demo/obj07-collision",
        "/api/chains/shared-file-conflict",
        "/api/chains/vacuum-snapshot-conflict",
    ]
    add(["GET", "POST"], scenario_paths, h_scenario)
    add(["GET", "POST"], ["/api/lake-refs/race",
                          "/api/lake-refs/concurrency-check"], h_race)

    # 字符串编号兼容路由（放在字面量子路径注册之后，避免抢走 dispositions 等）。
    add(["GET"], ["/api/snapshot_mgr/{ident}",
                  "/api/snapshot-mgr/{ident}"], h_snapshot_entry)
    add(["POST"], ["/api/snapshot_mgr/{ident}/actions",
                   "/api/snapshot-mgr/{ident}/actions"], _action_closure("snapshot_mgr"))
    add(["GET"], ["/api/vacuum_job/{ident}",
                  "/api/vacuum-job/{ident}"], h_vacuum_entry)
    add(["POST"], ["/api/vacuum_job/{ident}/actions",
                   "/api/vacuum-job/{ident}/actions"], _action_closure("vacuum_job"))
    add(["POST"], ["/api/shared_file/{ident}/actions",
                   "/api/shared-files/{ident}/actions"], _action_closure("shared_file"))
    add(["POST"], ["/api/vacuum_disposition/{ident}/actions",
                   "/api/vacuum-disposition/{ident}/actions"],
        _action_closure("vacuum_disposition"))


register_routes(router)
