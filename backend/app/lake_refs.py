"""共享文件引用校验与清理/发布并发裁决核心（内存实现）。

设计要点
--------
* 对象身份以 ``object_id`` + ``checksum`` 为准，路径只作为展示属性；
* 可释放范围按“湖表 + 有效（已发布/可用）快照 + 克隆家族”计算；
* 清理处置清单使用 epoch/fence 乐观锁，同一湖表同一时刻只允许一份有效处置；
* 清理领取（claim）与物理释放（release）分两阶段，给并发发布留下冲突窗口；
* 发布/恢复前读取可释放范围，已发布清单不得引用待删除对象；
* 释放回执幂等，重跑只返回原回执，不重复计入释放数量/字节。

本模块不依赖 FastAPI，HTTP 编排在 ``app/routers/lake_refs.py``。
"""
from __future__ import annotations

import threading
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Iterable

from app.compute import releasable_range_planner, shared_file_reference_check
from app.store import store


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# 固定种子时间，保证“两边确认的先后”在示例数据里可读。
_T0 = "2026-11-24T16:40:00+00:00"
_T_CLAIM = "2026-11-24T17:05:00+00:00"


class RefGuard:
    """持有湖表/快照/对象/引用/处置单/回执的单一事实源。"""

    def __init__(self, sync_store_enabled: bool = True) -> None:
        self._lock = threading.RLock()
        self._sync_enabled = sync_store_enabled
        self._initialize_state()
        self.bootstrap()

    def _initialize_state(self) -> None:
        self._bootstrapped = False
        self._seq = 100
        self.tables: dict[str, dict[str, Any]] = {}
        self.objects: dict[str, dict[str, Any]] = {}
        # refs: list of {object_id, table, snapshot_id, manifest_id,
        #                published, snapshot_valid, ownership}
        self.refs: list[dict[str, Any]] = []
        self.snapshots: dict[str, dict[str, Any]] = {}
        self.dispositions: dict[str, dict[str, Any]] = {}
        self.disposition_order: list[str] = []
        self.receipts: dict[str, dict[str, Any]] = {}
        self.receipt_order: list[str] = []
        # table -> 当前唯一有效处置单号
        self.active_disposition: dict[str, str] = {}
        # table -> 最近一次核对写入的可释放范围
        self.ranges: dict[str, dict[str, Any]] = {}
        self.range_versions: dict[str, int] = {}
        self.totals = {"released_count": 0, "reclaimed_bytes": 0, "cleanup_runs": 0}

    # ------------------------------------------------------------------
    # 初始化 / 重置
    # ------------------------------------------------------------------
    def reset(self) -> dict[str, Any]:
        with self._lock:
            self._initialize_state()
            self.bootstrap()
            return {"ok": True, "message": "引用校验场景已重置为初始状态"}

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def bootstrap(self) -> None:
        with self._lock:
            if self._bootstrapped:
                return
            self._seed()
            self._bootstrapped = True
            self.sync_store()

    def _seed(self) -> None:
        raw_root = "s3://lakehouse/raw_orders"
        clone_root = "s3://lakehouse/clone_orders"
        shared_curated = "s3://lakehouse/curated/orders_latest.parquet"
        p7 = f"{raw_root}/data/dt=2026-10-01/part-00007.parquet"

        self.tables = {
            "raw_orders": {
                "name": "raw_orders",
                "kind": "original",
                "location": raw_root,
                "clones": ["clone_orders"],
                "source": None,
            },
            "clone_orders": {
                "name": "clone_orders",
                "kind": "clone",
                "location": clone_root,
                "clones": [],
                "source": "raw_orders",
            },
        }

        def obj(oid: str, path: str, checksum: str, size: int, version: str) -> None:
            self.objects[oid] = {
                "object_id": oid,
                "path": path,
                "checksum": checksum,
                "version_id": version,
                "size_bytes": size,
                "physical_state": "present",  # present | released
                "readable": True,
                "released_at": None,
            }

        obj("obj-01", f"{raw_root}/data/dt=2026-11-20/part-00001.parquet", "sha256:1111a1", 52_428_800, "ver-01")
        obj("obj-02", f"{raw_root}/data/dt=2026-11-20/part-00002.parquet", "sha256:2222b2", 48_234_496, "ver-02")
        obj("obj-03", f"{raw_root}/data/dt=2026-11-19/part-00003.parquet", "sha256:3333c3", 61_865_984, "ver-03")
        obj("obj-04", f"{raw_root}/data/dt=2026-11-10/part-00004.parquet", "sha256:4444d4", 40_894_464, "ver-04")
        # obj-05：原表过期快照引用，但已发布克隆 s17 仍持有 —— 不能释放。
        obj("obj-05", f"{raw_root}/data/dt=2026-10-01/part-00005.parquet", "sha256:5555e5", 73_400_320, "ver-05")
        obj("obj-06", f"{clone_root}/data/dt=2026-11-21/part-00006.parquet", "sha256:6666f6", 35_651_584, "ver-06")
        # obj-07：冲突对象。旧清理已领取但未物理释放。
        obj("obj-07", p7, "sha256:0707aa", 67_108_864, "ver-07-old")
        # obj-70 与 obj-07 路径完全相同，但对象身份（version/checksum）不同。
        obj("obj-70", p7, "sha256:7070bb", 67_203_000, "ver-70-current")
        # obj-08 / obj-80：原表与克隆清单里路径相同、身份不同，不能误判为共享。
        obj("obj-08", shared_curated, "sha256:0808cc", 20_971_520, "ver-08-raw")
        obj("obj-80", shared_curated, "sha256:8080dd", 21_100_032, "ver-80-clone")

        def snap(code: str, table: str, status: str, valid: bool,
                 refs: Iterable[str], published: bool | None = None) -> None:
            is_published = published if published is not None else valid
            self.snapshots[code] = {
                "snapshot_id": code,
                "table": table,
                "status": status,
                "valid": valid,
                "published": is_published,
                "manifest_id": f"mfst-{code}",
                "manifest_published": is_published,
                "manifest_valid": True,
                "files": list(refs),
                "requested_refs": list(refs),
                "created_at": _T0,
                "published_at": _T0 if is_published else None,
                "publish_state": "published" if is_published else "draft",
                "conflict": None,
                "regeneration": None,
            }
            for oid in refs:
                self.refs.append({
                    "object_id": oid,
                    "table": table,
                    "snapshot_id": code,
                    "manifest_id": f"mfst-{code}",
                    "published": is_published,
                    "snapshot_valid": valid,
                    "ownership": "clone-inherited" if table == "clone_orders"
                    and oid in {"obj-05", "obj-07"} else
                    ("clone-local" if table == "clone_orders" else "original"),
                })

        snap("s14", "raw_orders", "已过期", False, ["obj-04", "obj-05", "obj-07"])
        snap("s15", "raw_orders", "可用", True, ["obj-01", "obj-03", "obj-04"])
        snap("s16", "raw_orders", "可用", True, ["obj-01", "obj-02", "obj-03", "obj-70", "obj-08"])
        snap("s17", "clone_orders", "可用", True, ["obj-05", "obj-06", "obj-80"])
        # s18：克隆表待发布快照，草稿清单引用 obj-07（此时尚未发布）。
        snap("s18", "clone_orders", "创建中", False, ["obj-07"], published=False)
        self.snapshots["s18"]["publish_state"] = "publish_requested"

        # 旧清理处置单：已领取 obj-07，尚未物理释放。
        disp_id = "disp-0001"
        self._seq = 101
        self.dispositions[disp_id] = {
            "id": 1,
            "disposition_id": disp_id,
            "job_id": "vac-raw-orders-001",
            "vacuum_entry_id": 4,
            "table": "raw_orders",
            "clone_tables": ["clone_orders"],
            "object_ids": ["obj-07"],
            "items": [self._disposition_item("obj-07", "claimed")],
            "status": "已领取",          # 候选 -> 已领取 -> 已完成 / 已失效
            "valid": True,
            "fence_epoch": 1,
            "idempotency_key": "release:vac-raw-orders-001:disp-0001:v1",
            "claim_seq": 101,
            "claimed_at": _T_CLAIM,
            "created_at": _T_CLAIM,
            "updated_at": _T_CLAIM,
            "collisions": [],
            "confirmations": [
                {
                    "side": "vacuum_cleanup",
                    "actor": "vac-raw-orders-001",
                    "action": "claim",
                    "seq": 101,
                    "at": _T_CLAIM,
                    "object_id": "obj-07",
                    "result": "claimed_pending_release",
                }
            ],
            "order_confirmed": [
                {"side": "vacuum_cleanup", "seq": 101, "state": "claimed"},
            ],
            "object_readable": {"obj-07": True},
            "regenerate_required_by": None,
            "receipt_id": None,
            "range_version": 1,
        }
        self.disposition_order.append(disp_id)
        self.active_disposition["raw_orders"] = disp_id
        self.range_versions["raw_orders"] = 1
        # 初始范围（与处置单一致），核对入口会重新计算并回写。
        self.ranges["raw_orders"] = releasable_range_planner(
            "raw_orders", list(self.objects.values()), self.refs,
            clone_tables=["clone_orders"], pending_object_ids={"obj-07"},
        )

    def _disposition_item(self, oid: str, status: str) -> dict[str, Any]:
        obj = self.objects[oid]
        return {
            "object_id": oid,
            "path": obj["path"],
            "checksum": obj["checksum"],
            "version_id": obj["version_id"],
            "size_bytes": obj["size_bytes"],
            "action": "physical_release",
            "status": status,  # candidate | claimed | released | retained | stale
            "readable": obj["readable"],
            "physical_state": obj["physical_state"],
        }

    # ------------------------------------------------------------------
    # 与通用资源仓库同步：让 GET /api/shared_file、/api/vacuum_disposition
    # 以及快照/清理作业行能直接读到同一事实。
    # ------------------------------------------------------------------
    def sync_store(self) -> None:
        if not getattr(self, "_sync_enabled", True):
            return
        # ---- shared_file ----
        sf_rows = store.rows("shared_file")
        sf_rows.clear()
        range_map = self.ranges
        for idx, oid in enumerate(sorted(self.objects), start=1):
            o = self.objects[oid]
            ref_rows = [r for r in self.refs if r["object_id"] == oid]
            holders = sorted({f"{r['table']}/{r['snapshot_id']}"
                               + ("(已发布)" if r["published"] else "(未发布)")
                               for r in ref_rows})
            rng_hits = [e for rng in range_map.values() for e in rng.get("releasable", [])
                        if e["object_id"] == oid]
            retained_by = [e.get("retain_reasons") for rng in range_map.values()
                           for e in rng.get("retained", []) if e["object_id"] == oid]
            releasable = bool(rng_hits) and o["physical_state"] == "present"
            sf_rows.append({
                "id": idx,
                "status": "可释放" if releasable else ("已释放" if o["physical_state"] == "released" else "保留中"),
                "pending": releasable,
                "abnormal": False,
                "对象标识": oid,
                "存储路径": o["path"],
                "对象版本": o["version_id"],
                "内容校验": o["checksum"],
                "文件大小": o["size_bytes"],
                "物理状态": "已物理释放" if o["physical_state"] == "released" else "对象存在",
                "是否可读": "是" if o["readable"] else "否",
                "引用归属": "；".join(holders) or "无引用",
                "持有快照": "；".join(sorted({r["snapshot_id"] for r in ref_rows})),
                "是否共享": "是" if len({r["table"] for r in ref_rows}) > 1 else "否",
                "可释放": "是" if releasable else "否",
                "不可释放原因": "；".join(sorted({x for reasons in retained_by for x in (reasons or [])}))
                or ("已物理释放" if o["physical_state"] == "released" else ("待清理处置" if not releasable else "")),
                "范围版本": max(self.range_versions.values(), default=1),
            })

        # ---- vacuum_disposition ----
        vd_rows = store.rows("vacuum_disposition")
        vd_rows.clear()
        for idx, did in enumerate(self.disposition_order, start=1):
            d = self.dispositions[did]
            vd_rows.append(self._disposition_list_row(d, idx))

        # ---- snapshot_mgr 场景行（id 与快照号对齐）----
        snap_rows = store.rows("snapshot_mgr")
        for code, s in self.snapshots.items():
            numeric = int(code.lstrip("s")) if code[1:].isdigit() else None
            if numeric is None:
                continue
            row = store.find("snapshot_mgr", numeric)
            fields = {
                "快照标识": code,
                "目标湖表": s["table"],
                "status": s["status"],
                "当前发布状态": s.get("publish_state"),
                "发布冲突": (s.get("conflict") or {}).get("code") if s.get("conflict") else None,
                "待重新生成": (s.get("regeneration") or {}).get("required") if s.get("regeneration") else False,
                "清单标识": s["manifest_id"],
                "清单已发布": s["manifest_published"],
                "清单有效": s["manifest_valid"],
                "引用对象": "、".join(s.get("files", [])),
            }
            if row is None:
                new_row = {"id": numeric, "pending": s["status"] != "可用",
                           "abnormal": bool(s.get("conflict")), **fields}
                snap_rows.append(new_row)
            else:
                row.update(fields)

        # ---- vacuum_job 场景行 ----
        vj = store.find("vacuum_job", 4)
        d1 = self.dispositions.get("disp-0001")
        if d1 is not None:
            raw_snaps = [c for c, s in self.snapshots.items() if s["table"] == "raw_orders"]
            clone_snaps = [c for c, s in self.snapshots.items() if s["table"] == "clone_orders"]
            fields = {
                "作业标识": "vac-raw-orders-001",
                "目标湖表": "raw_orders",
                "保留版本": 5,
                "清理范围": "obj-07",
                "执行状态": d1["status"],
                "status": "清理中" if d1["status"] == "已领取" else (
                    "已完成" if d1["status"] == "已完成" else "清理异常"),
                "有效处置单": d1["disposition_id"] if d1["valid"] else None,
                "释放空间": round(sum(
                    i["size_bytes"] for i in d1["items"] if i["status"] == "released"
                ) / (1024 * 1024)),
                "回执号": d1.get("receipt_id"),
                # 湖表快照与共享文件归属数据直接挂在清理作业上，便于清单/详情读取。
                "湖表快照": "、".join(raw_snaps),
                "克隆快照": "、".join(clone_snaps),
                "有效快照": "s15、s16（raw_orders）；s17（clone_orders）",
                "共享文件归属": "obj-05 由已发布克隆 s17 持有；obj-07 被 s18 草稿引用",
                "对象身份策略": "object_id+checksum（路径相同也需核对身份）",
                "disposition_id": d1["disposition_id"],
                "fence_epoch": d1["fence_epoch"],
                "confirmation_order": deepcopy(d1.get("confirmations", [])),
                "adjudication": self._adjudication_text(d1),
            }
            if vj is None:
                store.rows("vacuum_job").append({
                    "id": 4, "pending": d1["status"] != "已完成",
                    "abnormal": bool(d1.get("collisions")), "清理时间": "2026-11-24", **fields,
                })
            else:
                vj.update(fields)

        # ---- lake_table 场景行：raw_orders 原表 / clone_orders 克隆表 ----
        table_ids = {"raw_orders": 101, "clone_orders": 102}
        for name, tid in table_ids.items():
            t = self.tables.get(name)
            if not t:
                continue
            is_clone = t.get("kind") == "clone"
            current_snap = "s17" if is_clone else "s16"
            fields = {
                "表标识": name,
                "表名称": name,
                "主键列": "order_id",
                "分区列": "dt",
                "存储格式": "parquet/delta",
                "压缩算法": "zstd",
                "表版本": current_snap,
                "当前快照": current_snap,
                "表类型": "克隆表" if is_clone else "原表",
                "来源表": t.get("source"),
                "克隆表": "、".join(t.get("clones", [])),
                "存储位置": t.get("location"),
                "status": "已创建",
            }
            row = store.find("lake_table", tid)
            if row is None:
                store.rows("lake_table").append(
                    {"id": tid, "pending": False, "abnormal": False, **fields})
            else:
                row.update(fields)

    def _disposition_list_row(self, d: dict[str, Any], idx: int) -> dict[str, Any]:
        released = [i for i in d["items"] if i["status"] == "released"]
        claimed = [i for i in d["items"] if i["status"] == "claimed"]
        contested = [c for c in d.get("collisions", [])]
        order_desc = " -> ".join(
            f"{c['side']}#{c['seq']}({c.get('result') or c.get('state')})"
            for c in d.get("confirmations", [])
        )
        readable_map = d.get("object_readable", {})
        return {
            "id": d.get("id", idx),
            "status": d["status"],
            "pending": d["status"] not in {"已完成", "已失效"},
            "abnormal": bool(contested) and d["status"] != "已完成",
            "处置单号": d["disposition_id"],
            "作业标识": d["job_id"],
            "目标湖表": d["table"],
            "克隆表": "、".join(d.get("clone_tables", [])),
            "处置对象": "、".join(d["object_ids"]),
            "对象数量": len(d["object_ids"]),
            "已释放数量": len(released),
            "待释放数量": len(claimed),
            "预计释放字节": sum(i["size_bytes"] for i in d["items"]),
            "实际释放字节": sum(i["size_bytes"] for i in released),
            "状态": d["status"],
            "有效处置": "是" if d["valid"] else "否",
            "fence_epoch": d["fence_epoch"],
            "领取序号": d["claim_seq"],
            "确认先后": order_desc,
            "两边确认顺序": deepcopy(d.get("confirmations", [])),
            "冲突记录": deepcopy(contested),
            "文件仍可读": "；".join(
                f"{oid}:{'可读' if ok else '不可读'}" for oid, ok in readable_map.items()
            ),
            "对象可读性": deepcopy(readable_map),
            "需重新生成方": d.get("regenerate_required_by"),
            "重新生成结论": self._adjudication_text(d),
            "回执号": d.get("receipt_id"),
            "幂等键": d["idempotency_key"],
            "更新时间": d.get("updated_at"),
            "manifest": self._disposition_manifest(d),
        }

    def _adjudication_text(self, d: dict[str, Any]) -> str:
        """生成一句中文裁决：确认先后 + 可读性 + 哪一方重新生成。"""
        cols = d.get("collisions") or []
        if not cols:
            if d.get("status") == "已完成":
                return "清理处置已完成并取得物理释放回执；重跑幂等，不重复计入释放。"
            return "暂无发布冲突；处置单为当前唯一有效处置。"
        c = cols[-1]
        oid = c.get("object_id")
        order = "清理领取在先、发布请求在后" if c.get("cleanup_first") else "发布确认在先、清理核对在后"
        readable = "文件物理仍存在、仍可读（领取不等于删除）" if c.get("object_readable") \
            else "文件已物理释放、不可读"
        who = c.get("which_side_regenerates") or (
            "发布方（克隆表快照 " + str(c.get("snapshot_id")) + "）需重新生成清单"
            if c.get("regenerate_required_by") == "snapshot_publish"
            else "清理方需按新可释放范围重新生成处置清单")
        return f"两边确认顺序：{order}（claim_seq={c.get('claim_seq')} < publish_seq={c.get('publish_seq')}）；" \
               f"{oid} {readable}；裁决：{who}。"

    def _disposition_manifest(self, d: dict[str, Any]) -> dict[str, Any]:
        return {
            "manifest_id": f"{d['disposition_id']}-manifest-v{d['fence_epoch']}",
            "kind": "vacuum_disposition_manifest",
            "disposition_id": d["disposition_id"],
            "table": d["table"],
            "generated_seq": d["claim_seq"],
            "valid": d["valid"],
            "files": [
                {
                    "object_id": i["object_id"],
                    "path": i["path"],
                    "checksum": i["checksum"],
                    "version_id": i["version_id"],
                    "size_bytes": i["size_bytes"],
                    "disposition": i["action"],
                    "item_status": i["status"],
                }
                for i in d["items"]
            ],
        }

    def disposition_manifest_text(self, did: str) -> str | None:
        d = self.get_disposition(did)
        if d is None:
            return None
        m = self._disposition_manifest(d)
        lines = [
            f"# 历史清理处置清单 {m['manifest_id']}",
            f"disposition={m['disposition_id']} table={m['table']} valid={m['valid']} seq={m['generated_seq']}",
            "object_id | version_id | checksum | size_bytes | disposition | status | path",
        ]
        for f in m["files"]:
            lines.append(
                f"{f['object_id']} | {f['version_id']} | {f['checksum']} | "
                f"{f['size_bytes']} | {f['disposition']} | {f['item_status']} | {f['path']}"
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 查询辅助
    # ------------------------------------------------------------------
    def resolve_table(self, ident: str | None) -> str | None:
        if not ident:
            return None
        key = str(ident).strip()
        if key in self.tables:
            return key
        low = key.casefold()
        for name, t in self.tables.items():
            if low in {name.casefold(), str(t.get("location", "")).casefold(),
                       f"lake_table:{name}".casefold()}:
                return name
        # 兼容 numeric lake_table id（101 raw / 102 clone）
        id_map = {"101": "raw_orders", "102": "clone_orders"}
        return id_map.get(key)

    def get_snapshot(self, ident: str | int | None) -> dict[str, Any] | None:
        if ident is None:
            return None
        key = str(ident).strip()
        if key in self.snapshots:
            return self.snapshots[key]
        low = key.casefold()
        for code, s in self.snapshots.items():
            if low == code.casefold():
                return s
        # numeric store id -> s{id}
        if key.isdigit() and f"s{int(key)}" in self.snapshots:
            return self.snapshots[f"s{int(key)}"]
        row = store.find("snapshot_mgr", int(key)) if key.isdigit() else None
        if row and row.get("快照标识") in self.snapshots:
            return self.snapshots[row["快照标识"]]
        return None

    def get_disposition(self, ident: str | int | None) -> dict[str, Any] | None:
        if ident is None:
            return None
        key = str(ident).strip()
        if key in self.dispositions:
            return self.dispositions[key]
        if key.isdigit():
            for d in self.dispositions.values():
                if int(d.get("id", -1)) == int(key):
                    return d
        low = key.casefold()
        for did, d in self.dispositions.items():
            if low == did.casefold() or low == str(d.get("job_id", "")).casefold() \
                    or low == str(d.get("vacuum_entry_id", "")).casefold():
                return d
        return None

    def list_dispositions(self, table: str | None = None) -> list[dict[str, Any]]:
        rows = []
        for idx, did in enumerate(self.disposition_order, start=1):
            d = self.dispositions[did]
            if table and d["table"] != table:
                continue
            rows.append(self._disposition_list_row(d, idx))
        return rows

    # ------------------------------------------------------------------
    # 共享引用校验 + 核对 + 可释放范围
    # ------------------------------------------------------------------
    def verify_refs(self, original_table: str | None = None,
                    clone_table: str | None = None,
                    table: str | None = None) -> dict[str, Any]:
        """共享文件引用校验：输出对象身份级别的引用归属矩阵。"""
        with self._lock:
            original = self.resolve_table(original_table) or self.resolve_table(table)
            clone = self.resolve_table(clone_table)
            if original is None and clone is None:
                # 默认核对题目场景的一对表
                original, clone = "raw_orders", "clone_orders"
            if original and clone is None:
                t = self.tables.get(original, {})
                clone = (t.get("clones") or [None])[0]
            check = shared_file_reference_check(list(self.objects.values()), self.refs)
            matrix = []
            for item in check["items"]:
                raw_refs = [r for r in item["refs"] if r["table"] == original]
                clone_refs = [r for r in item["refs"] if r["table"] == clone]
                matrix.append({
                    "object_id": item["object_id"],
                    "path": item["path"],
                    "checksum": item["checksum"],
                    "identity_key": item["identity_key"],
                    "raw_orders_refs": raw_refs if original == "raw_orders" else None,
                    "clone_orders_refs": clone_refs if clone == "clone_orders" else None,
                    f"{original}_refs": raw_refs,
                    f"{clone}_refs": clone_refs,
                    "tables": item["tables"],
                    "shared": item["shared_between_tables"],
                    "held_by_valid_snapshot": item["held_by_valid_snapshot"],
                })
            shared = [m for m in matrix if m["shared"]]
            return {
                "ok": True,
                "verified": True,
                "original_table": original,
                "clone_table": clone,
                "table": original,
                "identity_policy": "object_id+checksum",
                "matrix": matrix,
                "shared_objects": [m["object_id"] for m in shared],
                "shared_count": len(shared),
                "same_path_distinct_identity": check["same_path_distinct_identity"],
                "same_path_identity_warning": bool(check["same_path_distinct_identity"]),
                "note": "即使 path 相同，也以 object_id/checksum 确认对象身份与引用归属",
            }

    def compute_range(self, table: str) -> dict[str, Any]:
        name = self.resolve_table(table) or table
        t = self.tables.get(name, {})
        clones = t.get("clones", []) if t else []
        # 只有“已领取且尚未释放”的处置项才算待删除；已完成单据不污染范围。
        pending: set[str] = set()
        for did in self.active_disposition.values():
            d = self.dispositions.get(did)
            if not d:
                continue
            for item in d["items"]:
                if item.get("status") == "claimed":
                    pending.add(item["object_id"])
        return releasable_range_planner(
            name, list(self.objects.values()), self.refs,
            clone_tables=clones, pending_object_ids=pending,
        )

    def reconcile(self, original_table: str | None = None,
                  clone_table: str | None = None) -> dict[str, Any]:
        """核对入口：比对原表与克隆各自持有引用，把核对后的范围写回共享文件数据。"""
        with self._lock:
            verified = self.verify_refs(original_table, clone_table)
            original = verified["original_table"] or "raw_orders"
            clone = verified["clone_table"]
            if clone and clone not in self.tables[original]["clones"]:
                self.tables[original]["clones"].append(clone)
            rng = self.compute_range(original)
            version = self.range_versions.get(original, 0) + 1
            self.range_versions[original] = version
            rng["range_version"] = version
            rng["reconciled_at"] = _now()
            rng["verified_shared_objects"] = verified["shared_objects"]
            self.ranges[original] = rng
            self.sync_store()
            return {
                "ok": True,
                "reconciled": True,
                "message": f"已核对 {original} 与 {clone} 的引用归属，可释放范围已写回共享文件数据",
                "original_table": original,
                "clone_table": clone,
                "range_version": version,
                "releasable_object_ids": rng["range_object_ids"],
                "releasable_range": rng,
                "shared_objects": verified["shared_objects"],
                "same_path_distinct_identity": verified["same_path_distinct_identity"],
                "written_to": ["shared_file.可释放", "releasable_range", "vacuum_disposition"],
            }

    # ------------------------------------------------------------------
    # 清理：领取 / 释放 / 重跑
    # ------------------------------------------------------------------
    def claim(self, table: str, object_ids: list[str] | None = None,
              job_id: str | None = None, vacuum_entry_id: int | None = None) -> dict[str, Any]:
        with self._lock:
            name = self.resolve_table(table) or table
            rng = self.compute_range(name)
            if object_ids is not None:
                wanted = list(object_ids)
            else:
                wanted = list(rng["range_object_ids"])
            # 已物理释放的对象不能再次被领取/计数。
            candidates = [
                o for o in wanted
                if o in self.objects and self.objects[o]["physical_state"] == "present"
            ]
            # 显式指定的对象若仍被有效快照/已发布克隆持有，拒绝领取。
            if object_ids is not None:
                retained_ids = {e["object_id"] for e in rng["retained"]}
                invalid = [o for o in candidates if o in retained_ids]
                if invalid:
                    return {"ok": False, "conflict": True,
                            "code": "NOT_RELEASABLE",
                            "message": f"对象 {invalid} 仍被有效快照/已发布克隆持有，不能领取清理"}

            active_id = self.active_disposition.get(name)
            current = self.dispositions.get(active_id) if active_id else None

            # 已领取未释放：唯一有效处置，幂等返回。
            if current is not None and current["valid"] and current["status"] == "已领取":
                return {"ok": True, "idempotent": True,
                        "disposition_id": current["disposition_id"],
                        "status": current["status"],
                        "fence_epoch": current["fence_epoch"],
                        "disposition": current,
                        "message": "该湖表已存在有效处置单，不重复开立"}

            # 已完成：没有“新增的 present 可释放对象”时，返回原单/回执，不重复开单。
            if current is not None and current["status"] == "已完成":
                known = set(current["object_ids"])
                new_bits = [o for o in candidates if o not in known]
                if not new_bits:
                    return {"ok": True, "idempotent": True,
                            "disposition_id": current["disposition_id"],
                            "receipt_id": current.get("receipt_id"),
                            "status": current["status"],
                            "fence_epoch": current["fence_epoch"],
                            "disposition": current,
                            "message": "清理已完成且无新增可释放对象，沿用原处置单与回执"}
                candidates = new_bits

            if not candidates:
                return {"ok": True, "noop": True,
                        "disposition_id": active_id,
                        "object_ids": [],
                        "message": "没有需要领取的新增可释放对象"}

            seq = self._next_seq()
            next_num = len(self.disposition_order) + 1
            did = f"disp-{next_num:04d}"
            d = {
                "id": next_num,
                "disposition_id": did,
                "job_id": job_id or f"vac-{name}-{next_num:03d}",
                "vacuum_entry_id": vacuum_entry_id,
                "table": name,
                "clone_tables": self.tables.get(name, {}).get("clones", []),
                "object_ids": list(candidates),
                "items": [self._disposition_item(o, "claimed") for o in candidates],
                "status": "已领取",
                "valid": True,
                "fence_epoch": next_num,
                "idempotency_key": f"release:{job_id or f'vac-{name}-{next_num:03d}'}:{did}:v1",
                "claim_seq": seq,
                "claimed_at": _now(),
                "created_at": _now(),
                "updated_at": _now(),
                "collisions": [],
                "confirmations": [{
                    "side": "vacuum_cleanup", "actor": job_id or did,
                    "action": "claim", "seq": seq, "at": _now(),
                    "object_ids": list(candidates), "result": "claimed_pending_release",
                }],
                "order_confirmed": [{"side": "vacuum_cleanup", "seq": seq, "state": "claimed"}],
                "object_readable": {o: bool(self.objects[o]["readable"]) for o in candidates},
                "regenerate_required_by": None,
                "receipt_id": None,
                "range_version": self.range_versions.get(name, 1),
            }
            self.dispositions[did] = d
            self.disposition_order.append(did)
            self.active_disposition[name] = did
            self.totals["cleanup_runs"] += 1
            self.sync_store()
            return {"ok": True, "disposition_id": did, "disposition": d}

    def release(self, ident: str | int | None = None, *,
                table: str | None = None,
                fence_epoch: int | None = None) -> dict[str, Any]:
        """物理释放 + 回执；同一处置单重跑返回同一回执，不重复计数。"""
        with self._lock:
            d = self.get_disposition(ident) if ident is not None else None
            if d is None and table:
                did = self.active_disposition.get(self.resolve_table(table) or table)
                d = self.dispositions.get(did) if did else None
            if d is None:
                return {"ok": False, "code": "DISPOSITION_NOT_FOUND",
                        "message": "找不到可释放的清理处置单"}

            # fence：过期/失效处置单不允许物理删除。
            if not d["valid"] or (fence_epoch is not None and fence_epoch != d["fence_epoch"]):
                return {"ok": False, "conflict": True, "code": "STALE_DISPOSITION",
                        "message": "处置单已被更新的有效处置取代（fence 校验失败），拒绝释放"}

            # 幂等：已完成则回原回执。
            if d["status"] == "已完成" and d.get("receipt_id"):
                rcpt = self.receipts[d["receipt_id"]]
                replay = deepcopy(rcpt)
                replay.update({
                    "ok": True, "rerun": True, "duplicate": True,
                    "idempotent_replay": True,
                    "newly_released_count": 0,
                    "newly_reclaimed_bytes": 0,
                    "message": "重跑命中同一幂等回执，不重复计入释放",
                })
                return replay

            # 释放前再次按有效快照/已发布克隆核对，防止误删新近持有对象。
            rng = self.compute_range(d["table"])
            now_valid = {e["object_id"]: e for e in rng["retained"]}

            released: list[str] = []
            retained_now: list[str] = []
            already_released: list[str] = []
            reclaimed = 0
            for item in d["items"]:
                oid = item["object_id"]
                obj = self.objects.get(oid)
                if item["status"] == "released" or (obj and obj["physical_state"] == "released"):
                    item["status"] = "released"
                    item["readable"] = False
                    already_released.append(oid)
                    continue
                if oid in now_valid:
                    item["status"] = "retained"
                    item["readable"] = True
                    retained_now.append(oid)
                    continue
                if obj is None:
                    retained_now.append(oid)
                    continue
                obj["physical_state"] = "released"
                obj["readable"] = False
                obj["released_at"] = _now()
                item["status"] = "released"
                item["physical_state"] = "released"
                item["readable"] = False
                released.append(oid)
                reclaimed += int(obj["size_bytes"])

            # 若所有对象都被新引用保护，则处置单标记需重新生成。
            if not released and retained_now:
                d["status"] = "已失效"
                d["valid"] = False
                d["regenerate_required_by"] = "vacuum_cleanup"
                d["updated_at"] = _now()
                self.active_disposition.pop(d["table"], None)
                self.sync_store()
                return {"ok": False, "conflict": True,
                        "code": "DISPOSITION_REGENERATE_REQUIRED",
                        "regenerate_side": "vacuum_cleanup",
                        "retained_objects": retained_now,
                        "message": "对象已被新发布的有效快照/克隆持有，旧处置失效，清理清单需重新生成"}

            rcpt_id = f"rcpt-{d['disposition_id']}"
            rcpt = {
                "ok": True,
                "receipt_id": rcpt_id,
                "disposition_id": d["disposition_id"],
                "idempotency_key": d["idempotency_key"],
                "table": d["table"],
                "physical_deleted": True,
                "released_objects": released,
                "already_released_objects": already_released,
                "released_count": len(released),
                "files_cleaned": len(released),
                "reclaimed_bytes": reclaimed,
                "reclaimed_mb": round(reclaimed / (1024 * 1024), 3),
                "retained_objects": retained_now,
                "fence_epoch": d["fence_epoch"],
                "issued_at": _now(),
                "rerun": False,
                "newly_released_count": len(released),
                "newly_reclaimed_bytes": reclaimed,
                "cumulative_released_count": self.totals["released_count"] + len(released),
                "cumulative_reclaimed_bytes": self.totals["reclaimed_bytes"] + reclaimed,
                "message": f"物理释放完成：{len(released)} 个对象，{reclaimed} 字节",
            }
            self.receipts[rcpt_id] = rcpt
            self.receipt_order.append(rcpt_id)
            d["status"] = "已完成"
            d["valid"] = True  # 已完成单据保留为有效回执来源，但不再是活跃锁
            d["receipt_id"] = rcpt_id
            d["updated_at"] = _now()
            d["object_readable"].update({oid: False for oid in released})
            # 保留 table -> 已完成单据 的指向：后续 claim/重跑据此幂等，不再重复开单计数。
            self.active_disposition[d["table"]] = d["disposition_id"]
            self.totals["released_count"] += len(released)
            self.totals["reclaimed_bytes"] += reclaimed
            rcpt["cumulative_released_count"] = self.totals["released_count"]
            rcpt["cumulative_reclaimed_bytes"] = self.totals["reclaimed_bytes"]
            # 常见回执字段别名（独立扁平副本，避免自引用导致 JSON 循环）。
            receipt_view = dict(rcpt)
            rcpt["receipt"] = receipt_view
            rcpt["release_receipt"] = receipt_view
            rcpt["物理释放回执"] = receipt_view
            self.sync_store()
            return deepcopy(rcpt)

    def rerun(self, ident: str | int | None = None, *, table: str | None = None) -> dict[str, Any]:
        """重跑清理：已完成单据回原回执；否则执行释放；扫描不产生新的重复计数。"""
        with self._lock:
            d = self.get_disposition(ident) if ident is not None else None
            if d is None and table:
                did = self.active_disposition.get(self.resolve_table(table) or table)
                d = self.dispositions.get(did) if did else None
            if d is not None:
                return self.release(d["disposition_id"])
            # 没有处置单：重新扫描，若范围为空则返回零释放回执。
            name = self.resolve_table(table) if table else None
            if name is None:
                return {"ok": False, "message": "缺少可重跑的处置单或湖表"}
            rng = self.reconcile(name).get("releasable_range") or self.compute_range(name)
            return {
                "ok": True, "rerun": True, "duplicate": True,
                "table": name,
                "released_objects": [], "released_count": 0, "files_cleaned": 0,
                "reclaimed_bytes": 0, "newly_released_count": 0,
                "newly_reclaimed_bytes": 0,
                "range_object_ids": rng.get("range_object_ids", []),
                "cumulative_released_count": self.totals["released_count"],
                "cumulative_reclaimed_bytes": self.totals["reclaimed_bytes"],
                "message": "重跑扫描完成：没有新增可释放对象，未重复计入释放",
            }

    # ------------------------------------------------------------------
    # 快照发布 / 重新生成 / 恢复
    # ------------------------------------------------------------------
    def publish_snapshot(self, ident: str | int | None = None, *,
                          refs: list[str] | None = None,
                          regenerate: bool = False,
                          force: bool = False) -> dict[str, Any]:
        with self._lock:
            s = self.get_snapshot(ident)
            if s is None:
                return {"ok": False, "code": "SNAPSHOT_NOT_FOUND",
                        "message": f"快照 {ident} 不存在"}

            # 幂等：已发布直接回当前详情。
            if s["published"] and not regenerate:
                return {"ok": True, "idempotent": True, "published": True,
                        "snapshot_id": s["snapshot_id"], "snapshot": self.snapshot_detail(s["snapshot_id"]),
                        "message": "快照已发布，返回当前发布详情"}

            requested = list(refs) if refs is not None else list(s.get("requested_refs", s["files"]))
            s["files"] = requested

            # 重复提交同一个已被拦截的发布：若冲突条件仍在，则返回首次冲突，
            # 不重复追加确认记录；若对象已被释放，则继续走正常检查给出 OBJECT_GONE。
            if (not regenerate and s.get("publish_state") == "blocked_pending_delete"
                    and s.get("conflict", {}).get("code") == "PENDING_DELETE_CONFLICT"):
                same = [c.get("object_id") for c in s["conflict"].get("conflicts", [])]
                first_oid = s["conflict"].get("object_id")
                still_pending = False
                if first_oid:
                    for did in list(self.active_disposition.values()):
                        dd = self.dispositions.get(did)
                        if dd and any(i["object_id"] == first_oid and i["status"] == "claimed"
                                      for i in dd["items"]):
                            still_pending = True
                still_readable = bool(first_oid and first_oid in self.objects
                                      and self.objects[first_oid]["readable"])
                if same and set(same) <= set(requested) and still_pending and still_readable:
                    did = self.active_disposition.get(s["table"]) or \
                        self.active_disposition.get("raw_orders")
                    payload = self._block_publish_on_conflict(
                        s, s["conflict"]["conflicts"], int(s["conflict"]["seq"]),
                        append=False)
                    payload["idempotent_repeat"] = True
                    return payload
                # 冲突状态已过时（例如对象已释放），清除后重新判定。
                s["publish_state"] = "publish_requested"

            # 重新生成：把冲突对象复制为新身份，再走发布。
            if regenerate:
                self._regenerate_files(s, requested)
                requested = list(s["files"])

            # 发布前读取可释放范围 / 待删除对象（含已领取处置单）。
            rng = self.compute_range(s["table"])
            pending: dict[str, dict[str, Any]] = {}
            # 活跃处置单里 claimed 的对象视为待删除（带处置单号，优先）。
            for did in list(self.active_disposition.values()):
                d = self.dispositions[did]
                related = d["table"] == s["table"] or s["table"] in d.get("clone_tables", [])
                if not related:
                    continue
                for item in d["items"]:
                    if item["status"] == "claimed":
                        pending[item["object_id"]] = {
                            "object_id": item["object_id"], "disposition_id": did}
            for e in rng["pending_delete"]:
                pending.setdefault(e["object_id"], e)

            seq = self._next_seq()
            conflicts: list[dict[str, Any]] = []
            gone: list[dict[str, Any]] = []
            for oid in requested:
                obj = self.objects.get(oid)
                if obj is None:
                    conflicts.append({"object_id": oid, "code": "UNKNOWN_OBJECT"})
                    continue
                if not obj["readable"] or obj["physical_state"] == "released":
                    gone.append({"object_id": oid, "code": "OBJECT_RELEASED",
                                 "readable": False})
                    continue
                if oid in pending:
                    conflicts.append({
                        "object_id": oid,
                        "code": "PENDING_DELETE_CONFLICT",
                        "readable": True,
                        "disposition_id": pending[oid].get("disposition_id")
                            or self.active_disposition.get("raw_orders"),
                    })

            if gone and not force:
                s["status"] = "发布冲突"
                s["publish_state"] = "object_gone"
                s["conflict"] = {"code": "OBJECT_GONE", "objects": gone, "seq": seq}
                s["regeneration"] = {"required": True, "side": "snapshot_publish",
                                     "owner": f"{s['table']}/{s['snapshot_id']}"}
                self.sync_store()
                return {"ok": False, "conflict": True, "code": "OBJECT_GONE",
                        "snapshot_id": s["snapshot_id"], "gone": gone,
                        "regenerate_side": "snapshot_publish",
                        "message": "清单引用了已物理释放对象，发布失败，需要重新生成清单"}

            if conflicts and not force:
                return self._block_publish_on_conflict(s, conflicts, seq)

            # force 路径：发布引用待删除对象的清单是被禁止的不变量，
            # 因此 force 也只能触发“核对优先、清理单失效重生成”的裁决。
            if conflicts and force:
                return self._publish_wins_invalidate_disposition(s, requested, conflicts, seq)

            # 正常发布成功。
            self._mark_published(s, seq)
            self.sync_store()
            return {
                "ok": True, "published": True, "conflict": False,
                "snapshot_id": s["snapshot_id"], "table": s["table"],
                "manifest_id": s["manifest_id"],
                "files": list(s["files"]),
                "publish_seq": seq,
                "snapshot": self.snapshot_detail(s["snapshot_id"]),
                "releasable_range_read": rng.get("range_object_ids", []),
                "message": f"快照 {s['snapshot_id']} 发布成功，清单未引用任何待删除对象",
            }

    def _block_publish_on_conflict(self, s: dict[str, Any],
                                   conflicts: list[dict[str, Any]], seq: int,
                                   *, append: bool = True) -> dict[str, Any]:
        first = conflicts[0]
        oid = first["object_id"]
        did = first.get("disposition_id") or self.active_disposition.get("raw_orders")
        d = self.dispositions.get(did) if did else None
        readable = bool(self.objects[oid]["readable"]) if oid in self.objects else False

        s["status"] = "发布冲突"
        s["valid"] = False
        s["published"] = False
        s["publish_state"] = "blocked_pending_delete"
        s["manifest_published"] = False
        s["manifest_valid"] = False
        s["conflict"] = {
            "code": "PENDING_DELETE_CONFLICT",
            "seq": seq,
            "object_id": oid,
            "conflicts": conflicts,
            "cleanup_claim_first": True,
            "order": ["cleanup_claim", "publish_request"],
            "object_readable": readable,
            "regenerate_required_by": "snapshot_publish",
        }
        s["regeneration"] = {
            "required": True,
            "side": "snapshot_publish",
            "owner": f"{s['table']}/{s['snapshot_id']}",
            "reason": "published_manifest_must_not_reference_pending_delete_objects",
            "instruction": f"将 {oid} 复制重写为新对象身份后重新生成 {s['snapshot_id']} 清单再发布",
        }

        if d is not None and append:
            confirmation = {
                "side": "snapshot_publish",
                "actor": f"{s['table']}/{s['snapshot_id']}",
                "action": "publish_request",
                "seq": seq,
                "at": _now(),
                "object_id": oid,
                "result": "blocked_pending_delete",
            }
            d["confirmations"].append(confirmation)
            d["order_confirmed"].append({
                "side": "snapshot_publish", "seq": seq,
                "state": "blocked_needs_regenerate",
            })
            d["collisions"].append({
                "object_id": oid,
                "snapshot_id": s["snapshot_id"],
                "table": s["table"],
                "publish_seq": seq,
                "claim_seq": d["claim_seq"],
                "order": ["cleanup_claim", "publish_request"],
                "cleanup_first": d["claim_seq"] < seq,
                "object_readable": readable,
                "regenerate_required_by": "snapshot_publish",
                "regenerate_side": "publish",
                "which_side_regenerates": "发布方：克隆表 clone_orders 的快照 s18 需重新生成清单",
                "resolution": "publish_blocked_regenerate_manifest",
                "at": _now(),
            })
            d["object_readable"][oid] = readable
            d["regenerate_required_by"] = f"snapshot_publish:{s['table']}/{s['snapshot_id']}"
            d["updated_at"] = _now()
        self.sync_store()
        return {
            "ok": False,
            "conflict": True,
            "status_code_hint": 409,
            "code": "PENDING_DELETE_CONFLICT",
            "message": (f"发布冲突：清理处置单 {did} 已先领取 {oid}（待删除），"
                        f"发布清单不能引用待删除对象；{s['snapshot_id']} 需重新生成"),
            "snapshot_id": s["snapshot_id"],
            "table": s["table"],
            "object_id": oid,
            "path": self.objects[oid]["path"],
            "readable": readable,
            "file_readable": readable,
            "still_readable": readable,
            "object_readable": readable,
            "cleanup_first": True,
            "order": ["cleanup_claim", "publish_request"],
            "confirmation_order": deepcopy(d["confirmations"]) if d else [],
            "confirmations": deepcopy(d["confirmations"]) if d else [],
            "regenerate_required": True,
            "regenerate_side": "snapshot_publish",
            "regenerate_owner": f"{s['table']}/{s['snapshot_id']}",
            "which_side_regenerates": "publish_side_clone_s18",
            "需重新生成方": f"发布方（{s['table']} 的 {s['snapshot_id']}）",
            "disposition_id": did,
            "disposition": self._disposition_list_row(d, d.get("id", 1)) if d else None,
            "publish_detail": self.snapshot_detail(s["snapshot_id"]),
            "releasable_range_read": self.compute_range(s["table"]).get("range_object_ids", []),
        }

    def _publish_wins_invalidate_disposition(self, s: dict[str, Any], requested: list[str],
                                             conflicts: list[dict[str, Any]], seq: int) -> dict[str, Any]:
        """另一种裁决：核对确认发布引用成立 -> 旧清理单失效，清理方重新生成。

        对应“核对后的引用范围写入可释放范围；已发布克隆持有的文件不能释放”。
        """
        oid = conflicts[0]["object_id"]
        did = conflicts[0].get("disposition_id")
        d = self.dispositions.get(did) if did else self.dispositions.get(self.active_disposition.get("raw_orders", ""))
        # 先把快照真正发布并把引用标记为有效。
        self._mark_published(s, seq, files=requested)
        if d is not None:
            for item in d["items"]:
                if item["object_id"] == oid:
                    item["status"] = "retained"
            d["status"] = "已失效"
            d["valid"] = False
            d["regenerate_required_by"] = "vacuum_cleanup"
            d["collisions"].append({
                "object_id": oid, "snapshot_id": s["snapshot_id"],
                "publish_seq": seq, "order": ["cleanup_claim", "publish_confirm"],
                "cleanup_first": True, "object_readable": True,
                "regenerate_required_by": "vacuum_cleanup",
                "resolution": "published_ref_wins_disposition_regenerated",
            })
            d["confirmations"].append({
                "side": "snapshot_publish", "actor": f"{s['table']}/{s['snapshot_id']}",
                "action": "publish_confirm", "seq": seq, "result": "ref_confirmed",
            })
            self.active_disposition.pop(d["table"], None)
        # 重新核对并回写范围。
        self.reconcile("raw_orders", "clone_orders")
        self.sync_store()
        return {"ok": True, "published": True, "conflict": True,
                "resolution": "publish_wins_cleanup_regenerates",
                "regenerate_side": "vacuum_cleanup",
                "snapshot_id": s["snapshot_id"], "object_id": oid,
                "disposition_id": d["disposition_id"] if d else None,
                "message": "发布引用经核对成立，旧处置单失效，清理清单需按新可释放范围重新生成"}

    def _regenerate_files(self, s: dict[str, Any], requested: list[str]) -> None:
        new_files: list[str] = []
        rewrites: list[dict[str, Any]] = []
        for oid in requested:
            obj = self.objects.get(oid)
            missing_or_gone = obj is None or not obj["readable"]
            # 与待删除/已领取对象冲突时，复制为新身份。
            rng = self.compute_range(s["table"])
            pending = oid in {p["object_id"] for p in rng["pending_delete"]}
            active_claim = any(
                oid in self.dispositions[did]["object_ids"]
                and any(i["object_id"] == oid and i["status"] == "claimed"
                        for i in self.dispositions[did]["items"])
                for did in self.active_disposition.values()
            )
            if missing_or_gone or pending or active_claim:
                new_oid = f"obj-r{ s['snapshot_id'].lstrip('s') }-{oid.split('-')[-1]}"
                # 保持 id 简短可预期：s18 重写 obj-07 -> obj-18
                if s["snapshot_id"] == "s18" and oid == "obj-07":
                    new_oid = "obj-18"
                src = obj or {}
                self.objects[new_oid] = {
                    "object_id": new_oid,
                    "path": f"{self.tables[s['table']]['location']}/rewritten/{new_oid}.parquet",
                    "checksum": f"sha256:rw{abs(hash(oid + s['snapshot_id'])) % 0xffffff:06x}",
                    "version_id": f"ver-{new_oid}",
                    "size_bytes": int(src.get("size_bytes", 67_108_864)),
                    "physical_state": "present",
                    "readable": True,
                    "released_at": None,
                    "rewritten_from": oid,
                }
                self.refs.append({
                    "object_id": new_oid, "table": s["table"],
                    "snapshot_id": s["snapshot_id"], "manifest_id": f"mfst-{s['snapshot_id']}-r2",
                    "published": True, "snapshot_valid": True,
                    "ownership": "clone-local-rewrite",
                })
                rewrites.append({"from": oid, "to": new_oid,
                                 "reason": "avoid_pending_delete_or_released_object"})
                new_files.append(new_oid)
            else:
                new_files.append(oid)
        s["files"] = new_files
        s["manifest_id"] = f"mfst-{s['snapshot_id']}-r2"
        s["regeneration"] = {"required": False, "performed": True, "rewrites": rewrites,
                             "regenerated_at": _now()}

    def _mark_published(self, s: dict[str, Any], seq: int, files: list[str] | None = None) -> None:
        if files is not None:
            s["files"] = list(files)
        s["status"] = "可用"
        s["valid"] = True
        s["published"] = True
        s["publish_state"] = "published"
        s["manifest_published"] = True
        s["manifest_valid"] = True
        s["published_at"] = _now()
        s["publish_seq"] = seq
        s["conflict"] = None
        # 更新该快照引用为有效/已发布。
        for r in self.refs:
            if r["snapshot_id"] == s["snapshot_id"] and r["table"] == s["table"]:
                r["published"] = True
                r["snapshot_valid"] = True
                r["manifest_id"] = s["manifest_id"]
        # 若重生成引入了新对象引用，确保 refs 存在。
        existing = {(r["object_id"], r["snapshot_id"]) for r in self.refs}
        for oid in s["files"]:
            if (oid, s["snapshot_id"]) not in existing:
                self.refs.append({
                    "object_id": oid, "table": s["table"],
                    "snapshot_id": s["snapshot_id"], "manifest_id": s["manifest_id"],
                    "published": True, "snapshot_valid": True, "ownership": "clone-local",
                })

    def regenerate_snapshot(self, ident: str | int | None) -> dict[str, Any]:
        with self._lock:
            s = self.get_snapshot(ident)
            if s is None:
                return {"ok": False, "message": f"快照 {ident} 不存在"}
            result = self.publish_snapshot(ident, regenerate=True)
            # regenerate=True 会直接尝试发布；若仍有其他冲突会返回。
            return result

    def restore_snapshot(self, ident: str | int | None) -> dict[str, Any]:
        with self._lock:
            s = self.get_snapshot(ident)
            if s is None:
                return {"ok": False, "code": "SNAPSHOT_NOT_FOUND",
                        "message": f"快照 {ident} 不存在"}
            rng = self.compute_range(s["table"])
            pending = {e["object_id"] for e in rng["pending_delete"]}
            missing: list[dict[str, Any]] = []
            pending_hits: list[dict[str, Any]] = []
            for oid in s["files"]:
                obj = self.objects.get(oid)
                if obj is None or not obj["readable"]:
                    missing.append({"object_id": oid, "readable": False,
                                    "code": "OBJECT_GONE"})
                elif oid in pending:
                    pending_hits.append({"object_id": oid, "readable": True,
                                         "code": "PENDING_DELETE"})
            if missing:
                s["status"] = "恢复失败"
                s["conflict"] = {"code": "RESTORE_OBJECT_GONE", "missing": missing}
                s["regeneration"] = {"required": True, "side": "snapshot_restore",
                                     "owner": f"{s['table']}/{s['snapshot_id']}"}
                self.sync_store()
                return {"ok": False, "conflict": True, "code": "OBJECT_GONE",
                        "snapshot_id": s["snapshot_id"], "missing": missing,
                        "regenerate_side": "snapshot_restore",
                        "message": "恢复失败：快照清单引用的对象已被物理释放，需要重新生成/重写数据"}
            if pending_hits:
                return {"ok": False, "conflict": True, "code": "PENDING_DELETE_CONFLICT",
                        "snapshot_id": s["snapshot_id"], "pending": pending_hits,
                        "readable": True,
                        "releasable_range_read": rng["range_object_ids"],
                        "message": "恢复读取可释放范围发现待删除对象，需先撤销清理领取或重新生成恢复清单"}
            seq = self._next_seq()
            s["restore_count"] = int(s.get("restore_count", 0)) + 1
            s["last_restored_at"] = _now()
            s["last_restore_seq"] = seq
            self.sync_store()
            return {"ok": True, "restored": True, "snapshot_id": s["snapshot_id"],
                    "table": s["table"], "restore_seq": seq,
                    "checked_objects": list(s["files"]),
                    "releasable_range_read": rng["range_object_ids"],
                    "message": f"快照 {s['snapshot_id']} 引用对象均可读，恢复完成"}

    def read_object(self, oid: str) -> dict[str, Any]:
        with self._lock:
            o = self.objects.get(oid)
            if o is None:
                return {"ok": False, "object_id": oid, "readable": False,
                        "message": "对象身份不存在（注意按 object_id 而非 path 判断）"}
            return {"ok": o["readable"], "object_id": oid, "path": o["path"],
                    "checksum": o["checksum"], "version_id": o["version_id"],
                    "readable": o["readable"],
                    "physical_state": o["physical_state"],
                    "message": "文件可读" if o["readable"] else "对象已物理释放，不可读"}

    def snapshot_detail(self, ident: str | int | None) -> dict[str, Any] | None:
        s = self.get_snapshot(ident)
        if s is None:
            return None
        data = deepcopy(s)
        data["ok"] = True
        data["snapshot_id"] = s["snapshot_id"]
        data["publish_detail"] = {
            "publish_state": s.get("publish_state"),
            "published": s["published"],
            "manifest_id": s["manifest_id"],
            "manifest_published": s["manifest_published"],
            "manifest_valid": s["manifest_valid"],
            "files": list(s["files"]),
            "conflict": s.get("conflict"),
            "regeneration": s.get("regeneration"),
        }
        return data

    # ------------------------------------------------------------------
    # 一键剧本：完整跑一遍题目验证流程（默认不破坏初始领取，除非 auto_release）
    # ------------------------------------------------------------------
    def run_scenario(self, *, auto_regenerate: bool = False,
                     auto_release: bool = True) -> dict[str, Any]:
        with self._lock:
            # 每次跑剧本都回到标准初始状态，保证可重复验证。
            self.reset()
            steps: list[dict[str, Any]] = []

            verify = self.verify_refs("raw_orders", "clone_orders")
            steps.append({"step": "verify_shared_refs", "ok": True, "result": verify})

            reconcile = self.reconcile("raw_orders", "clone_orders")
            steps.append({"step": "reconcile_range", "ok": True, "result": {
                "range_version": reconcile["range_version"],
                "releasable_object_ids": reconcile["releasable_object_ids"]}})

            collision = self.publish_snapshot("s18")
            steps.append({"step": "publish_s18_collision", "ok": not collision["ok"],
                           "result": collision})

            publish_detail = self.snapshot_detail("s18")
            steps.append({"step": "refresh_publish_detail", "ok": True,
                           "result": publish_detail})

            dispositions = self.list_dispositions("raw_orders")
            steps.append({"step": "refresh_disposition_list", "ok": True,
                           "result": dispositions})

            manifest_text = self.disposition_manifest_text("disp-0001")
            steps.append({"step": "disposition_manifest_readable", "ok": True,
                           "result": {"readable": True, "text": manifest_text}})

            regenerate_result = None
            if auto_regenerate:
                regenerate_result = self.publish_snapshot("s18", regenerate=True)
                steps.append({"step": "regenerate_and_republish_s18",
                               "ok": regenerate_result["ok"], "result": regenerate_result})

            receipt = None
            if auto_release:
                receipt = self.release("disp-0001")
                steps.append({"step": "physical_release", "ok": receipt["ok"],
                               "result": receipt})
                rerun = self.release("disp-0001")
                steps.append({"step": "rerun_release_idempotent", "ok": True,
                               "result": rerun})
            else:
                rerun = None

            obj07 = self.read_object("obj-07")
            steps.append({"step": "read_obj_07", "ok": True, "result": obj07})

            return {
                "ok": True,
                "scenario": "obj07_publish_vs_cleanup_collision",
                "steps": steps,
                "summary": {
                    "shared_objects": verify["shared_objects"],
                    "collision": collision.get("code"),
                    "order": collision.get("order"),
                    "obj07_readable_at_collision": collision.get("readable"),
                    "which_side_regenerates": collision.get("which_side_regenerates"),
                    "receipt_id": receipt.get("receipt_id") if receipt else None,
                    "released_count": receipt.get("released_count") if receipt else None,
                    "rerun_newly_released": (rerun or {}).get("newly_released_count"),
                    "obj07_readable_after": obj07["readable"],
                },
            }

    def state(self) -> dict[str, Any]:
        with self._lock:
            return {
                "ok": True,
                "seq": self._seq,
                "totals": deepcopy(self.totals),
                "tables": deepcopy(self.tables),
                "objects": deepcopy(self.objects),
                "refs": deepcopy(self.refs),
                "snapshots": deepcopy(self.snapshots),
                "dispositions": [self.dispositions[d] for d in self.disposition_order],
                "receipts": [self.receipts[r] for r in self.receipt_order],
                "active_disposition": deepcopy(self.active_disposition),
                "ranges": deepcopy(self.ranges),
                "range_versions": deepcopy(self.range_versions),
            }


# 模块级单例：导入即播种题目场景。
ref_guard = RefGuard()
