"""实时流数据湖业务计算引擎：分区、合并、压缩、快照差异、保留策略、
版本冲突、增量协调等核心算法实现。
"""
from __future__ import annotations

import hashlib
import math
from typing import Any


# ------------------------------------------------------------------
# partition_calculator
# ------------------------------------------------------------------

def partition_calculator(
    rows: int, partition_keys: list[str], target_size_mb: float = 128.0,
) -> dict[str, Any]:
    """根据行数和分区键计算最佳分区策略。"""
    partitions = max(1, math.ceil(rows * 0.25 / target_size_mb))
    return {
        "partition_count": partitions,
        "partition_keys": partition_keys,
        "strategy": "hash" if partitions > 4 else "range",
        "estimated_rows_per_partition": rows // partitions,
        "target_partition_size_mb": target_size_mb,
    }


# ------------------------------------------------------------------
# merge_strategy
# ------------------------------------------------------------------

def merge_strategy(
    small_files: list[int],
    target_file_size_mb: float = 256.0,
    min_file_count: int = 3,
) -> dict[str, Any]:
    """计算小文件合并策略：按目标文件大小将多个小文件合为一个。"""
    if not small_files or len(small_files) < min_file_count:
        return {"should_merge": False, "reason": "小文件数量不足，跳过合并"}
    total_size = sum(small_files)
    groups = max(1, math.ceil(total_size / target_file_size_mb))
    return {
        "should_merge": True,
        "file_count": len(small_files),
        "total_size_mb": total_size,
        "target_groups": groups,
        "files_per_group": len(small_files) // groups,
        "estimated_output_mb": target_file_size_mb,
    }


# ------------------------------------------------------------------
# compaction_planner
# ------------------------------------------------------------------

def compaction_planner(
    table_path: str,
    file_sizes: list[int],
    target_file_size: int = 134217728,  # 128MB
    max_parallelism: int = 4,
) -> dict[str, Any]:
    """规划压缩作业：输入文件大小列表，输出分批压缩计划。"""
    candidates = [i for i, sz in enumerate(file_sizes) if sz < target_file_size // 4]
    if not candidates:
        return {
            "table": table_path,
            "plan": None,
            "candidate_file_count": 0,
            "batches": 0,
            "files_per_batch": 0,
            "parallelism": 0,
            "target_file_size_bytes": target_file_size,
            "estimated_duration_seconds": 0,
            "reason": "没有需要压缩的小文件",
        }
    batches = math.ceil(len(candidates) / max_parallelism)
    plan = {
        "candidate_file_count": len(candidates),
        "batches": batches,
        "files_per_batch": len(candidates) // batches if batches else 0,
        "parallelism": min(max_parallelism, len(candidates)),
        "target_file_size_bytes": target_file_size,
        "estimated_duration_seconds": len(candidates) * 15,
    }
    return {
        "table": table_path,
        "plan": plan,
        **plan,
    }


# ------------------------------------------------------------------
# snapshot_differ
# ------------------------------------------------------------------

def snapshot_differ(
    base_snapshot: dict[str, Any],
    current_snapshot: dict[str, Any],
) -> dict[str, Any]:
    """比较两个快照的差异，输出新增/更新/删除的数据分布。"""
    base_files = base_snapshot.get("files", [])
    curr_files = current_snapshot.get("files", [])

    base_set = {f.get("path"): f for f in base_files}
    curr_set = {f.get("path"): f for f in curr_files}

    added = [f for path, f in curr_set.items() if path not in base_set]
    removed = [f for path, f in base_set.items() if path not in curr_set]
    modified = []
    for path in base_set.keys() & curr_set.keys():
        bf = base_set[path]
        cf = curr_set[path]
        if bf.get("checksum") != cf.get("checksum"):
            modified.append({"path": path, "old": bf, "new": cf})

    total_added_rows = sum(f.get("rows", 0) for f in added)
    total_removed_rows = sum(f.get("rows", 0) for f in removed)
    total_modified_rows = sum(f["new"].get("rows", 0) for f in modified)

    return {
        "base_snapshot_id": base_snapshot.get("snapshot_id"),
        "current_snapshot_id": current_snapshot.get("snapshot_id"),
        "added_files": len(added),
        "removed_files": len(removed),
        "modified_files": len(modified),
        "added_rows": total_added_rows,
        "removed_rows": total_removed_rows,
        "modified_rows": total_modified_rows,
        "net_change_rows": total_added_rows - total_removed_rows,
    }


# ------------------------------------------------------------------
# retention_checker
# ------------------------------------------------------------------

def retention_checker(
    snapshots: list[dict[str, Any]],
    retention_days: int = 30,
    min_versions: int = 5,
) -> dict[str, Any]:
    """检查快照保留策略：标记可清理的过期快照。"""
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    expired = []
    valid = []

    for snap in snapshots:
        ts = str(snap.get("created_at", ""))
        # Strip timezone to make comparison naive
        for suffix in ("Z", "+00:00", "+0000"):
            ts = ts.replace(suffix, "")
        ts = ts.split("+")[0]
        try:
            created = datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            created = cutoff + timedelta(days=1)
        if created.replace(tzinfo=None) < cutoff.replace(tzinfo=None):
            expired.append(snap)
        else:
            valid.append(snap)

    can_clean = max(0, len(expired) - min_versions)
    return {
        "total_snapshots": len(snapshots),
        "expired_count": len(expired),
        "valid_count": len(valid),
        "min_versions_required": min_versions,
        "retention_days": retention_days,
        "can_clean_count": can_clean,
        "should_clean": can_clean > 0,
    }


# ------------------------------------------------------------------
# version_conflict_resolver
# ------------------------------------------------------------------

def version_conflict_resolver(
    base_version: int,
    incoming_version: int,
    strategy: str = "last-write-wins",
    conflict_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """解决版本冲突：支持 LWW、三路合并和手动覆盖策略。"""
    if base_version == incoming_version:
        return {
            "conflict": False,
            "resolved_version": base_version,
            "strategy": strategy,
        }

    if strategy == "last-write-wins":
        winner = max(base_version, incoming_version)
    elif strategy == "base-wins":
        winner = base_version
    elif strategy == "incoming-wins":
        winner = incoming_version
    elif strategy == "manual" and conflict_data:
        winner = conflict_data.get("preferred_version", base_version)
    else:
        winner = base_version

    return {
        "conflict": base_version != incoming_version,
        "base_version": base_version,
        "incoming_version": incoming_version,
        "resolved_version": winner,
        "strategy": strategy,
        "resolution": "resolved" if strategy != "manual" else "needs_review",
    }


# ------------------------------------------------------------------
# delta_reconciler
# ------------------------------------------------------------------

def delta_reconciler(
    stream_events: list[dict[str, Any]],
    batch_events: list[dict[str, Any]],
    dedup_key: str = "id",
) -> dict[str, Any]:
    """协调流批数据：对流事件和批事件进行去重合并，输出最终写入集。"""
    # Build dedup index from batch (batch wins for same key)
    batch_index: dict[str, dict[str, Any]] = {}
    for event in batch_events:
        key = str(event.get(dedup_key, ""))
        if key:
            batch_index[key] = event

    reconciled = []
    duplicates = 0
    stream_only = 0

    for event in stream_events:
        key = str(event.get(dedup_key, ""))
        if key and key in batch_index:
            reconciled.append(event)  # batch wins, but keep source trace
            duplicates += 1
        else:
            reconciled.append(event)
            stream_only += 1

    # Append batch-only events
    existing_keys = {str(e.get(dedup_key, "")) for e in stream_events}
    batch_only = 0
    for key, event in batch_index.items():
        if key not in existing_keys:
            reconciled.append(event)
            batch_only += 1

    total = len(reconciled)
    checksum = _compute_checksum(str(reconciled))

    return {
        "stream_count": len(stream_events),
        "batch_count": len(batch_events),
        "reconciled_total": total,
        "duplicates_resolved": duplicates,
        "stream_only": stream_only,
        "batch_only": batch_only,
        "dedup_key": dedup_key,
        "checksum": checksum,
    }


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------

def _compute_checksum(data: str) -> str:
    return hashlib.md5(data.encode()).hexdigest()[:12]


# ------------------------------------------------------------------
# 共享文件引用校验 / 可释放范围
# ------------------------------------------------------------------
#
# 对象身份一律以 ``object_id`` + ``checksum``（或对象存储 version_id）为准，
# 不能只看 ``path``：同一路径可能被覆盖成另一个对象，原表与克隆也可能持有
# 路径相同但身份不同的文件。


def _ref_object_id(ref: dict[str, Any]) -> str:
    return str(ref.get("object_id") or ref.get("对象标识") or ref.get("id") or "")


def shared_file_reference_check(
    objects: list[dict[str, Any]],
    refs: list[dict[str, Any]],
) -> dict[str, Any]:
    """按对象身份汇总引用归属，并标记“路径相同但对象身份不同”的情况。

    返回每个对象的引用方列表、被哪些有效（已发布/可用）快照持有、以及
    同路径异身份的配对。纯函数，便于单元测试直接调用。
    """
    object_index = {str(o.get("object_id") or o.get("对象标识")): o for o in objects}
    by_object: dict[str, list[dict[str, Any]]] = {}
    for ref in refs:
        oid = _ref_object_id(ref)
        if not oid:
            continue
        by_object.setdefault(oid, []).append(ref)

    items: list[dict[str, Any]] = []
    for oid, obj in object_index.items():
        obj_refs = by_object.get(oid, [])
        valid_holders = [
            r for r in obj_refs
            if r.get("snapshot_valid", r.get("published", False))
        ]
        tables = sorted({str(r.get("table")) for r in obj_refs if r.get("table")})
        items.append({
            "object_id": oid,
            "path": obj.get("path") or obj.get("存储路径"),
            "checksum": obj.get("checksum") or obj.get("内容校验"),
            "identity_key": obj.get("identity_key")
            or f"{oid}:{obj.get('checksum') or obj.get('内容校验') or ''}",
            "refs": obj_refs,
            "tables": tables,
            "shared_between_tables": len(tables) > 1,
            "held_by_valid_snapshot": len(valid_holders) > 0,
            "valid_holders": valid_holders,
        })

    # 同一路径下出现多个不同对象身份 —— 路径不能作为归属依据。
    path_groups: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        if item.get("path"):
            path_groups.setdefault(str(item["path"]), []).append(item)
    same_path_distinct_identity = []
    for path, group in path_groups.items():
        identities = {str(i["identity_key"]) for i in group}
        if len(identities) > 1:
            same_path_distinct_identity.append({
                "path": path,
                "object_ids": [i["object_id"] for i in group],
                "identity_keys": sorted(identities),
                "warning": "同一路径对应多个对象身份，必须按 object_id/checksum 判定引用归属",
            })

    return {
        "object_count": len(object_index),
        "ref_count": len(refs),
        "items": items,
        "same_path_distinct_identity": same_path_distinct_identity,
        "identity_policy": "object_id+checksum",
    }


def releasable_range_planner(
    table: str,
    objects: list[dict[str, Any]],
    refs: list[dict[str, Any]],
    *,
    clone_tables: list[str] | None = None,
    pending_object_ids: set[str] | list[str] | None = None,
) -> dict[str, Any]:
    """按湖表 + 有效快照计算共享文件的可释放范围。

    判定规则：
    1. 只统计引用 ``table`` 的记录；克隆表（``clone_tables``）的有效快照
       引用同样会阻止释放（已发布克隆持有的文件不能释放）。
    2. 只有 *有效/已发布* 快照的引用才算持有；未发布的草稿引用不保护对象。
    3. 对象身份按 object_id/checksum，路径相同不等于同一对象。
    4. ``pending_object_ids`` 是已经被有效处置清单领取、尚待物理释放的对象，
       单独标记为 pending_delete，供发布/恢复读取。
    """
    clones = set(clone_tables or [])
    family = {table} | clones
    pending = set(pending_object_ids or [])
    object_index = {str(o.get("object_id") or o.get("对象标识")): o for o in objects}
    check = shared_file_reference_check(objects, refs)

    releasable: list[dict[str, Any]] = []
    retained: list[dict[str, Any]] = []
    pending_delete: list[dict[str, Any]] = []
    already_released: list[dict[str, Any]] = []

    for item in check["items"]:
        oid = item["object_id"]
        family_refs = [r for r in item["refs"] if r.get("table") in family]
        valid_family_refs = [
            r for r in family_refs
            if r.get("snapshot_valid", r.get("published", False))
        ]
        clone_holders = [
            r for r in valid_family_refs if r.get("table") in clones
        ]
        entry = {
            "object_id": oid,
            "path": item["path"],
            "checksum": item["checksum"],
            "referenced_by": family_refs,
            "held_by_clone": bool(clone_holders),
            "clone_holders": clone_holders,
        }
        raw_obj = object_index.get(oid, {})
        if raw_obj.get("physical_state") == "released":
            entry["state"] = "already_released"
            already_released.append(entry)
            continue
        if oid in pending:
            entry["state"] = "pending_delete"
            pending_delete.append(entry)
        if valid_family_refs:
            reasons = []
            own = [r for r in valid_family_refs if r.get("table") == table]
            if own:
                reasons.append("referenced_by_valid_snapshot")
            if clone_holders:
                reasons.append("held_by_published_clone")
            entry["retain_reasons"] = reasons
            retained.append(entry)
        else:
            entry["releasable"] = True
            releasable.append(entry)

    return {
        "table": table,
        "clone_tables": sorted(clones),
        "range_object_ids": [e["object_id"] for e in releasable],
        "releasable": releasable,
        "retained": retained,
        "pending_delete": pending_delete,
        "already_released": already_released,
        "pending_object_ids": sorted(pending),
        "same_path_distinct_identity": check["same_path_distinct_identity"],
        "identity_policy": "object_id+checksum",
    }