"""共享文件引用校验 / 可释放范围 / 清理-发布冲突 / 幂等回执 的端到端契约。

运行：backend/.venv/bin/python -m pytest tests/test_shared_refs.py -q
"""
from __future__ import annotations

import sys

sys.path.insert(0, ".")

import pytest
from fastapi.testclient import TestClient

from app.lake_refs import ref_guard
from app.main import app


@pytest.fixture(autouse=True)
def _reset_scenario():
    ref_guard.reset()
    yield
    ref_guard.reset()


@pytest.fixture()
def client():
    return TestClient(app)


class TestSharedReferenceVerification:
    def test_raw_and_clone_shared_refs(self):
        result = ref_guard.verify_refs("raw_orders", "clone_orders")
        assert result["ok"] is True
        # obj-05（已发布克隆持有）与 obj-07（s18 草稿引用）都是跨表共享身份。
        assert set(result["shared_objects"]) == {"obj-05", "obj-07"}

    def test_identity_not_path(self):
        result = ref_guard.verify_refs()
        traps = result["same_path_distinct_identity"]
        paths = {item["path"].split("/")[-1] for item in traps}
        assert "part-00007.parquet" in paths
        # obj-07 与 obj-70 路径相同但身份不同，必须同时存在且互不合并。
        ids = {oid for trap in traps for oid in trap["object_ids"]}
        assert {"obj-07", "obj-70"} <= ids

    def test_reconcile_range_only_obj07(self):
        result = ref_guard.reconcile("raw_orders", "clone_orders")
        assert result["releasable_object_ids"] == ["obj-07"]
        # 核对结果已写回共享文件行。
        rows = {r["对象标识"]: r for r in __import__("app.store", fromlist=["store"]).store.rows("shared_file")}
        assert rows["obj-07"]["可释放"] == "是"
        assert rows["obj-05"]["可释放"] == "否"

    def test_published_clone_holder_blocks_release(self):
        # obj-05 被已发布克隆 s17 持有，显式领取必须被拒绝。
        result = ref_guard.claim("raw_orders", ["obj-05"])
        assert result["ok"] is False
        assert result["code"] == "NOT_RELEASABLE"


class TestPublishCleanupCollision:
    def test_s18_publish_hits_old_claim(self):
        collision = ref_guard.publish_snapshot("s18")
        assert collision["ok"] is False
        assert collision["conflict"] is True
        assert collision["code"] == "PENDING_DELETE_CONFLICT"
        assert collision["order"] == ["cleanup_claim", "publish_request"]
        assert collision["cleanup_first"] is True
        # 领取不等于物理删除，冲突时文件仍可读。
        assert collision["readable"] is True
        assert collision["regenerate_side"] == "snapshot_publish"
        assert "s18" in collision["regenerate_owner"]

        d = ref_guard.get_disposition("disp-0001")
        sides = [(c["side"], c["seq"]) for c in d["confirmations"]]
        assert sides[0][0] == "vacuum_cleanup" and sides[1][0] == "snapshot_publish"
        assert sides[0][1] < sides[1][1]
        assert d["collisions"][-1]["object_readable"] is True
        assert "s18" in d["collisions"][-1]["which_side_regenerates"]

    def test_repeated_publish_does_not_duplicate_confirmation(self):
        ref_guard.publish_snapshot("s18")
        again = ref_guard.publish_snapshot("s18")
        assert again.get("idempotent_repeat") is True
        d = ref_guard.get_disposition("disp-0001")
        assert len([c for c in d["confirmations"] if c["side"] == "snapshot_publish"]) == 1

    def test_manifest_and_list_readable(self):
        ref_guard.publish_snapshot("s18")
        text = ref_guard.disposition_manifest_text("disp-0001")
        assert text and "obj-07" in text and "disp-0001" in text
        row = ref_guard.list_dispositions("raw_orders")[0]
        assert row["对象可读性"]["obj-07"] is True
        assert "s18" in row["需重新生成方"]
        assert row["重新生成结论"]

    def test_release_receipt_and_rerun_idempotent(self):
        ref_guard.publish_snapshot("s18")
        receipt = ref_guard.release("disp-0001")
        assert receipt["ok"] is True
        assert receipt["released_objects"] == ["obj-07"]
        assert receipt["released_count"] == 1
        assert receipt["reclaimed_bytes"] > 0
        assert receipt["physical_deleted"] is True
        assert ref_guard.read_object("obj-07")["readable"] is False

        rerun = ref_guard.release("disp-0001")
        assert rerun["rerun"] is True
        assert rerun["newly_released_count"] == 0
        assert rerun["newly_reclaimed_bytes"] == 0
        assert rerun["receipt_id"] == receipt["receipt_id"]
        assert ref_guard.totals["released_count"] == 1

    def test_regenerate_then_publish_uses_new_identity(self):
        ref_guard.release("disp-0001")
        result = ref_guard.publish_snapshot("s18", regenerate=True)
        assert result["ok"] is True
        assert result["files"] == ["obj-18"]
        assert ref_guard.objects["obj-18"]["rewritten_from"] == "obj-07"

    def test_restore_checks_readability(self):
        assert ref_guard.restore_snapshot("s17")["ok"] is True
        ref_guard.release("disp-0001")
        old = ref_guard.restore_snapshot("s14")
        assert old["ok"] is False and old["code"] == "OBJECT_GONE"


class TestHttpEndpoints:
    def test_http_narrative(self, client: TestClient):
        r = client.get("/api/lake-refs/verify",
                       params={"original_table": "raw_orders", "clone_table": "clone_orders"})
        assert r.status_code == 200 and r.json()["ok"]

        r = client.post("/api/snapshots/s18/publish")
        body = r.json()
        assert body["ok"] is False and body["code"] == "PENDING_DELETE_CONFLICT"

        # 严格模式可返回 409。
        assert client.post("/api/snapshots/s18/publish", params={"strict_http": 1}).status_code == 409

        detail = client.get("/api/snapshots/s18").json()
        assert detail["publish_state"] == "blocked_pending_delete"

        listing = client.get("/api/vacuum-dispositions").json()
        assert listing["total"] >= 1
        assert listing["items"][0]["确认先后"]

        manifest = client.get("/api/lake-refs/dispositions/disp-0001/manifest?format=text")
        assert manifest.status_code == 200 and "obj-07" in manifest.text

        receipt = client.post("/api/lake-refs/dispositions/disp-0001/release").json()
        assert receipt["receipt_id"] == "rcpt-disp-0001"
        rerun = client.post("/api/lake-refs/dispositions/disp-0001/rerun").json()
        assert rerun["newly_released_count"] == 0

    def test_generic_resource_pages(self, client: TestClient):
        assert client.get("/api/shared_file").json()["total"] >= 10
        assert client.get("/api/vacuum_disposition").json()["total"] >= 1
        snap = client.get("/api/snapshot_mgr/18").json()
        assert snap["快照标识"] == "s18"

    def test_exports_not_shadowed(self, client: TestClient):
        assert client.get("/api/snapshot_mgr/export").status_code == 200
        assert client.get("/api/vacuum_job/export").status_code == 200

    def test_race_single_valid_disposition(self, client: TestClient):
        body = client.post("/api/lake-refs/race").json()
        assert body["disposition_count_for_table"] == 1
        assert body["receipt_count"] == 1
