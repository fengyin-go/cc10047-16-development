"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.stream_source import StreamSourceService
from app.services.lake_table import LakeTableService
from app.services.stream_ingest import StreamIngestService
from app.services.delta_commit import DeltaCommitService
from app.services.partition_merge import PartitionMergeService
from app.services.stream_schema import StreamSchemaService
from app.services.snapshot_mgr import SnapshotMgrService
from app.services.stream_quality import StreamQualityService
from app.services.vacuum_job import VacuumJobService
from app.services.batch_merge import BatchMergeService
from app.services.lake_lineage import LakeLineageService
from app.services.retention_policy import RetentionPolicyService
from app.services.compaction_task import CompactionTaskService
from app.services.backfill_pipeline import BackfillPipelineService
from app.services.stream_metric import StreamMetricService
from app.services.iceberg_migrate import IcebergMigrateService
from app.services.table_catalog import TableCatalogService
from app.services.lake_audit import LakeAuditService
from app.services.stream_checkpoint import StreamCheckpointService
from app.services.shared_file import SharedFileService, VacuumDispositionService

SERVICES: dict[str, BaseModuleService] = {
    "stream_source": StreamSourceService(),
    "lake_table": LakeTableService(),
    "stream_ingest": StreamIngestService(),
    "delta_commit": DeltaCommitService(),
    "partition_merge": PartitionMergeService(),
    "stream_schema": StreamSchemaService(),
    "snapshot_mgr": SnapshotMgrService(),
    "stream_quality": StreamQualityService(),
    "vacuum_job": VacuumJobService(),
    "batch_merge": BatchMergeService(),
    "lake_lineage": LakeLineageService(),
    "retention_policy": RetentionPolicyService(),
    "compaction_task": CompactionTaskService(),
    "backfill_pipeline": BackfillPipelineService(),
    "stream_metric": StreamMetricService(),
    "iceberg_migrate": IcebergMigrateService(),
    "table_catalog": TableCatalogService(),
    "lake_audit": LakeAuditService(),
    "stream_checkpoint": StreamCheckpointService(),
    "shared_file": SharedFileService(),
    "vacuum_disposition": VacuumDispositionService(),
}

# 常见近义模块 key 指向同一实现，避免不同命名习惯下 /api/<key> 404。
for _alias_key in (
    "shared-files", "shared_files", "shared-file", "shared-file-ref",
    "shared_file_ref", "shared-file-refs", "shared_file_refs",
    "shared-object", "shared_object", "shared-objects", "shared_objects",
    "data_file", "data_files", "data-file", "data-files",
    "lake_file", "lake_files", "lake-file", "lake-files",
    "file_ref", "file_refs", "file-reference", "file-ownership",
):
    SERVICES[_alias_key] = SERVICES["shared_file"]

for _alias_key in (
    "vacuum-disposition", "vacuum_dispositions", "vacuum-dispositions",
    "cleanup_disposition", "cleanup-disposition", "disposition", "dispositions",
    "disposal_list", "disposal-list",
):
    SERVICES[_alias_key] = SERVICES["vacuum_disposition"]


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
