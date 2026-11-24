"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, chains, lake_refs

# lake_refs 含需要优先匹配的固定/别名路径，放在通用资源路由之前注册。
ROUTERS = [lake_refs, resource, chains]
