from sqlalchemy.ext.asyncio import AsyncSession
from ..models import ClusterInfoDB
from ..db.dbo.cluster import ClusterInfo
from fastapi import HTTPException

class ClusterService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_or_update_cluster(self, info: ClusterInfo):
        db_info = ClusterInfoDB(
            fsid=info.fsid,
            version=info.version,
            health=info.health,
            capacity=info.capacity.dict()
        )
        self.db.merge(db_info)
        await self.db.commit()
        return info

    async def get_cluster(self, fsid: str) -> ClusterInfo:
        result = await self.db.get(ClusterInfoDB, fsid)
        if result is None:
            raise HTTPException(status_code=404, detail="Cluster info not found")
        return ClusterInfo(
            fsid=result.fsid,
            version=result.version,
            health=result.health,
            capacity=result.capacity
        )