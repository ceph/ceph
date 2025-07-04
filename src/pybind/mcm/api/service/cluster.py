from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from fastapi import HTTPException
from ..models.cluster import ClusterInfo
from ..db.dbo.cluster import ClusterInfoDB

class ClusterService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_or_update_cluster(self, info: ClusterInfo) -> ClusterInfo:
        stmt = pg_insert(ClusterInfoDB).values(
            fsid=info.fsid,
            version=info.version,
            health=info.health,
            capacity=info.capacity.dict()
        ).on_conflict_do_update(
            index_elements=['fsid'],
            set_={
                'version': info.version,
                'health': info.health,
                'capacity': info.capacity.dict()
            }
        )

        async with self.db.begin():
            await self.db.execute(stmt)

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

    async def list_clusters(self) -> list[ClusterInfo]:
        stmt = select(ClusterInfoDB)
        result = await self.db.execute(stmt)
        clusters = result.scalars().all()

        return [
            ClusterInfo(
                fsid=cluster.fsid,
                version=cluster.version,
                health=cluster.health,
                capacity=cluster.capacity
            ) for cluster in clusters
        ]
