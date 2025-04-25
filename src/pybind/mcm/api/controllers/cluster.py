from fastapi import APIRouter, Depends
from ..models.cluster import ClusterInfo
from ..service import ClusterService
from ..db.manager import get_db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

@router.post("/cluster", response_model=ClusterInfo)
async def create_cluster_info(
    info: ClusterInfo,
    db: AsyncSession = Depends(get_db)
):
    service = ClusterService(db)
    return await service.create_or_update_cluster(info)

@router.get("/cluster/{fsid}", response_model=ClusterInfo)
async def get_cluster_info(
    fsid: str,
    db: AsyncSession = Depends(get_db)
):
    service = ClusterService(db)
    return await service.get_cluster(fsid)