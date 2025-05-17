from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from ..models.cluster import ClusterInfo
from ..service.cluster import ClusterService
from ..db.manager import get_db

router = APIRouter()

@router.post("/cluster", response_model=ClusterInfo)
async def create_or_update_cluster(info: ClusterInfo, db: AsyncSession = Depends(get_db)):
    try:
        print("received: ", info, "\n")
        service = ClusterService(db)
        return await service.create_or_update_cluster(info)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating cluster info: {e}")

@router.get("/cluster/{fsid}", response_model=ClusterInfo)
async def get_cluster(fsid: str, db: AsyncSession = Depends(get_db)):
    try:
        service = ClusterService(db)
        return await service.get_cluster(fsid)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving cluster info: {e}")

@router.get("/clusters", response_model=List[ClusterInfo])
async def list_clusters(db: AsyncSession = Depends(get_db)):
    try:
        service = ClusterService(db)
        return await service.list_clusters()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing clusters: {e}")
