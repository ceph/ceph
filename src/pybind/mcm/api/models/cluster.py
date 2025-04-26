from pydantic import BaseModel

class Capacity(BaseModel):
    total: float
    used: float

class ClusterInfo(BaseModel):
    fsid: str
    capacity: Capacity
    version: str
    health: str
