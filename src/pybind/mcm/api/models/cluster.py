from pydantic import BaseModel

class Capacity(BaseModel):
    total: str
    used: str

class ClusterInfo(BaseModel):
    fsid: str
    capacity: Capacity
    version: str
    health: str
