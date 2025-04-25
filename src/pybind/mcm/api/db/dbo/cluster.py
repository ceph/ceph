from sqlalchemy import Column, String, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ClusterInfoDB(Base):
    __tablename__ = "cluster_info"
    fsid = Column(String, primary_key=True, index=True)
    version = Column(String)
    health = Column(String)
    capacity = Column(JSON)