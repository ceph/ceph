from mcm.api.db.dbo.cluster import Base
from mcm.api.db.manager import engine
import asyncio

async def init():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

asyncio.run(init())