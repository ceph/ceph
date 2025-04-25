from fastapi import FastAPI
from mcm.api.controllers import cluster
from mcm.api.config import get_settings

# Initialize FastAPI app
app = FastAPI()

# Include routers for all cluster related routes
app.include_router(cluster.router)

# Optional: You could add other routers (e.g., auth, user)