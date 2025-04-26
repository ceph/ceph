from fastapi import FastAPI
from mcm.api.controllers import cluster

# Initialize FastAPI app
app = FastAPI()
print("registering routers now...\n")
# Include routers for all cluster related routes
app.include_router(cluster.router)

# Optional: You could add other routers (e.g., auth, user)
