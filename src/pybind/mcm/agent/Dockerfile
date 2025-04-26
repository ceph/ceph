# Start from a minimal Python base
FROM python:3.11-slim

# Create working directory
WORKDIR /opt/pybind

# Install necessary system dependencies (for SSL, timezone, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/pybind/mcm/agent/mcm-agent

# Copy agent source code
COPY mcm/agent /opt/pybind/mcm/agent

# Install Python dependencies
RUN python -m venv venv && ./venv/bin/pip install --no-cache-dir -r mcm/agent/requirements.txt

# Expose any needed ports (if agent has HTTP server inside, otherwise not needed)
# EXPOSE 8000

# Entry point
ENTRYPOINT ["python -m mcm.agent.main --prometheus-api-base-path http://192.168.64.3:9095 --dashboard-api-base-path https://192.168.64.3:8443/ --max-queue-size 10 --mcm-api-base-path http://192.168.64.4:9099"]
