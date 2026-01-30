# WeSense Ingester - Meshtastic (Unified)
# Build: docker build -t wesense-ingester-meshtastic .
#
# Unified ingester for public and community Meshtastic networks.
# Set MESHTASTIC_MODE=public (default) or MESHTASTIC_MODE=community.
#
# Expects wesense-ingester-core to be available at ../wesense-ingester-core
# when building with docker-compose (which sets the build context).

FROM python:3.11-slim

WORKDIR /app

# Copy dependency files first for better layer caching
COPY wesense-ingester-core/ /tmp/wesense-ingester-core/
COPY wesense-ingester-meshtastic/requirements-docker.txt .

# Install gcc, build all pip packages, then remove gcc in one layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    pip install --no-cache-dir /tmp/wesense-ingester-core && \
    pip install --no-cache-dir -r requirements-docker.txt && \
    apt-get purge -y --auto-remove gcc && \
    rm -rf /var/lib/apt/lists/* /tmp/wesense-ingester-core

# Copy application code
COPY wesense-ingester-meshtastic/meshtastic_ingester.py .

# Create directories for cache, logs, and config
RUN mkdir -p /app/cache /app/logs /app/config

ENV TZ=UTC

CMD ["python", "-u", "meshtastic_ingester.py"]
