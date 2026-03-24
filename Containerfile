# Stage 1: Build UI
FROM node:22-slim AS ui-build
WORKDIR /ui
COPY pipeline-ui/package.json pipeline-ui/package-lock.json ./
RUN npm ci
COPY pipeline-ui/ .
RUN npm run build

# Stage 2: Python runtime
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl libmagic1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install Python deps
COPY pyproject.toml .
COPY pipeline/ pipeline/
COPY pipeline_mcp/ pipeline_mcp/
RUN uv pip install --system --no-cache .

# Copy config files
COPY shared/ shared/

# Copy built UI
COPY --from=ui-build /ui/dist pipeline-ui/dist/

# Runtime dirs (mounted as volumes in production)
RUN mkdir -p data/drop data/corpus

EXPOSE 8080

CMD ["python", "-m", "pipeline.main"]
