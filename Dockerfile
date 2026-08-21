# ============================================================================
# Dockerfile — Willow Dagster project
# Containerized Dagster: pinned deps + code + UI, runs identically anywhere.
# Build:  docker build -t willow-dagster .
# Run  :  docker run --rm -p 3000:3000 willow-dagster     # UI at :3000
# ============================================================================
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    DAGSTER_HOME=/app/.dagster_home

WORKDIR /app

# install deps first (cached layer), then the project
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project

COPY . .
RUN uv sync --frozen && mkdir -p /app/.dagster_home

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 3000
# serve the UI on all interfaces so it's reachable from the host / Codespace
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "willow_dagster.definitions"]
