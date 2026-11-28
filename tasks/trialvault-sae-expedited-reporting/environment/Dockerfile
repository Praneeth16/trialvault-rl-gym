FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TRIALVAULT_PV_DATA_DIR=/app/data

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    fastapi==0.165.0 \
    uvicorn==0.54.0 \
    pydantic==2.14.3 \
    httpx==0.28.1 \
    pytest==9.2.0

RUN useradd -m -s /bin/bash agent

WORKDIR /app
COPY workspace/ /app/
RUN chown -R agent:agent /app

USER agent
