FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN pip install --no-cache-dir \
    fastapi==0.165.0 \
    uvicorn==0.54.0 \
    pydantic==2.14.3 \
    httpx==0.28.1 \
    harbor-rewardkit==0.2.4

WORKDIR /app
COPY . /tests/
RUN chmod +x /tests/test.sh
