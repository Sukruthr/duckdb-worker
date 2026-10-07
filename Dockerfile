FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.10.4 /uv /bin/uv
WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-cache
COPY run.py query.sql ./

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["/app/.venv/bin/python", "/app/run.py"]
