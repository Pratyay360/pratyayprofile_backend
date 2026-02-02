FROM docker.io/python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_HTTP_TIMEOUT=120 \
    PIP_NO_CACHE_DIR=1

WORKDIR /

COPY pyproject.toml uv.lock .env ./

RUN pip install --no-cache-dir uv \
    && uv pip install --system -r pyproject.toml

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
