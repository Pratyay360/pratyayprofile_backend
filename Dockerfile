FROM docker.io/python:3.14.2-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_HTTP_TIMEOUT=120 \
    PIP_NO_CACHE_DIR=1

WORKDIR /

COPY pyproject.toml ./

RUN pip install --no-cache-dir uv \
    && uv venv && source .venv/bin/activate\
    && uv pip install "fastapi[standard]" \
    && uv pip install --system -r pyproject.toml

COPY . .

EXPOSE 3000

CMD ["fastapi", "run", "main.py", "--host", "0.0.0.0", "--port", "3000"]

