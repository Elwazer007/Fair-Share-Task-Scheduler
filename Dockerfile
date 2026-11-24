FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN groupadd --system app && useradd --system --gid app --home-dir /app app

COPY pyproject.toml ./
COPY config ./config
COPY scheduler ./scheduler
RUN mkdir -p loadtest && touch loadtest/__init__.py

FROM base AS runtime
RUN pip install .
COPY . .
USER app
EXPOSE 8000
CMD ["gunicorn", "config.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "4", "--threads", "4"]

FROM base AS dev
RUN pip install ".[dev]"
COPY . .
RUN chown -R app:app /app
USER app
CMD ["pytest"]
