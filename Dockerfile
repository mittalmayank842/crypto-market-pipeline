FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY tests ./tests
ENV PYTHONPATH=/app/src
# default: full run (fetch + process + quality gate + dashboard)
CMD ["sh", "-c", "python -m crypto_pipeline.pipeline && python -m crypto_pipeline.quality_gate && python -m crypto_pipeline.dashboard"]
