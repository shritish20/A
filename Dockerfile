FROM python:3.11-slim

LABEL maintainer="VolGuard v4.0"
LABEL description="Automated overnight options trading system for Indian markets"

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY volguard_v4_final.py .
RUN mkdir -p /app/data /app/logs

ENV DATABASE_URL=sqlite:////app/data/volguard.db
ENV LOG_FILE=/app/logs/volguard.log

RUN useradd -m -u 1000 volguard && \
    chown -R volguard:volguard /app
USER volguard

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "volguard_v4_final:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
