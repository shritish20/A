FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY volguard_final.py .
COPY .env .env

# Create data directory
RUN mkdir -p /app/data /app/logs

# Run as non-root
RUN useradd -m -u 1000 volguard && chown -R volguard:volguard /app
USER volguard

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

EXPOSE 8000
CMD ["uvicorn", "volguard_final:app", "--host", "0.0.0.0", "--port", "8000"]
