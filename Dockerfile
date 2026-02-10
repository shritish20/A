# ============================================================================
# VolGuard v3.2 - Production Dockerfile
# ============================================================================
# Multi-stage build for optimized image size and security
# ============================================================================

# Stage 1: Builder
FROM python:3.11-slim as builder

# Set working directory
WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --user -r requirements.txt

# ============================================================================
# Stage 2: Runtime
FROM python:3.11-slim

# Metadata
LABEL maintainer="VolGuard Team"
LABEL version="3.2.0"
LABEL description="VolGuard Options Trading System with P&L Attribution"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create non-root user for security
RUN useradd -m -u 1000 -s /bin/bash volguard

# Set working directory
WORKDIR /app

# Copy Python dependencies from builder
COPY --from=builder /root/.local /home/volguard/.local

# Create necessary directories
RUN mkdir -p /app/data /app/logs && \
    chown -R volguard:volguard /app

# Copy application code
COPY --chown=volguard:volguard volguard_integrated_full.py /app/volguard.py

# Copy environment example (user will mount their own .env)
COPY --chown=volguard:volguard .env.example /app/.env.example

# Switch to non-root user
USER volguard

# Update PATH to include user-installed packages
ENV PATH=/home/volguard/.local/bin:$PATH

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/api/health', timeout=5)" || exit 1

# Run the application
CMD ["python", "volguard.py"]
