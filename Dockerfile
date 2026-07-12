# ---- Build Stage ----
FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- Runtime Stage ----
FROM python:3.12-slim

# Security: Create non-root user for OpenShift SCC compliance
RUN groupadd -r appgroup && useradd -r -g appgroup -u 1001 -d /app -s /sbin/nologin appuser

WORKDIR /app

# Copy installed dependencies from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY app/ ./app/

# Set ownership of the application directory to the non-root user
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER 1001

# Environment defaults
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO

# Health check placeholder (readiness/liveness probes configured in OpenShift)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "print('healthy')" || exit 1

ENTRYPOINT ["python", "-m", "app.worker"]
