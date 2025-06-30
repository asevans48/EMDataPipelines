# Multi-stage Dockerfile for Emergency Management Pipeline
# Optimized for development and Gov Cloud deployment

FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create dagster user for security
RUN groupadd -r dagster && useradd -r -g dagster dagster

# Create directories and set permissions
RUN mkdir -p /opt/dagster/app \
    && mkdir -p /opt/dagster/dagster_home \
    && mkdir -p /opt/dagster/storage \
    && mkdir -p /opt/dagster/storage/artifacts \
    && mkdir -p /opt/dagster/storage/audit \
    && mkdir -p /opt/dagster/storage/usage_logs \
    && chown -R dagster:dagster /opt/dagster

WORKDIR /opt/dagster/app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Copy Dagster configuration
COPY dagster.yml /opt/dagster/dagster_home/dagster.yaml
COPY workspace.yml /opt/dagster/dagster_home/workspace.yaml

# Set ownership
RUN chown -R dagster:dagster /opt/dagster

# Switch to dagster user
USER dagster

# Expose Dagster webserver port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3000/server_info || exit 1

# Default command
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]