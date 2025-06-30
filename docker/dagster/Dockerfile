FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/storage

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create dagster user for security
RUN useradd --create-home --shell /bin/bash dagster

# Set working directory
WORKDIR /opt/dagster/app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /opt/dagster/storage && \
    chown -R dagster:dagster /opt/dagster

# Switch to dagster user
USER dagster

# Expose port
EXPOSE 3000

# Default command
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]