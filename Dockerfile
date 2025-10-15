# Multi-stage build for efficient dependency caching
FROM python:3.12-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_CONSTRAINT=/app/constraints.txt 

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy pyproject.toml first for better caching
COPY pyproject.toml .
COPY constraints.txt .  

# Install dependencies (this layer will be cached unless pyproject.toml changes)
RUN pip install -c constraints.txt --editable .[dev]

# Copy application code
COPY . /app

# Reinstall in editable mode after copying all files
RUN pip install -c constraints.txt --editable .

# Copy and setup Jupyter kernel setup script
COPY setup-jupyter-kernel.sh /tmp/setup-jupyter-kernel.sh
RUN chmod +x /tmp/setup-jupyter-kernel.sh

# Install basic kernel (the comprehensive one will be set up at runtime)
RUN pip install ipykernel

# Create .dagster_home directory and empty dagster.yaml
RUN mkdir -p /app/.dagster_home && touch /app/.dagster_home/dagster.yaml


# Expose Dagster web server port
EXPOSE 3000

# Default command to run Dagster web server
CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "3000", "--module-name", "dagster_quickstart.definitions"]