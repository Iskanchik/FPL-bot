FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PATH="/usr/local/bin:$PATH"

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tzdata \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first (for better Docker cache)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy application
COPY fpl_bot.py .

# Create directory for snapshots
RUN mkdir -p /app/fpl_snapshots && \
    chmod 777 /app/fpl_snapshots

# Create non-root user
RUN useradd -m botuser && chown -R botuser:botuser /app
USER botuser

# Expose metrics/health port
EXPOSE 8080

CMD ["python", "-u", "fpl_bot.py"]
