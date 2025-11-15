FROM python:3.11-slim AS base

# ------------------------------
# System preparation
# ------------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Required system libs: tzdata for correct timezones, curl for debugging
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tzdata \
        curl && \
    rm -rf /var/lib/apt/lists/*

# ------------------------------
# Python dependencies
# ------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ------------------------------
# Application
# ------------------------------
COPY fpl_bot_enterprise.py .

# Pre-create non-root user
RUN useradd -m botuser
USER botuser

# ------------------------------
# Health endpoint for Northflank
# ------------------------------
EXPOSE 8080

# ------------------------------
# Final command
# ------------------------------
CMD ["python", "fpl_bot.py"]
