FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# System packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tzdata \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# App
COPY fpl_bot.py .
RUN mkdir -p /app/fpl_snapshots
RUN useradd -m botuser && chown -R botuser:botuser /app
USER botuser

EXPOSE 8080

CMD ["python", "-u", "fpl_bot.py"]
