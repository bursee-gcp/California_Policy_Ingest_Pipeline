FROM python:3.11-slim-bookworm

# 1. Patch OS vulnerabilities and clean up
RUN apt-get update && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

# 2. Create a non-root user and group
RUN groupadd -r appgroup && useradd -r -g appgroup -m appuser

WORKDIR /app

# 3. Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy source code
COPY ingest.py capublic.sql ./

# 5. Change ownership and drop privileges
RUN chown -R appuser:appgroup /app
USER appuser

ENTRYPOINT ["python", "ingest.py"]
