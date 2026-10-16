FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN pip install --no-cache-dir -e .

ENV CLOUD_DB_PATH=/app/data/mayacardio_cloud.db
RUN mkdir -p /app/data

EXPOSE 8000

CMD ["uvicorn", "mayacardio.cloud_server:app", "--host", "0.0.0.0", "--port", "8000"]
