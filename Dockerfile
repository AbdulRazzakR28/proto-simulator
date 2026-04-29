# Use Python 3.11 slim as base
FROM python:3.11-slim

# Install system dependencies for proto compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir grpcio-tools

# Copy the rest of the application
COPY . .

# Expose the dashboard port
EXPOSE 8080

# Run the app with gunicorn and eventlet for production/AWS
CMD ["gunicorn", "-k", "eventlet", "-w", "1", "--bind", "0.0.0.0:8080", "app:app"]
