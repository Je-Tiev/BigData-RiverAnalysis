# ==============================================================================
# Big Data River Quality Platform - Python Microservices Dockerfile
# Serves both FastAPI Model Serving and Streamlit River Operations Dashboard
# ==============================================================================

FROM python:3.12-slim

WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install minimal OS dependencies for network health checks and building
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code and configurations
COPY src/ /app/src/
COPY dags/ /app/dags/
COPY monitoring/ /app/monitoring/
COPY .env.example /app/.env

# Default expose ports: 8000 (FastAPI), 8501 (Streamlit)
EXPOSE 8000 8501

# Default command: launch FastAPI Model Serving microservice
CMD ["python", "-m", "uvicorn", "src.serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
