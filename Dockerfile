FROM python:3.12-slim

LABEL maintainer="ctz168"
LABEL description="LOF Fund Premium/Discount Monitor"

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Database is initialized on application startup so runtime mounts can persist data.
EXPOSE 8080

CMD ["python", "server.py"]
