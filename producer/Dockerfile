FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py models.py factories.py producer.py ./

CMD ["python", "-u", "producer.py"]
