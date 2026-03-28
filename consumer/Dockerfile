FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py config.py models.py validators.py enrichers.py db.py dlq.py ./

CMD ["python", "-u", "consumer.py"]
