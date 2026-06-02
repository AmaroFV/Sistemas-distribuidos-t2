FROM python:3.11-slim
WORKDIR /app
COPY kafka_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared /app/shared
COPY kafka_consumer/main.py /app/main.py
ENV PYTHONPATH=/app
CMD ["python", "main.py"]