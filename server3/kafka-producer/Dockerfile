FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir paho-mqtt kafka-python
COPY kafka_producer.py .
CMD ["python", "-u", "kafka_producer.py"]
