FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir paho-mqtt
COPY worker.py .
CMD ["python", "-u", "worker.py"]
