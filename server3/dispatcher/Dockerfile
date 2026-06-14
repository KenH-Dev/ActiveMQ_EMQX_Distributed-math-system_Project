FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir stomp.py paho-mqtt
COPY dispatcher.py .
CMD ["python", "-u", "dispatcher.py"]
