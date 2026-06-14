FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir stomp.py paho-mqtt
COPY bridge.py .
CMD ["python", "-u", "bridge.py"]
