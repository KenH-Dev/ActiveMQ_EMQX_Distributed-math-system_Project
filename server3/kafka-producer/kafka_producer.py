import os, json, time, logging
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [kafka-producer] %(levelname)s: %(message)s")
log = logging.getLogger("s3-kafka-producer")

MQTT_HOST    = os.environ["MQTT_HOST"]
MQTT_PORT    = int(os.environ.get("MQTT_PORT", 1883))
RESULT_TOPIC = os.environ.get("RESULT_MQTT_TOPIC", "worker/+/results")

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "math.results")

def get_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"))
            log.info("Connected to Kafka @ %s", KAFKA_BOOTSTRAP)
            return p
        except Exception as e:
            log.warning("Kafka not ready (%s) - retry in 3s", e)
            time.sleep(3)

producer = get_producer()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(RESULT_TOPIC, qos=1)
        log.info("Subscribed to MQTT: %s", RESULT_TOPIC)
    else:
        log.error("MQTT connect failed rc=%d", rc)

def on_message(client, userdata, msg):
    global producer
    try:
        data = json.loads(msg.payload.decode())
        producer.send(KAFKA_TOPIC, value=data)
        producer.flush()
        log.info("Sent task %s -> kafka:%s",
                 data.get("task_id","?")[:8], KAFKA_TOPIC)
    except Exception as e:
        log.error("Kafka send error: %s - reconnecting", e)
        producer = get_producer()

client = mqtt.Client(client_id="kafka-producer")
client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        client.connect(MQTT_HOST, MQTT_PORT)
        client.loop_forever()
    except Exception as e:
        log.error("MQTT error: %s - retry in 5s", e)
        time.sleep(5)
