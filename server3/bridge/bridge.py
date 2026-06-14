import os, json, time, logging
import stomp, paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [mqtt-bridge] %(levelname)s: %(message)s")
log = logging.getLogger("s3-bridge")

AMQ_HOST          = os.environ["AMQ_HOST"]
AMQ_PORT          = int(os.environ.get("AMQ_PORT", 61613))
AMQ_USER          = os.environ["AMQ_USER"]
AMQ_PASSWORD      = os.environ["AMQ_PASSWORD"]
MQTT_HOST         = os.environ["MQTT_HOST"]
MQTT_PORT         = int(os.environ.get("MQTT_PORT", 1883))
RESULT_MQTT_TOPIC = os.environ.get("RESULT_MQTT_TOPIC", "worker/+/results")
RESULT_AMQ_QUEUE  = os.environ.get("RESULT_AMQ_QUEUE", "math.results")

def connect_amq():
    while True:
        try:
            c = stomp.Connection([(AMQ_HOST, AMQ_PORT)])
            c.connect(AMQ_USER, AMQ_PASSWORD, wait=True)
            log.info("AMQ connected for bridge -> %s:%d", AMQ_HOST, AMQ_PORT)
            return c
        except Exception as e:
            log.warning("AMQ connect failed (%s) - retrying in 5s", e)
            time.sleep(5)

amq = connect_amq()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(RESULT_MQTT_TOPIC, qos=1)
        log.info("Subscribed to MQTT: %s", RESULT_MQTT_TOPIC)
    else:
        log.error("MQTT connect failed rc=%d", rc)

def on_message(client, userdata, msg):
    global amq
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)
        amq.send(body=payload,
                 destination=f"/queue/{RESULT_AMQ_QUEUE}",
                 headers={"task_id": data.get("task_id",""), "source": msg.topic})
        log.info("Bridged task %s -> AMQ:%s", data.get("task_id","?")[:8], RESULT_AMQ_QUEUE)
    except Exception as e:
        log.error("Bridge error: %s - reconnecting AMQ", e)
        amq = connect_amq()

client = mqtt.Client(client_id="mqtt-amq-bridge")
client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        client.connect(MQTT_HOST, MQTT_PORT)
        client.loop_forever()
    except Exception as e:
        log.error("MQTT error: %s - retry in 5s", e)
        time.sleep(5)
