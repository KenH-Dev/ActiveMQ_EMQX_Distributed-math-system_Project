import os, json, time, logging, threading
import paho.mqtt.client as mqtt
import stomp

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [dispatcher] %(levelname)s: %(message)s")
log = logging.getLogger("s3-dispatcher")

AMQ_HOST       = os.environ["AMQ_HOST"]
AMQ_PORT       = int(os.environ.get("AMQ_PORT", 61616))
AMQ_USER       = os.environ["AMQ_USER"]
AMQ_PASSWORD   = os.environ["AMQ_PASSWORD"]
MQTT_HOST      = os.environ["MQTT_HOST"]
MQTT_PORT      = int(os.environ.get("MQTT_PORT", 1883))
WORKER_A_TOPIC = os.environ.get("WORKER_A_TOPIC", "worker/a/tasks")
WORKER_B_TOPIC = os.environ.get("WORKER_B_TOPIC", "worker/b/tasks")
REQUEST_QUEUE  = os.environ.get("REQUEST_QUEUE", "math.requests")
TOPIC_MAP      = {"a": WORKER_A_TOPIC, "b": WORKER_B_TOPIC}

def parse_weights(raw):
    return {k.strip(): int(v.strip()) for part in raw.split(",") for k, v in [part.split(":")]}

WORKER_WEIGHTS = parse_weights(os.environ.get("WORKER_WEIGHTS", "a:1,b:1"))
log.info("Weights: %s | AMQ: %s:%d | MQTT: %s:%d", WORKER_WEIGHTS, AMQ_HOST, AMQ_PORT, MQTT_HOST, MQTT_PORT)

class RoundRobinBalancer:
    def __init__(self, weights):
        self._seq = [w for w, n in weights.items() for _ in range(n)]
        self._idx = 0
        self._lock = threading.Lock()
    def next(self):
        with self._lock:
            w = self._seq[self._idx % len(self._seq)]
            self._idx += 1
            return w

class Dispatcher(stomp.ConnectionListener):
    def __init__(self):
        self.balancer = RoundRobinBalancer(WORKER_WEIGHTS)
        self.mqtt = mqtt.Client(client_id="dispatcher-mqtt")
        self.mqtt.connect(MQTT_HOST, MQTT_PORT)
        self.mqtt.loop_start()
        log.info("MQTT connected -> %s:%d", MQTT_HOST, MQTT_PORT)
        self._connect_amq()

    def _connect_amq(self):
        self.conn = stomp.Connection([(AMQ_HOST, AMQ_PORT)])
        self.conn.set_listener("", self)
        self.conn.connect(AMQ_USER, AMQ_PASSWORD, wait=True)
        self.conn.subscribe(f"/queue/{REQUEST_QUEUE}", id=1, ack="client-individual")
        log.info("AMQ connected -> %s:%d queue:%s", AMQ_HOST, AMQ_PORT, REQUEST_QUEUE)

    def on_message(self, frame):
        try:
            body = json.loads(frame.body)
            worker = self.balancer.next()
            payload = json.dumps({**body, "dispatched_to": f"worker_{worker}", "timestamp": time.time()})
            self.mqtt.publish(TOPIC_MAP[worker], payload, qos=1)
            self.conn.ack(frame.headers["message-id"], 1)
            log.info("Task %s -> Worker %s", body.get("task_id","?")[:8], worker.upper())
        except Exception as e:
            log.error("Dispatch error: %s", e)
            try: self.conn.nack(frame.headers["message-id"], 1)
            except: pass

    def on_error(self, frame):
        log.error("AMQ error: %s", frame.body)

    def on_disconnected(self):
        log.warning("AMQ disconnected - reconnecting...")
        while True:
            try:
                time.sleep(5)
                self._connect_amq()
                return
            except Exception as e:
                log.warning("Reconnect failed (%s) - retrying in 5s", e)

if __name__ == "__main__":
    Dispatcher()
    while True:
        time.sleep(10)
