import os, json, time, math, logging
import paho.mqtt.client as mqtt

WORKER_ID = os.environ.get("WORKER_ID", "a")

logging.basicConfig(level=logging.INFO,
    format=f"%(asctime)s [worker-{WORKER_ID.upper()}] %(levelname)s: %(message)s")
log = logging.getLogger(f"worker-{WORKER_ID}")

MQTT_HOST    = os.environ["MQTT_HOST"]
MQTT_PORT    = int(os.environ.get("MQTT_PORT", 1883))
TASK_TOPIC   = f"worker/{WORKER_ID}/tasks"
RESULT_TOPIC = f"worker/{WORKER_ID}/results"

OPERATIONS = {
    "add":       lambda ops: sum(ops),
    "subtract":  lambda ops: ops[0] - ops[1],
    "multiply":  lambda ops: math.prod(ops),
    "divide":    lambda ops: ops[0] / ops[1],
    "sqrt":      lambda ops: math.sqrt(ops[0]),
    "power":     lambda ops: ops[0] ** ops[1],
    "factorial": lambda ops: float(math.factorial(int(ops[0]))),
}

def execute(data):
    task_id  = data.get("task_id", "?")
    op       = data.get("operation", "").lower()
    operands = [float(x) for x in data.get("operands", [])]
    t0 = time.time()
    try:
        if op not in OPERATIONS:
            raise ValueError(f"Unknown operation: '{op}'")
        result   = OPERATIONS[op](operands)
        duration = round((time.time()-t0)*1000, 3)
        log.info("OK %s(%s) = %s [%.2fms]", op, operands, result, duration)
        return {**data, "result": result, "status": "success",
                "worker_id": f"worker_{WORKER_ID}", "duration_ms": duration}
    except Exception as e:
        log.error("FAIL task %s: %s", task_id, e)
        return {**data, "status": "error", "error": str(e),
                "worker_id": f"worker_{WORKER_ID}", "duration_ms": 0}

client = mqtt.Client(client_id=f"math-worker-{WORKER_ID}")

def on_connect(c, userdata, flags, rc):
    if rc == 0:
        c.subscribe(TASK_TOPIC, qos=1)
        log.info("Connected -> %s:%d  listening on %s", MQTT_HOST, MQTT_PORT, TASK_TOPIC)
    else:
        log.error("MQTT connect failed rc=%d", rc)

def on_message(c, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        c.publish(RESULT_TOPIC, json.dumps(execute(data)), qos=1)
    except Exception as e:
        log.error("Message error: %s", e)

client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        client.connect(MQTT_HOST, MQTT_PORT)
        client.loop_forever()
    except Exception as e:
        log.error("Disconnected: %s - retry in 5s", e)
        time.sleep(5)
