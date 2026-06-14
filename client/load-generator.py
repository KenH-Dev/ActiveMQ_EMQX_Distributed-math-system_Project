import json, uuid, time, random, logging
from datetime import datetime
import stomp

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [load-gen] %(levelname)s: %(message)s")
log = logging.getLogger("load-gen")

AMQ_HOST      = "192.168.56.2"
AMQ_PORT      = 61613
AMQ_USER      = "admin"
AMQ_PASSWORD  = "admin"
REQUEST_QUEUE = "math.requests"
RESULT_QUEUE  = "math.results"
LOG_FILE      = "results_log.txt"

OPERATIONS = [
    ("add",       lambda: [random.randint(1, 100) for _ in range(random.randint(2, 4))]),
    ("subtract",  lambda: [random.randint(1, 100), random.randint(1, 100)]),
    ("multiply",  lambda: [random.randint(1, 20), random.randint(1, 20)]),
    ("divide",    lambda: [random.randint(1, 100), random.randint(1, 10)]),
    ("sqrt",      lambda: [random.randint(1, 200)]),
    ("power",     lambda: [random.randint(2, 5), random.randint(2, 5)]),
    ("factorial", lambda: [random.randint(1, 12)]),
]

_pending_questions = {}
conn = None

def write_log(line):
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

class ResultListener(stomp.ConnectionListener):
    def on_message(self, frame):
            try:
                data = json.loads(frame.body)
                task_id = data.get("task_id", "?")
                question = _pending_questions.pop(task_id, "(unknown question)")

                if data.get("status") == "success":
                    outcome = f"result = {data.get('result')}"
                    icon = "OK"
                else:
                    outcome = f"ERROR = {data.get('error')}"
                    icon = "ERR"

                line = (f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
                        f"{question}  ->  {outcome}  "
                        f"(via {data.get('worker_id','?')}, {data.get('duration_ms',0)}ms)")

                print(f"[{icon}] {line}")
                write_log(line)
            except Exception as e:
                log.error("Result error: %s", e)

    def on_disconnected(self):
        log.warning("Disconnected - reconnecting...")
        connect()

def connect():
    global conn
    while True:
        try:
            conn = stomp.Connection([(AMQ_HOST, AMQ_PORT)])
            conn.set_listener("", ResultListener())
            conn.connect(AMQ_USER, AMQ_PASSWORD, wait=True)
            conn.subscribe(destination=f"/queue/{RESULT_QUEUE}", id=1, ack="auto")
            log.info("Connected to ActiveMQ @ %s:%d", AMQ_HOST, AMQ_PORT)
            return
        except Exception as e:
            log.warning("Connection failed (%s) - retrying in 5s", e)
            time.sleep(5)

def send_random():
    global conn
    op, gen_operands = random.choice(OPERATIONS)
    operands = gen_operands()
    task_id = str(uuid.uuid4())
    question = f"{op}({', '.join(str(o) for o in operands)})"
    _pending_questions[task_id] = question

    try:
        conn.send(
            body=json.dumps({"task_id": task_id, "operation": op, "operands": operands}),
            destination=f"/queue/{REQUEST_QUEUE}")
        print(f"-> Sent: {question}")
    except Exception as e:
        log.warning("Send failed (%s) - reconnecting", e)
        connect()

if __name__ == "__main__":
    connect()
    log.info("Sending a random calculation every 10 seconds. Logging to %s. Ctrl+C to stop.", LOG_FILE)
    try:
        while True:
            send_random()
            time.sleep(10)
    except KeyboardInterrupt:
        log.info("Stopping...")
        conn.disconnect()