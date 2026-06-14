import json, uuid, time, threading, logging
import stomp

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [client] %(levelname)s: %(message)s")
log = logging.getLogger("client")

AMQ_HOST      = "192.168.56.2"   # S2
AMQ_PORT      = 61613            # STOMP port
AMQ_USER      = "admin"
AMQ_PASSWORD  = "admin"
REQUEST_QUEUE = "math.requests"
RESULT_QUEUE  = "math.results"

_pending = {}
_results = {}

class ResultListener(stomp.ConnectionListener):
    def on_message(self, frame):
        try:
            data    = json.loads(frame.body)
            task_id = data.get("task_id")
            _results[task_id] = data
            icon = "OK" if data.get("status") == "success" else "ERR"
            log.info("[%s] [%s]  %-10s  result=%-12s  via %s  %.1fms",
                icon, task_id[:8], data.get("operation","?"),
                str(data.get("result","?"))[:12],
                data.get("worker_id","?"),
                data.get("duration_ms", 0))
            if task_id in _pending:
                _pending[task_id].set()
            conn.ack(frame.headers["message-id"], 2)
        except Exception as e:
            log.error("Result error: %s", e)

    def on_disconnected(self):
        log.warning("Disconnected from ActiveMQ")

    def on_error(self, frame):
        log.error("AMQ error: %s", frame.body)


conn = stomp.Connection([(AMQ_HOST, AMQ_PORT)])
conn.set_listener("", ResultListener())
conn.connect(AMQ_USER, AMQ_PASSWORD, wait=True)
conn.subscribe(destination=f"/queue/{RESULT_QUEUE}", id=2, ack="client-individual")
log.info("Connected to ActiveMQ @ %s:%d", AMQ_HOST, AMQ_PORT)


def send(operation, operands, timeout=30.0):
    task_id = str(uuid.uuid4())
    _pending[task_id] = threading.Event()
    conn.send(
        body=json.dumps({"task_id": task_id, "operation": operation, "operands": operands}),
        destination=f"/queue/{REQUEST_QUEUE}")
    log.info("-> [%s]  %s(%s)", task_id[:8], operation, operands)
    if _pending[task_id].wait(timeout=timeout):
        del _pending[task_id]
        return _results.pop(task_id)
    del _pending[task_id]
    log.warning("Timeout for task %s", task_id[:8])
    return None


if __name__ == "__main__":
    print("\n--- Single request ---")
    r = send("add", [10, 20, 30])
    if r: print(f"add(10,20,30) = {r['result']}  via {r['worker_id']}\n")

    print("--- Batch test ---")
    for op, ops in [("multiply",[7,8]), ("sqrt",[144]), ("power",[2,10]), ("factorial",[10])]:
        r = send(op, ops)
        if r: print(f"{op}({ops}) = {r['result']}  via {r['worker_id']}")
        
    print("\n--- Error test ---")
    r = send("divide", [10, 0])
    if r:
        print(f"divide(10,0) = {r}")