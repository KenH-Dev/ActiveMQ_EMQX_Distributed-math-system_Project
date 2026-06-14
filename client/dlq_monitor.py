import time, logging
import xml.etree.ElementTree as ET
import urllib.request, urllib.error
from datetime import datetime

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [dlq-monitor] %(levelname)s: %(message)s")
log = logging.getLogger("dlq-monitor")

AMQ_HOST      = "192.168.56.2"
AMQ_PORT      = 8161
AMQ_USER      = "admin"
AMQ_PASSWORD  = "admin"
CHECK_INTERVAL = 30   # seconds
DLQ_THRESHOLD  = 0    # alert if DLQ size > this
LOG_FILE       = "dlq_alerts.txt"

URL = f"http://{AMQ_HOST}:{AMQ_PORT}/admin/xml/queues.jsp"

def write_alert(line):
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def check_queues():
    try:
        # build request with basic auth
        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, URL, AMQ_USER, AMQ_PASSWORD)
        handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
        opener = urllib.request.build_opener(handler)

        with opener.open(URL, timeout=10) as response:
            xml_data = response.read().decode("utf-8")

        root = ET.fromstring(xml_data)

        # print status of all queues
        log.info("--- Queue Status ---")
        for queue in root.findall("queue"):
            name  = queue.get("name")
            stats = queue.find("stats")
            size  = int(stats.get("size", 0))
            enq   = stats.get("enqueueCount", "?")
            deq   = stats.get("dequeueCount", "?")
            consumers = stats.get("consumerCount", "?")
            log.info("%-35s size=%-4d enq=%-6s deq=%-6s consumers=%s",
                     name, size, enq, deq, consumers)

            # alert if this is a DLQ with messages
            if "DLQ" in name and size > DLQ_THRESHOLD:
                alert = (f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
                         f"ALERT: {name} has {size} dead letter message(s)! "
                         f"Messages are failing to be processed.")
                print(f"\n{'='*60}")
                print(f"  ⚠  {alert}")
                print(f"{'='*60}\n")
                write_alert(alert)

        # also alert if math.requests is backing up
        for queue in root.findall("queue"):
            if queue.get("name") == "math.requests":
                size = int(queue.find("stats").get("size", 0))
                if size > 10:
                    alert = (f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
                             f"WARNING: math.requests has {size} pending messages — "
                             f"dispatcher may be down!")
                    print(f"\n{'='*60}")
                    print(f"  ⚠  {alert}")
                    print(f"{'='*60}\n")
                    write_alert(alert)

    except urllib.error.URLError as e:
        log.error("Cannot reach ActiveMQ @ %s:%d — %s", AMQ_HOST, AMQ_PORT, e)
    except ET.ParseError as e:
        log.error("Failed to parse queue XML: %s", e)
    except Exception as e:
        log.error("Unexpected error: %s", e)

if __name__ == "__main__":
    log.info("DLQ Monitor started — checking every %ds. Alerts logged to %s",
             CHECK_INTERVAL, LOG_FILE)
    log.info("Watching: DLQ.math.requests (dead letters) + math.requests (backlog)")
    while True:
        check_queues()
        time.sleep(CHECK_INTERVAL)