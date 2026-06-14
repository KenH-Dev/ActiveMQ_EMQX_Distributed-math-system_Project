import os, json, time, logging
import paho.mqtt.client as mqtt
import psycopg2

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [db-writer] %(levelname)s: %(message)s")
log = logging.getLogger("s3-db-writer")

MQTT_HOST    = os.environ["MQTT_HOST"]
MQTT_PORT    = int(os.environ.get("MQTT_PORT", 1883))
RESULT_TOPIC = os.environ.get("RESULT_MQTT_TOPIC", "worker/+/results")

DB_HOST     = os.environ["DB_HOST"]
DB_PORT     = int(os.environ.get("DB_PORT", 5432))
DB_NAME     = os.environ["DB_NAME"]
DB_USER     = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS results (
    id          SERIAL PRIMARY KEY,
    task_id     TEXT NOT NULL,
    operation   TEXT,
    operands    JSONB,
    result      DOUBLE PRECISION,
    status      TEXT,
    worker_id   TEXT,
    duration_ms DOUBLE PRECISION,
    received_at TIMESTAMPTZ DEFAULT now()
);
"""

INSERT_SQL = """
INSERT INTO results (task_id, operation, operands, result, status, worker_id, duration_ms)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASSWORD)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
            log.info("Connected to Postgres @ %s:%d, table ready", DB_HOST, DB_PORT)
            return conn
        except Exception as e:
            log.warning("DB not ready (%s) - retry in 3s", e)
            time.sleep(3)

def safe_insert(db, data):
    try:
        with db.cursor() as cur:
            cur.execute(INSERT_SQL, (
                data.get("task_id"),
                data.get("operation"),
                json.dumps(data.get("operands")),
                data.get("result"),
                data.get("status"),
                data.get("worker_id"),
                data.get("duration_ms"),
            ))
        log.info("Saved task %s (%s) result=%s",
                 data.get("task_id","?")[:8],
                 data.get("operation"),
                 data.get("result"))
        return db
    except psycopg2.OperationalError:
        log.warning("DB connection lost - reconnecting")
        return get_db_connection()
    except Exception as e:
        log.error("Insert error: %s", e)
        return db

db = get_db_connection()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(RESULT_TOPIC, qos=1)
        log.info("Subscribed to MQTT: %s", RESULT_TOPIC)
    else:
        log.error("MQTT connect failed rc=%d", rc)

def on_message(client, userdata, msg):
    global db
    try:
        data = json.loads(msg.payload.decode())
        db = safe_insert(db, data)
    except Exception as e:
        log.error("Message error: %s", e)

client = mqtt.Client(client_id="db-writer")
client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        client.connect(MQTT_HOST, MQTT_PORT)
        client.loop_forever()
    except Exception as e:
        log.error("MQTT error: %s - retry in 5s", e)
        time.sleep(5)
