import json
from datetime import datetime
from confluent_kafka import Consumer
from clickhouse_driver import Client

# ==========================================================
# Configuration
# ==========================================================
KAFKA_TOPIC = "postgres.public.raw_users"
KAFKA_BOOTSTRAP_SERVERS = "redpanda:9092"

CLICKHOUSE_HOST = "host.docker.internal"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "123456"
CLICKHOUSE_DB = "user_events"
CLICKHOUSE_TABLE = "users_cleaned"

# ==========================================================
# Helper Functions
# ==========================================================
def to_bool_int(value):
    return 1 if str(value).lower() in ["1", "true", "yes"] else 0

def parse_datetime(val):
    try:
        if val:
            return datetime.fromisoformat(val.replace("Z", ""))
    except Exception:
        pass
    return None

def parse_date(val):
    try:
        if val:
            return datetime.fromisoformat(val.replace("Z", "")).date()
    except Exception:
        pass
    return None

# ==========================================================
# Table Creation
# ==========================================================
def create_clickhouse_table():
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

    client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
        uuid String,
        id String,
        first_name String,
        last_name String,
        email String,
        email_opt_in UInt8,
        gender String,
        age UInt8,
        dob Date,
        created_at DateTime,
        last_updated_at Nullable(DateTime),
        last_login_at Nullable(DateTime),
        is_active UInt8,
        is_verified UInt8,
        registration_source String,
        username String,
        account_level String,
        total_orders UInt32,
        total_logins UInt32,
        total_spent Float64,
        avg_order_value Float64,
        risk_score Float32,
        country String,
        city String,
        street_address String,
        postal_code String,
        nat String,
        phone String,
        cell String,
        timezone String,
        latitude Float64,
        longitude Float64,
        inserted_at_clickhouse DateTime DEFAULT now()
    ) ENGINE = MergeTree
    ORDER BY (created_at, id)
    """

    client.execute(ddl)

    print(f" ClickHouse table '{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}' is ready.")

    return client

# ==========================================================
# Message Processing
# ==========================================================
def process_message(msg_value):
    try:
        payload_section = msg_value.get("payload", {})
        after_data = payload_section.get("after")

        if not after_data:
            return None

        if "payload" in after_data:
            user_raw = after_data["payload"]
        else:
            user_raw = after_data

        user_data = json.loads(user_raw) if isinstance(user_raw, str) else user_raw

        if not isinstance(user_data, dict):
            return None

        return (
            str(user_data.get("uuid", user_data.get("id", ""))),
            str(user_data.get("id", "")),
            user_data.get("first_name", ""),
            user_data.get("last_name", ""),
            user_data.get("email", ""),
            to_bool_int(user_data.get("email_opt_in", False)),
            user_data.get("gender", ""),
            int(user_data.get("age") or 0),
            parse_date(user_data.get("dob")),
            parse_datetime(user_data.get("created_at")) or datetime.now(),
            parse_datetime(user_data.get("last_updated_at")),
            parse_datetime(user_data.get("last_login_at")),
            to_bool_int(user_data.get("is_active", False)),
            to_bool_int(user_data.get("is_verified", False)),
            user_data.get("registration_source", ""),
            user_data.get("username", ""),
            user_data.get("account_level", ""),
            int(user_data.get("total_orders") or 0),
            int(user_data.get("total_logins") or 0),
            float(user_data.get("total_spent") or 0.0),
            float(user_data.get("avg_order_value") or 0.0),
            float(user_data.get("risk_score") or 0.0),
            user_data.get("country", ""),
            user_data.get("city", ""),
            user_data.get("street_address", ""),
            user_data.get("postal_code", ""),
            user_data.get("nat", ""),
            user_data.get("phone", ""),
            user_data.get("cell", ""),
            user_data.get("timezone", ""),
            float(user_data.get("latitude") or 0.0),
            float(user_data.get("longitude") or 0.0),
        )

    except Exception as e:
        print(f" Error processing message: {e}")
        return None


# ==========================================================
# Main Runner
# ==========================================================
def run_consumer():

    ch_client = create_clickhouse_table()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "clickhouse_consumer_v6",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([KAFKA_TOPIC])

    print(f" Consumer started. Reading messages from '{KAFKA_TOPIC}'...")

    batch = []
    batch_size = 10

    while True:

        msg = consumer.poll(1.0)

        # اگر پیام جدیدی نبود یعنی صف خالی شده → خروج
        if msg is None:
            break

        if msg.error():
            print(f" Kafka error: {msg.error()}")
            continue

        try:
            value = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            print("JSON decode error:", e)
            continue

        clean_row = process_message(value)

        if clean_row:
            batch.append(clean_row)

        if len(batch) >= batch_size:

            try:

                ch_client.execute(
                    f"""
                    INSERT INTO {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
                        uuid, id, first_name, last_name, email, email_opt_in,
                        gender, age, dob, created_at, last_updated_at, last_login_at,
                        is_active, is_verified, registration_source, username, account_level,
                        total_orders, total_logins, total_spent, avg_order_value, risk_score,
                        country, city, street_address, postal_code, nat, phone, cell, timezone,
                        latitude, longitude
                    ) VALUES
                    """,
                    batch,
                )

                print(f" Inserted {len(batch)} record(s) into ClickHouse.")
                batch.clear()

            except Exception as e:
                print(f" ClickHouse Insert Error: {e}")

    # insert remaining records
    if batch:
        try:
            ch_client.execute(
                f"""
                INSERT INTO {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
                    uuid, id, first_name, last_name, email, email_opt_in,
                    gender, age, dob, created_at, last_updated_at, last_login_at,
                    is_active, is_verified, registration_source, username, account_level,
                    total_orders, total_logins, total_spent, avg_order_value, risk_score,
                    country, city, street_address, postal_code, nat, phone, cell, timezone,
                    latitude, longitude
                ) VALUES
                """,
                batch,
            )
            print(f" Inserted remaining {len(batch)} record(s) into ClickHouse.")
        except Exception as e:
            print(f" ClickHouse Insert Error: {e}")

    consumer.close()
    print(" Consumer finished. No more messages in topic.")


if __name__ == "__main__":
    run_consumer()
