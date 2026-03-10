import asyncio
import sys
import json
import os
import ssl
import asyncpg
from aiomqtt import Client, MqttError
from datetime import datetime
from prometheus_client import Counter, start_http_server
from validator import validate
from crypto import decrypt

# Prometheus metrics (exposed at /metrics via prometheus_client HTTP server)
MESSAGES_RECEIVED = Counter("consumer_messages_received_total", "Total received messages")
VALIDATION_ERRORS = Counter("consumer_validation_errors_total", "Total messages that failed validation")
MESSAGES_CONSUMED = Counter("consumer_messages_consumed_total", "Total messages stored in QuestDB")
PROCESSING_ERRORS = Counter("consumer_processing_errors_total", "Total exceptions during processing")

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

try:
    from init_db import main as run_init_db
except ImportError:
    run_init_db = None


async def consume():
    start_http_server(8002) 
    # 1. QuestDB Connection with Retry
    db = None
    while db is None:
        try:
            db = await asyncpg.connect(
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", 8812)),
                database=os.getenv("DB_NAME", "qdb")
            )
            table_exists = await db.fetchval(
                "SELECT count(*) FROM tables() WHERE table_name = 'machine_data'"
            )

            if table_exists == 0:
                print("Table 'machine_data' not found. Running initialization...")
                if run_init_db:
                    await run_init_db()
                    print("Initialization complete.")
                else:
                    print("Error: Could not find run_init_db function!")
            else:
                print("Table 'machine_data' verified.")

            print("Successfully connected to QuestDB!")
        except Exception as e:
            print(f"QuestDB not ready ({e}). Retrying in 5s...")
            await asyncio.sleep(5)

    # 2. MQTT Connection
    mqtt_host = os.getenv("EMQX_HOST")
    mqtt_port = int(os.getenv("EMQX_PORT", 8883))
    use_tls = mqtt_port == 8883
    tls_context = ssl.create_default_context() if use_tls else None

    try:
        async with Client(
            hostname=mqtt_host,
            port=mqtt_port,
            username=os.getenv("EMQX_USER") or os.getenv("MQTT_USER"),
            password=os.getenv("EMQX_PASS") or os.getenv("MQTT_PASS"),
            tls_context=tls_context,
            timeout=30,
        ) as client:
            print(f"Successfully connected to MQTT broker ({mqtt_host}:{mqtt_port})")
            await client.subscribe("factory/machine/+")

            async for message in client.messages:
                try:
                    decrypted = decrypt(message.payload)
                    data = json.loads(decrypted)

                    MESSAGES_RECEIVED.inc()
                    if not validate(data):
                        VALIDATION_ERRORS.inc()
                        print(f"Validation failed for message: {data}")
                        continue

                    ts = datetime.fromisoformat(
                        data["timestamp"].replace("Z", "+00:00")
                    ).replace(tzinfo=None)

                    await db.execute(
                        """
                        INSERT INTO machine_data (
                            machine_id,
                            timestamp,
                            power,
                            current,
                            temperature,
                            production_count,
                            status
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """,
                        data["machine_id"],
                        ts,
                        data["power"],
                        data.get("current"),
                        data["temperature"],
                        data["production_count"],
                        data["status"],
                    )

                    MESSAGES_CONSUMED.inc()
                    print(f"Data stored for {data['machine_id']}")
                except Exception as e:
                    PROCESSING_ERRORS.inc()
                    print(f"Processing error: {e}")

    except (MqttError, asyncio.CancelledError) as e:
        print(f"Stopping consumer: {e}")

    finally:
        # 3. Graceful Shutdown of Database
        if db:
            print("Closing QuestDB connection...")
            await db.close()
            print("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        # Silences the messy traceback when pressing Ctrl+C
        pass
