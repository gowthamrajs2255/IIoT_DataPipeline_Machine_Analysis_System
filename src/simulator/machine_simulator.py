import sys
import os
import asyncio
from aiomqtt import Client
import random
from datetime import datetime,timezone
import json
from crypto import encrypt
from prometheus_client import start_http_server, Counter

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

MESSAGES_SENT = Counter("simulator_messages_sent_total", "Total messages sent by simulator", ["machine_id"])

async def simulate_machine(machine_id):
    prod_count=0
    mqtt_host = os.getenv("MQTT_HOST", "localhost")
    mqtt_port = int(os.getenv("MQTT_PORT", 1883))
    while True:
        try:
            async with Client(mqtt_host, port=mqtt_port, identifier=machine_id) as client:
                print(f"{machine_id} connected to MQTT broker")
                while True:
                    status = 1 if random.random()>0.1 else 0
                    if status==1:
                        prod_count+=1
                    temperature = random.uniform(35, 50) if random.random() > 0.05 else random.uniform(70, 85)
                    
                    data={
                        "machine_id": machine_id,
                        "timestamp": datetime.now(timezone.utc).isoformat(), 
                        "power": round(random.uniform(3, 8), 2),
                        "current": round(random.uniform(10, 20), 2),
                        "temperature": round(temperature, 2),
                        "production_count": prod_count,
                        "status": status
                    }
                    topic=f"factory/machine/{machine_id}"
                    await client.publish(topic, encrypt(json.dumps(data).encode()), qos=1)
                    MESSAGES_SENT.labels(machine_id=machine_id).inc() 
                    print(f"Published data for {machine_id}: {data}")
                    await asyncio.sleep(5)
        except Exception as e:
            print(f"{machine_id} connection lost: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)  

async def main():
    start_http_server(8001)
    machines=["MC1001", "MC1002", "MC1003", "MC1004", "MC1005"]
    # print(tasks)
    await asyncio.gather(*[simulate_machine(machine_id) for machine_id in machines])
    
if __name__ == "__main__":
    try :
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Simulation stopped by user.")