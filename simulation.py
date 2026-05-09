import redis
import json
import time
import random

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

NUM_BUSES = 10

# Initial positions (Ho Chi Minh area example)
buses = {}

for i in range(NUM_BUSES):
    buses[f"bus_{i}"] = {
        "bus_id": f"bus_{i}",
        "lat": 10.76 + random.uniform(-0.01, 0.01),
        "lng": 106.66 + random.uniform(-0.01, 0.01),
        "speed": random.randint(20, 60)
    }

print(buses)
print("🚍 Starting simulation...")

while True:
    for bus_id, bus in buses.items():

        # Simulate movement
        bus["lat"] += random.uniform(-0.0005, 0.0005)
        bus["lng"] += random.uniform(-0.0005, 0.0005)
        bus["speed"] = random.randint(20, 60)

        json_data = json.dumps(bus)

        # Store latest position
        r.hset("bus_latest", bus_id, json_data)

        # Publish update
        r.publish("bus_updates", json_data)

    time.sleep(1)
