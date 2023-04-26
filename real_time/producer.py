import json
import time
from kafka import KafkaProducer
import urllib.request

api_request_limiter = 10
sleep_time = 10  # in seconds

topic_name = "near_earth_objects"
kafka_broker = 'kafka:9092'

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[kafka_broker],
    value_serializer=lambda m: json.dumps(m).encode('ascii'),
    buffer_memory=33554432,
    request_timeout_ms=10000
)

# Get today's date
today = time.strftime('%Y-%m-%d', time.gmtime())

for i in range(api_request_limiter):
    
    # Get the date for the next day
    date = time.strftime('%Y-%m-%d', time.gmtime(time.time() + 86400))
    print(f"Getting data for {date}")

    # Retrieve data 
    url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={date}&end_date={date}&api_key=vbdXqVkpa9sPqau3R1UWjzFeeHA6iZuD0cKivpFO"
    response = urllib.request.urlopen(url)
    result = json.loads(response.read())

    # Parse the JSON data and send it to Kafka
    asteroids = result["near_earth_objects"]
    for asteroid in asteroids:
        for field in asteroids[asteroid]:
            data = {
                "asteroid_name": field["name"],
                "estimated_diameter": round((field["estimated_diameter"]["meters"]["estimated_diameter_min"] + field["estimated_diameter"]["meters"]["estimated_diameter_max"]) / 2, 0),
                "close_approach_date_time": field["close_approach_data"][0]["close_approach_date_full"],
                "velocity_km_h": field["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"],
                "distance_to_earth_km": field["close_approach_data"][0]["miss_distance"]["kilometers"],
                "is_potentially_hazardous_asteroid": field["is_potentially_hazardous_asteroid"]
            }
            producer.send(topic_name, data)

    time.sleep(sleep_time)