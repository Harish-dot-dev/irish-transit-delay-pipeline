import os
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2

# Load the .env file so we can use TFI_API_KEY
load_dotenv()

# ---------- Config ----------
TFI_API_KEY = os.getenv("TFI_API_KEY")
KAFKA_TOPIC  = "transit-raw"
KAFKA_BROKER = "localhost:9092"
TFI_URL      = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"

# ---------- Why a producer? ----------
# KafkaProducer is the object that connects to Kafka and sends messages.
# value_serializer tells it how to convert our Python dict into bytes
# because Kafka only understands bytes, not Python objects.
# dict → json string → bytes  happens automatically on every send()
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_tfi_data():
    """
    Calls the TFI API and returns a decoded GTFS-RT feed.
    
    The API returns binary protobuf data — not JSON.
    Protobuf is a compact binary format Google designed for speed.
    gtfs_realtime_pb2 is the decoder that turns those bytes into
    a Python object we can loop over.
    """
    headers  = {"x-api-key": TFI_API_KEY}
    response = requests.get(TFI_URL, headers=headers)
    response.raise_for_status()  # Crash loudly if API call failed

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)  # Decode binary → Python object
    return feed

def extract_records(feed):
    """
    The feed is structured like this:
    
    feed
     └── entity (one per trip)
          └── trip_update
               ├── trip (trip_id, route_id)
               └── stop_time_update (one per stop)
                    ├── stop_id
                    ├── stop_sequence
                    └── arrival.delay  ← what we want (seconds late)
    
    We flatten this nested structure into individual flat records —
    one record per stop per trip. This is what lands in Kafka.
    """
    records      = []
    ingested_at  = datetime.utcnow().isoformat()

    for entity in feed.entity:

        # Skip entities that have no trip update
        if not entity.HasField("trip_update"):
            continue

        trip     = entity.trip_update.trip
        trip_id  = trip.trip_id
        route_id = trip.route_id

        for stop_update in entity.trip_update.stop_time_update:

            # delay is in seconds — positive = late, negative = early, 0 = on time
            delay_seconds = 0
            if stop_update.HasField("arrival"):
                delay_seconds = stop_update.arrival.delay

            record = {
                "trip_id"       : trip_id,
                "route_id"      : route_id,
                "stop_id"       : stop_update.stop_id,
                "stop_sequence" : stop_update.stop_sequence,
                "delay_seconds" : delay_seconds,
                "ingested_at"   : ingested_at
            }
            records.append(record)

    return records

def run():
    """
    Main loop — runs forever.
    Every 60 seconds:
      1. Fetch fresh data from TFI
      2. Extract flat records
      3. Publish each record as a message to Kafka
    
    producer.send()  — puts one message into the Kafka topic
    producer.flush() — forces Kafka to actually send buffered messages now
                       without flush(), messages sit in memory and may be lost
    """
    print(f"Producer started — publishing to topic: {KAFKA_TOPIC}")

    while True:
        try:
            feed    = fetch_tfi_data()
            records = extract_records(feed)

            for record in records:
                producer.send(KAFKA_TOPIC, value=record)

            producer.flush()
            print(f"[{datetime.utcnow().isoformat()}] Published {len(records)} records")

        except Exception as e:
            print(f"Error: {e}")

        time.sleep(60)  # Wait 60 seconds before next fetch

if __name__ == "__main__":
    run()