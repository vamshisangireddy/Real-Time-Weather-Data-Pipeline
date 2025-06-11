import os
import json
import time
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer, KafkaException

# Load environment variables from .env file
load_dotenv()

# --- Kafka Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = 'weather_raw_data'

# --- Weather API Configuration ---
# Correct endpoint for WeatherAPI.com current weather data
WEATHER_API_URL = os.getenv('WEATHER_API_URL', 'http://api.weatherapi.com/v1/current.json')
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')

# --- Producer Callback for Delivery Reports ---
def delivery_report(err, msg):
    """
    Callback function to report on the delivery status of a message.
    """
    if err is not None:
        print(f"ERROR: Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic '{msg.topic()}' "
              f"[Partition: {msg.partition()}] @ offset {msg.offset()}")

# --- Function to Fetch Weather Data from API ---
from typing import Optional

def fetch_weather_data(city_name: str, api_key: str) -> Optional[dict]:
    """
    Fetches current weather data for a given city from WeatherAPI.com.
    Corrected to use 'key' parameter for API key.
    """
    if not api_key:
        print("ERROR: WEATHER_API_KEY is not set.")
        return None

    # Correct parameters for WeatherAPI.com
    params = {
        'key': api_key, # Use 'key' for WeatherAPI.com
        'q': city_name, # Use 'q' for query/city name
        'aqi': 'no'     # Optional: 'yes' to include Air Quality Data
    }
    try:
        response = requests.get(WEATHER_API_URL, params=params, timeout=10)
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.Timeout:
        print(f"ERROR: Request to API timed out for city: {city_name}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to fetch weather data for {city_name}: {e}")
        return None

# --- Main Producer Logic ---
def produce_weather_events():
    """
    Continuously fetches weather data for predefined cities and publishes it to Kafka.
    """
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'weather-data-producer-app',
        'acks': 'all',
        'retries': 3,
        'compression.type': 'snappy',
        'linger.ms': 5,
        'batch.size': 16384
    }

    try:
        producer = Producer(producer_conf)
    except KafkaException as e:
        print(f"ERROR: Failed to create Kafka Producer: {e}")
        return

    cities = ['London', 'New York', 'Tokyo', 'Paris', 'Berlin', 'Sydney']
    polling_interval_seconds = 300 # Fetch data every 5 minutes (300 seconds)

    print(f"Starting weather data producer. Sending to topic: {KAFKA_TOPIC}")
    print(f"Kafka Brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Weather API URL: {WEATHER_API_URL}")

    while True:
        for city in cities:
            weather_data = fetch_weather_data(city, WEATHER_API_KEY)
            if weather_data:
                # Add an ingestion timestamp to the data
                weather_data['ingestion_timestamp_ms'] = int(time.time() * 1000)

                key = city.encode('utf-8')
                value = json.dumps(weather_data).encode('utf-8')

                try:
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=key,
                        value=value,
                        callback=delivery_report
                    )
                    producer.poll(0)
                except BufferError:
                    print('Local producer queue full, waiting for messages to be delivered...')
                    producer.poll(1)
                except Exception as e:
                    print(f"ERROR: Failed to produce message for {city}: {e}")
            else:
                print(f"Skipping production for {city} due to data fetching error.")

        producer.flush()
        print(f"All messages flushed for this cycle. Waiting {polling_interval_seconds} seconds...")
        time.sleep(polling_interval_seconds)

if __name__ == '__main__':
    if not os.getenv('WEATHER_API_KEY'):
        print("CRITICAL ERROR: WEATHER_API_KEY environment variable is not set. Please set it in .env or your system.")
        exit(1)
    if not os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
        print(f"WARNING: KAFKA_BOOTSTRAP_SERVERS not set. Using default: {KAFKA_BOOTSTRAP_SERVERS}")

    produce_weather_events()
