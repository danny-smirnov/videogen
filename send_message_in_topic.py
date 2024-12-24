from confluent_kafka import Producer
import json

def delivery_report(err, msg):
    """Callback to confirm message delivery."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_message(broker, topic, message):
    """Send a message to a Kafka topic."""
    producer = Producer({'bootstrap.servers': broker})

    try:
        # Serialize message to JSON
        serialized_message = json.dumps(message)

        # Send message
        producer.produce(topic, key=message.get("video_id"), value=serialized_message, callback=delivery_report)
        producer.flush()  # Ensure all messages are sent
        print(f"Message sent to topic '{topic}': {serialized_message}")
    except Exception as e:
        print(f"Error sending message: {e}")

if __name__ == "__main__":
    broker = "localhost:9092"
    topic = "video_processing"

    # Example message
    message = {
        "video_id": "12345",
        "channel_id": "channel_1",
        "s3_key": "videos/12345.mp4"
    }

    send_message(broker, topic, message)
