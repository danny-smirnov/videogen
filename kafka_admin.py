from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
load_dotenv()

def create_topic_if_not_exists(broker, topic_name, num_partitions=1, replication_factor=1):
    """
    Creates a Kafka topic if it does not exist.

    :param broker: Kafka broker string (e.g., 'localhost:9092').
    :param topic_name: The name of the topic to create.
    :param num_partitions: Number of partitions for the topic.
    :param replication_factor: Replication factor for the topic.
    """
    admin_client = AdminClient({'bootstrap.servers': broker})

    # Fetch existing topics
    existing_topics = admin_client.list_topics(timeout=5).topics

    if topic_name in existing_topics:
        print(f"Topic '{topic_name}' already exists.")
        return

    # Create the topic
    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    try:
        # AdminClient.create_topics() returns a future per topic
        futures = admin_client.create_topics([new_topic])
        for topic, future in futures.items():
            try:
                future.result()  # Wait for operation to finish
                print(f"Topic '{topic}' created successfully.")
            except Exception as e:
                print(f"Failed to create topic '{topic}': {e}")
    except Exception as e:
        print(f"Error in topic creation: {e}")

# Usage example
if __name__ == "__main__":
    create_topic_if_not_exists(
        broker="localhost:9092",
        topic_name="video_processing",
        num_partitions=3,
        replication_factor=1
    )
