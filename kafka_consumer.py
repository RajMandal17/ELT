from kafka import KafkaConsumer
import json
import struct
from enum import Enum

class CommandType(Enum):
    PLACE_ORDER = 1
    CANCEL_ORDER = 2
    DEPOSIT = 3
    WITHDRAWAL = 4
    PUT_PRODUCT = 5

    @staticmethod
    def value_of_byte(byte_value):
        for command_type in CommandType:
            if command_type.value == byte_value:
                return command_type
        raise ValueError(f"Unknown byte: {byte_value}")

def custom_deserializer(message_bytes):
    try:
        # Extract the command type from the first byte
        command_type_byte = message_bytes[0]
        command_type = CommandType.value_of_byte(command_type_byte)

        # Deserialize based on the command type
        if command_type == CommandType.PUT_PRODUCT:
            return json.loads(message_bytes[1:].decode('utf-8'))
        elif command_type == CommandType.DEPOSIT:
            return json.loads(message_bytes[1:].decode('utf-8'))
        elif command_type == CommandType.PLACE_ORDER:
            return json.loads(message_bytes[1:].decode('utf-8'))
        elif command_type == CommandType.CANCEL_ORDER:
            return json.loads(message_bytes[1:].decode('utf-8'))
        else:
            print("Unhandled command type:", command_type)
            return None
    except Exception as e:
        print("Deserialization error:", e)
        return None

def consume_kafka_topic():
    # Create a Kafka consumer for the 'matching-engine-command' topic
    consumer = KafkaConsumer(
        'matching-engine-command',
        bootstrap_servers='localhost:9092',
        value_deserializer=custom_deserializer
    )

    print("Listening to Kafka topic 'matching-engine-command'...")

    # Add debugging logs to inspect raw messages
    for message in consumer:
        try:
            print("Raw message received (bytes):", message.value)
            if message.value:
                data = custom_deserializer(message.value)
                print("Deserialized message:", data)
            else:
                print("Empty message received.")
        except Exception as e:
            print("Unexpected error while processing message:", e)

        # Log message metadata for debugging
        print("Message metadata - Offset:", message.offset, "Partition:", message.partition)

if __name__ == "__main__":
    consume_kafka_topic()