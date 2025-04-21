from kafka import KafkaConsumer
import json
import struct
from enum import Enum
import matplotlib.pyplot as plt
from collections import defaultdict

class CommandType(Enum):
    PLACE_ORDER = 1
    CANCEL_ORDER = 2
    DEPOSIT = 3
    WITHDRAWAL = 4
    PUT_PRODUCT = 5

    @staticmethod
    def value_of_byte(byte_value):
        try:
            for command_type in CommandType:
                if command_type.value == byte_value:
                    return command_type
            raise ValueError(f"Unknown byte: {byte_value}")
        except Exception as e:
            print(f"Error mapping byte to CommandType: {byte_value}", e)
            return None

def custom_deserializer(message_bytes):
    try:
        # Extract the command type from the first byte
        command_type_byte = message_bytes[0]
        command_type = CommandType.value_of_byte(command_type_byte)

        if command_type is None:
            print("Unknown command type byte received:", command_type_byte)
            return None

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
    except IndexError:
        print("Message is too short to contain a valid command type byte.")
        return None
    except Exception as e:
        print("Deserialization error:", e)
        return None

def analyze_and_plot_orders(orders):
    # Analyze orders based on productId and status
    product_status_counts = defaultdict(lambda: {'FILLED': 0, 'OPEN': 0})

    for order in orders:
        product_id = order.get('productId')
        status = order.get('status', 'OPEN')  # Default to 'OPEN' if status is missing
        if product_id:
            product_status_counts[product_id][status] += 1

    # Generate pie charts for each productId
    for product_id, status_counts in product_status_counts.items():
        labels = status_counts.keys()
        sizes = status_counts.values()

        plt.figure(figsize=(6, 6))
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
        plt.title(f"Order Status Distribution for Product ID: {product_id}")
        plt.show()

def consume_kafka_topic():
    # Create a Kafka consumer for the 'matching-engine-command' topic
    consumer = KafkaConsumer(
        'matching-engine-command',
        bootstrap_servers='localhost:9092',
        value_deserializer=custom_deserializer
    )

    print("Listening to Kafka topic 'matching-engine-command'...")

    # Add debugging logs to inspect raw messages
    orders = []
    for message in consumer:
        try:
            print("Raw message received (bytes):", message.value)
            if message.value:
                data = custom_deserializer(message.value)
                print("Deserialized message:", data)

                # Collect PLACE_ORDER messages
                if data and data.get('type') == 'PLACE_ORDER':
                    orders.append(data)

            else:
                print("Empty message received.")
        except Exception as e:
            print("Unexpected error while processing message:", e)

        # Log message metadata for debugging
        print("Message metadata - Offset:", message.offset, "Partition:", message.partition)

        # Analyze and plot orders after processing a batch of messages
        if len(orders) >= 10:  # Adjust the batch size as needed
            analyze_and_plot_orders(orders)
            orders.clear()

if __name__ == "__main__":
    consume_kafka_topic()