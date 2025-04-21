from flask import Flask, render_template
import matplotlib.pyplot as plt
import io
import base64
from collections import defaultdict
from kafka import KafkaConsumer
import json
import threading
from queue import Queue
from enum import Enum

app = Flask(__name__)

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

# Kafka consumer setup
consumer = KafkaConsumer(
    'matching-engine-command',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: custom_deserializer(m)
)

# Shared queue to store Kafka messages
message_queue = Queue()

def custom_deserializer(message_bytes):
    try:
        if not message_bytes:
            print("Empty message received.")
            return None

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
    except json.JSONDecodeError as e:
        print("Invalid JSON message received:", message_bytes)
        print("Error:", e)
        return None
    except Exception as e:
        print("Deserialization error:", e)
        return None

# Function to consume Kafka messages in a separate thread
def kafka_consumer_thread():
    for message in consumer:
        try:
            data = message.value
            if data.get('type') == 'PLACE_ORDER':
                message_queue.put(data)
        except Exception as e:
            print("Error processing Kafka message:", e)

# Start the Kafka consumer thread
threading.Thread(target=kafka_consumer_thread, daemon=True).start()

# Function to fetch data from the shared queue
def fetch_kafka_data():
    orders = []
    while not message_queue.empty() and len(orders) < 10:
        order = message_queue.get()
        print("Fetched order from queue:", order)  # Log each fetched order
        orders.append(order)
    print("Total orders fetched:", len(orders))  # Log the total number of orders fetched
    return orders

def generate_pie_chart(product_id, status_counts):
    labels = status_counts.keys()
    sizes = status_counts.values()

    plt.figure(figsize=(6, 6))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title(f"Order Status Distribution for Product ID: {product_id}")

    # Save the plot to a BytesIO object
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plt.close()
    return base64.b64encode(img.getvalue()).decode('utf-8')

@app.route('/')
def index():
    # Fetch data from Kafka
    orders = fetch_kafka_data()

    # Analyze orders based on productId and status
    product_status_counts = defaultdict(lambda: {'FILLED': 0, 'OPEN': 0})

    for order in orders:
        product_id = order.get('productId')
        status = order.get('status', 'OPEN')  # Default to 'OPEN' if status is missing
        if product_id:
            product_status_counts[product_id][status] += 1

    # Generate pie charts for each productId
    charts = {}
    for product_id, status_counts in product_status_counts.items():
        charts[product_id] = generate_pie_chart(product_id, status_counts)

    return render_template('index.html', charts=charts)

if __name__ == '__main__':
    app.run(debug=True)