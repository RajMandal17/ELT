from kafka import KafkaConsumer
import json
import struct
from enum import Enum
import matplotlib.pyplot as plt
from collections import defaultdict
from geopy.geocoders import Nominatim
import requests
from geopy.distance import geodesic
from pymongo import MongoClient
from bson import ObjectId

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
     #   print("Deserialization error:", e)
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

def get_geolocation(ip_address):
    try:
        # Use an external API to fetch geolocation data
        response = requests.get(f"https://ipinfo.io/{ip_address}/json")
        if response.status_code == 200:
            data = response.json()
            city = data.get('city', 'Unknown')
            region = data.get('region', 'Unknown')
            country = data.get('country', 'Unknown')
            return f"{city}, {region}, {country}"
        else:
            return "Geolocation not found"
    except Exception as e:
        print("Error fetching geolocation:", e)
        return "Error"

def get_coordinates(ip_address):
    try:
        # Use an external API to fetch geolocation data
        response = requests.get(f"https://ipinfo.io/{ip_address}/json")
        if response.status_code == 200:
            data = response.json()
            loc = data.get('loc', None)  # 'loc' contains latitude and longitude as a string
            if loc:
                return tuple(map(float, loc.split(',')))
        return None
    except Exception as e:
        print("Error fetching coordinates:", e)
        return None

def calculate_distance(ip1, ip2):
    coords1 = get_coordinates(ip1)
    coords2 = get_coordinates(ip2)

    if coords1 and coords2:
        return geodesic(coords1, coords2).kilometers
    else:
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
    orders = []
    UserId = None
    reference_ip = "139.167.57.66"
    for message in consumer:
        try:
            print("Raw message received (bytes):", message.value)
            if message.value:
                data = message.value  # Directly use message.value as it is already deserialized
                # Collect PLACE_ORDER messages
                if data and data.get('type') == 'PLACE_ORDER':
                    orders.append(data)

                    # Extract ip_Address from the message
                    ip_address = "91.92.202.179"
                    print("Extracted IP Address:", ip_address)

                    # Fetch and print geolocation
                    geolocation = get_geolocation(ip_address)
                    print("Geolocation:", geolocation)

                    # Extract userId from the message
                    UserId = data.get('userId')
                    if UserId:
                        # Fetch user details from MongoDB using UserId
                        user = user_repo.find_by_user_id(UserId)
                        if user:
                            print("User details fetched from MongoDB:", user)

                            # Check if the extracted IP address is in the user's ipAddresses list
                            if ip_address not in user.get('ipAddresses', []):
                                user_repo.collection.update_one(
                                    {"_id": user["_id"]},
                                    {"$push": {"ipAddresses": ip_address}}
                                )
                                print(f"IP Address {ip_address} added to user's ipAddresses list.")
                            else:
                                print(f"IP Address {ip_address} already exists in user's ipAddresses list. Skipping update.")
                                                    
                        else:
                            print(f"No user found with user ID: {UserId}")
                    else:
                        print("UserId not found in the message.")


                    # Calculate and print distance to reference IP
                    distance = calculate_distance(reference_ip, ip_address)
                    if distance is not None:
                        print(f"Distance to {reference_ip}: {distance:.2f} km")
                    else:
                        print("Could not calculate distance.")

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

class User:
    def __init__(self, id, created_at, updated_at, email, password_hash, password_salt, two_step_verification_type, gotp_secret, nick_name, ip_addresses):
        self.id = id
        self.created_at = created_at
        self.updated_at = updated_at
        self.email = email
        self.password_hash = password_hash
        self.password_salt = password_salt
        self.two_step_verification_type = two_step_verification_type
        self.gotp_secret = gotp_secret
        self.nick_name = nick_name
        self.ip_addresses = ip_addresses

    def to_dict(self):
        return {
            "id": self.id,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "email": self.email,
            "passwordHash": self.password_hash,
            "passwordSalt": self.password_salt,
            "twoStepVerificationType": self.two_step_verification_type,
            "gotpSecret": self.gotp_secret,
            "nickName": self.nick_name,
            "ipAddresses": self.ip_addresses,
        }

class UserRepository:
    def __init__(self, database):
        # Use the lowercase name of the User class as the collection name
        self.collection = database[User.__name__.lower()]
        self.collection.create_index("email", unique=True)

    def find_by_user_id(self, user_id):
        # Query the `_id` field in the database to match the userId
        return self.collection.find_one({"_id": user_id})

    def save_or_update(self, user):
        # Insert or update user based on the id field
        self.collection.update_one(
            {"id": user["id"]},
            {"$set": user},
            upsert=True
        )

if __name__ == "__main__":
    # MongoDB connection URI
    MONGODB_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"

    # Connect to MongoDB
    try:
        client = MongoClient(MONGODB_URI)
        print("Connected to MongoDB successfully!")

        # Example: Access a database and collection
        db = client["gitbitex"]
        collection = db["user"]

        # Example usage
        user_repo = UserRepository(db)

        # Fetch user by email
        email = "test@example.com"
        user = user_repo.find_by_email(email)
        if user:
            print("User found by email:", user)
        else:
            print("No user found with email:", email)

        # Fetch user by user ID
       
        user = user_repo.find_by_user_id(UserId)
        if user:
            print("User found by user ID:", user)
        else:
            print("No user found with user ID:", user_id)
    except Exception as e:
        print("Error connecting to MongoDB:", e)

    consume_kafka_topic()