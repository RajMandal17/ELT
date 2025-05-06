#!/bin/bash

# Install dependencies
echo "Installing dependencies..."
pip install kafka-python matplotlib requests geopy pymongo

# Run the Kafka consumer application
echo "Running kafka_consumer.py..."
python3 kafka_consumer.py