#!/bin/bash

set -e

KAFKA_BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:29092}"
TOPIC_NAME="${TOPIC_NAME:-aura-plan}"
PARTITIONS="${PARTITIONS:-1}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"

echo "Waiting for Kafka to be ready..."
for i in {1..30}; do
    if kafka-broker-api-versions --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" > /dev/null 2>&1; then
        echo "Kafka is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "Kafka did not become ready in time"
        exit 1
    fi
    echo "Waiting for Kafka... ($i/30)"
    sleep 2
done

echo "Checking if topic '$TOPIC_NAME' exists..."
if kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" --list | grep -q "^${TOPIC_NAME}$"; then
    echo "Topic '$TOPIC_NAME' already exists. Skipping creation."
else
    echo "Creating topic '$TOPIC_NAME' with $PARTITIONS partitions and replication factor $REPLICATION_FACTOR..."
    kafka-topics --create \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --topic "$TOPIC_NAME" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION_FACTOR" \
        --if-not-exists || true
    
    echo "Verifying topic creation..."
    if kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" --list | grep -q "^${TOPIC_NAME}$"; then
        echo "Topic '$TOPIC_NAME' created successfully!"
    else
        echo "Warning: Topic creation may have failed, but continuing..."
    fi
fi

echo "Topic initialization complete!"

