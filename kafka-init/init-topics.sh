#!/bin/bash
# Don't use set -e, handle errors explicitly

KAFKA_BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:29092}"
TOPICS_TO_CREATE="${TOPICS_TO_CREATE:-aura-plan:1:1}"

# Wait for Kafka
echo "Waiting for Kafka to be ready..."
KAFKA_READY=false
for i in {1..60}; do
    if kafka-broker-api-versions --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" > /dev/null 2>&1; then
        echo "Kafka is ready!"
        KAFKA_READY=true
        break
    fi
    echo "Waiting for Kafka... ($i/60)"
    sleep 2
done

if [ "$KAFKA_READY" != "true" ]; then
    echo "ERROR: Kafka did not become ready after 120 seconds"
    exit 1
fi

# Wait a bit more for Kafka to fully initialize
sleep 5

# Create Topics
echo "Creating topics: $TOPICS_TO_CREATE"
# Split by comma
IFS=',' read -ra ADDR <<< "$TOPICS_TO_CREATE"
CREATED_COUNT=0
FAILED_COUNT=0

for topic_str in "${ADDR[@]}"; do
    # Split by colon
    IFS=':' read -r name partitions replicas <<< "$topic_str"
    
    if [ -z "$name" ]; then 
        echo "WARNING: Empty topic name, skipping"
        continue
    fi
    
    partitions="${partitions:-1}"
    replicas="${replicas:-1}"
    
    echo "Creating topic $name (P:$partitions, R:$replicas)"
    if kafka-topics --create --if-not-exists \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --topic "$name" \
        --partitions "$partitions" \
        --replication-factor "$replicas" 2>&1; then
        echo "âœ“ Topic $name created successfully"
        CREATED_COUNT=$((CREATED_COUNT + 1))
    else
        echo "âœ— Failed to create topic $name (may already exist)"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
done

echo ""
echo "Initialization complete. Created: $CREATED_COUNT, Failed: $FAILED_COUNT"

# Verify topics exist
echo "Verifying topics..."
for topic_str in "${ADDR[@]}"; do
    IFS=':' read -r name partitions replicas <<< "$topic_str"
    if [ -n "$name" ]; then
        if kafka-topics --list --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" | grep -q "^${name}$"; then
            echo "âœ“ Topic $name verified"
        else
            echo "âœ— Topic $name NOT found"
        fi
    fi
done

exit 0


