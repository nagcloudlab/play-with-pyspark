# Kafka Topic Creation for Financial Streaming Workshop

# 1. Verify Kafka is running
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# 2. Create financial data topics
echo "Creating financial streaming topics..."

# Main transaction stream
./bin/kafka-topics.sh --create \
  --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Customer account updates
./bin/kafka-topics.sh --create \
  --topic account-updates \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1

# Fraud alerts (we'll use this later)
./bin/kafka-topics.sh --create \
  --topic fraud-alerts \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1

# Market data stream (for later phases)
./bin/kafka-topics.sh --create \
  --topic market-data \
  --bootstrap-server localhost:9092 \
  --partitions 4 \
  --replication-factor 1

# Verify topics were created
echo "Verifying topics..."
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Check topic details
./bin/kafka-topics.sh --describe --topic transactions --bootstrap-server localhost:9092