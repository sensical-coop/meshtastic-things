docker exec -it kafka bin/kafka-topics.sh \
  --create \
  --topic ${KAFKA_MQTT_BRIDGE_TOPIC} \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1