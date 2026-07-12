# Meshtastic kafka

Ingest meshtastic data into kafka via MQTT.

Currently working via multiple asyncio scripts, although moving to flink for intermediate jobs.

Structure:

- [bridge](meshtastic_kafka/bridge/): script to log data from MQTT and add it into kafka topic.
- [decoder](meshtastic_kafka/decoder/): script to decode meshtastic data based on protobuf:
    - Pending:
        - control over encryption key
        - move to flink
- [writer](meshtastic_kafka/writer/): script to store data into influxDB

Note: for iterations on protobufs, install meshtastic locally as editable and build protobufs locally.