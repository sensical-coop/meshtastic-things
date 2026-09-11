# Meshtastic Things

Meshtastic Things is a system for ingesting Meshtastic mesh telemetry from MQTT into Kafka,
decode/decrypt it, and store it. It provides an advanced system for:

- Sensor data storage on timeseries databases (InfluxDB) and S3-like buckets as Parquet archives
- Process data on-the-fly or on batch, to perform:
  + Sensor calibration
  + Pattern detection and data-quality checks
  + Alarms (threshold breach, malfunctioning, etc.) via email or other measurements
  + Sensor / measurement catalog
  + Per-owner access control
- Querys for sensor data
- Fleet management (registration of meshes, devices, data processing pipelines).

## Stack

- Message store and pipeline: Apache Kafka
- Decode and on-the-fly processing: pure python (Lite) and Apache Flink (Full)
- Storage: InfluxDB and S3/MinIO
- APIs (keys and query): Django

Data pipeline:

```
MQTT ──> bridge ──> mesh.telemetry.raw.v1 ──> decode/decrypt ──> mesh.telemetry.decoded.v1
                                                     ^                          │
                                          mesh.keys.v1 (gateway PSKs)           ├──> influxdb_writer ──> InfluxDB ──> queryapi
                                          from keyapi                           └──> s3_writer ──────> Parquet on S3/MinIO
```

## Getting started

```bash
cp env.example .env    # then see docs/README.md#quick-start
docker compose --profile full up -d
```

**Note**: For iterations on protobufs, install `meshtastic` locally as editable and
build protobufs locally — see [`scripts/regen-protos.sh`](meshtastic_things/scripts/regen-protos.sh).

**Warning**: Nothing reaches InfluxDB until a mesh has a **registered gateway** — that
registration is the gate that authorizes decoding.
