#!/bin/sh
# The broker's start for docker-compose.yml: the TLS keystore, a storage
# format that creates the SCRAM user, then the image's own start.
#
# Kafka reads a PEM keystore (KIP-651) as one file with the key and the chain,
# so the two PEM files of pascal-common-faa's test PKI are concatenated.
#
# SCRAM credentials live in the KRaft metadata log. kafka-storage.sh format
# can write them before the first start (--add-scram), but the image's start
# (/etc/kafka/docker/run, through KafkaDockerWrapper) formats with fixed
# arguments and no way to add any. So the storage is formatted here first,
# with the same cluster id, node id and log directory as the configuration
# the image writes from the KAFKA_* variables; the image's format then finds
# it formatted, which its launch script accepts ("already formatted"). The
# log directory is set in docker-compose.yml (KAFKA_LOG_DIRS) on purpose:
# with KAFKA_* variables the image doesn't read its own server.properties,
# and Kafka's default (/tmp/kafka-logs) would be formatted by the image
# alone, without the user (measured: SCRAM failed with the right password).
# The configuration given to this format only needs what format reads:
# roles, node id, the controller quorum and listener, and the log directory.
#
# Users (test credentials, no security value):
#   PLAIN          alice / alice-secret   (JAAS, in docker-compose.yml)
#   SCRAM-SHA-256  bob   / bob-secret     (here, 8192 iterations)
#
# Topics: once the broker answers, pascal-kafka-faa-it-zstd is created (one
# partition, compression.type=zstd), so that the broker compresses what it
# is given with Java's zstd: the integration suite reads zstd batches from
# it, which this library doesn't write. The image's start then runs in the
# background, and this shell, PID 1, passes SIGTERM on to it (docker stop).
# The healthcheck waits for the topic.
set -e
mkdir -p /tmp/tls
cat /pki/srv_key.pem /pki/srv_cert.pem > /tmp/tls/server.pem
if [ ! -f /tmp/kraft-combined-logs/meta.properties ]; then
  cat > /tmp/format.properties <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9094
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://:9092,CONTROLLER://:9094
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=/tmp/kraft-combined-logs
EOF
  /opt/kafka/bin/kafka-storage.sh format --cluster-id "$CLUSTER_ID" \
    --config /tmp/format.properties \
    --add-scram 'SCRAM-SHA-256=[name=bob,password=bob-secret,iterations=8192]'
fi
/etc/kafka/docker/run &
BROKER=$!
trap 'kill -TERM "$BROKER" 2>/dev/null' TERM INT
(
  until /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
      --if-not-exists --topic pascal-kafka-faa-it-zstd --partitions 1 \
      --config compression.type=zstd > /dev/null 2>&1; do
    sleep 1
  done
) &
# The first wait ends at a signal (the trap has passed it on), the second
# when the broker has stopped.
wait "$BROKER" || wait "$BROKER"
