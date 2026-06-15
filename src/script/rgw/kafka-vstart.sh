#!/bin/bash
#
# Bootstrap a local Kafka broker for RGW notification testing.
#
# Starts a single-node Kafka container via podman (KRaft mode, no
# ZooKeeper), creates a health-check topic, and writes the [kafka]
# section to s3tests.conf.
#
# Prerequisites:
#   - podman
#   - radosgw running (from rgw-vstart.sh)
#   - S3TEST_CONF pointing to s3tests.conf (or ./s3tests.conf in build dir)
#
# Usage:
#   kafka-vstart.sh              # start + provision
#   kafka-vstart.sh --stop       # stop + remove container
#
# The RGW notification endpoint URL for topics is:
#   kafka://localhost:9092

set -euo pipefail

# rootless podman needs the systemd user bus; some sessions (screen, tmux,
# ssh without ForwardAgent) inherit a stale DBUS_SESSION_BUS_ADDRESS
if [[ -S "/run/user/$(id -u)/bus" ]]; then
	export DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/$(id -u)/bus"
fi

KAFKA_PORT=9092
KAFKA_CONTAINER=kafka-vstart
KAFKA_IMAGE=docker.io/apache/kafka:latest

BUILD_DIR="$(pwd)"
S3CONF="${S3TEST_CONF:-$BUILD_DIR/s3tests.conf}"

if [[ "${1:-}" == "--stop" ]]; then
	echo "==> stopping Kafka container"
	podman stop "$KAFKA_CONTAINER" 2>/dev/null || true
	podman rm "$KAFKA_CONTAINER" 2>/dev/null || true
	exit 0
fi

if [[ ! -f "$S3CONF" ]]; then
	echo "error: $S3CONF not found" >&2
	echo "Run rgw-vstart.sh first, or set S3TEST_CONF to your config file." >&2
	exit 1
fi

if ! command -v podman &>/dev/null; then
	echo "error: podman is required" >&2
	exit 1
fi

# --- start Kafka (reuse if already running) ---

if podman ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
	echo "==> Kafka container already running"
else
	# remove stopped container if it exists
	podman rm "$KAFKA_CONTAINER" 2>/dev/null || true

	echo "==> starting Kafka on port $KAFKA_PORT (KRaft mode, no ZooKeeper)"
	podman run -d --name "$KAFKA_CONTAINER" \
		--network=host \
		-e KAFKA_NODE_ID=1 \
		-e KAFKA_PROCESS_ROLES=broker,controller \
		-e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:${KAFKA_PORT},CONTROLLER://0.0.0.0:9093" \
		-e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://127.0.0.1:${KAFKA_PORT}" \
		-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
		-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT" \
		-e KAFKA_CONTROLLER_QUORUM_VOTERS="1@localhost:9093" \
		-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
		-e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
		-e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
		-e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
		-e CLUSTER_ID="$(cat /proc/sys/kernel/random/uuid | tr -d '-' | head -c 22)AAAA" \
		"$KAFKA_IMAGE"

	echo -n "==> waiting for Kafka to start"
	for i in $(seq 1 30); do
		if podman exec "$KAFKA_CONTAINER" \
			/opt/kafka/bin/kafka-metadata.sh --snapshot /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log --cluster-id dummy 2>/dev/null | grep -q 'MetadataVersion'; then
			echo " ready"
			break
		fi
		echo -n "."
		sleep 2
	done

	# simpler readiness check: try to list topics
	if ! podman exec "$KAFKA_CONTAINER" \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:${KAFKA_PORT} --list 2>/dev/null; then
		echo ""
		echo "warning: Kafka may not be fully ready, but container is running" >&2
	fi
fi

# --- write [kafka] to s3tests.conf ---

echo "==> writing [kafka] to $S3CONF"

# remove existing section if present
if grep -q '^\[kafka\]' "$S3CONF" 2>/dev/null; then
	sed -i '/^\[kafka\]/,/^\[/{/^\[kafka\]/d;/^\[/!d}' "$S3CONF"
fi

cat >> "$S3CONF" << EOF

[kafka]
broker = 127.0.0.1:${KAFKA_PORT}
EOF

echo ""
echo "==> Kafka ready on port ${KAFKA_PORT}"
echo "    RGW endpoint URL: kafka://localhost:${KAFKA_PORT}"
echo ""
echo "To stop: $0 --stop"
