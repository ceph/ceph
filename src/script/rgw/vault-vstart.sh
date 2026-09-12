#!/bin/bash
#
# Bootstrap a local HashiCorp Vault for RGW SSE-KMS / SSE-S3 testing.
#
# Starts Vault in dev mode via podman, enables the transit secret engine,
# and creates two transit keys (testkey-1, testkey-2) matching the
# s3tests-rs config.rs defaults.  Writes the root token to a file that
# RGW reads at runtime via rgw_crypt_vault_token_file.
#
# Prerequisites:
#   - podman
#   - curl
#
# Usage:
#   vault-vstart.sh              # start + provision
#   vault-vstart.sh --stop       # stop + remove container
#
# RGW ceph.conf options are injected by rgw-vstart.sh, not here.

set -euo pipefail

# rootless podman needs the systemd user bus
if [[ -S "/run/user/$(id -u)/bus" ]]; then
	export DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/$(id -u)/bus"
fi

VAULT_PORT=8200
VAULT_CONTAINER=vault-vstart
VAULT_IMAGE=docker.io/hashicorp/vault:latest
VAULT_ROOT_TOKEN=root
VAULT_ADDR="http://127.0.0.1:${VAULT_PORT}"

BUILD_DIR="$(pwd)"
VAULT_TOKEN_FILE="$BUILD_DIR/vault-token"

if [[ "${1:-}" == "--stop" ]]; then
	echo "==> stopping Vault container"
	podman stop "$VAULT_CONTAINER" 2>/dev/null || true
	podman rm "$VAULT_CONTAINER" 2>/dev/null || true
	exit 0
fi

if ! command -v podman &>/dev/null; then
	echo "error: podman is required" >&2
	exit 1
fi

if ! command -v curl &>/dev/null; then
	echo "error: curl is required" >&2
	exit 1
fi

# --- start Vault (reuse if already running) ---

if podman ps --format '{{.Names}}' | grep -q "^${VAULT_CONTAINER}$"; then
	echo "==> Vault container already running"
else
	# remove stopped container if it exists
	podman rm "$VAULT_CONTAINER" 2>/dev/null || true

	echo "==> starting Vault dev server on port $VAULT_PORT"
	podman run -d --name "$VAULT_CONTAINER" \
		--network=host \
		--cap-add=IPC_LOCK \
		-e VAULT_DEV_ROOT_TOKEN_ID="$VAULT_ROOT_TOKEN" \
		-e VAULT_DEV_LISTEN_ADDRESS="0.0.0.0:${VAULT_PORT}" \
		"$VAULT_IMAGE"

	echo -n "==> waiting for Vault to start"
	for i in $(seq 1 30); do
		if curl -sf "${VAULT_ADDR}/v1/sys/health" >/dev/null 2>&1; then
			echo " ready"
			break
		fi
		echo -n "."
		sleep 2
	done

	if ! curl -sf "${VAULT_ADDR}/v1/sys/health" >/dev/null 2>&1; then
		echo ""
		echo "error: Vault did not become healthy" >&2
		podman logs "$VAULT_CONTAINER" 2>&1 | tail -20
		exit 1
	fi
fi

# --- enable transit secret engine ---

echo "==> enabling transit secret engine"
curl -sf -X POST "${VAULT_ADDR}/v1/sys/mounts/transit" \
	-H "X-Vault-Token: ${VAULT_ROOT_TOKEN}" \
	-H "Content-Type: application/json" \
	-d '{"type":"transit"}' >/dev/null 2>&1 || true

# --- create transit keys ---

for key in testkey-1 testkey-2; do
	echo "    creating transit key: $key"
	curl -sf -X POST "${VAULT_ADDR}/v1/transit/keys/${key}" \
		-H "X-Vault-Token: ${VAULT_ROOT_TOKEN}" \
		-H "Content-Type: application/json" \
		-d '{}' >/dev/null
done

# --- write token file ---
# rgw_kms.cc rejects token files with group-write or other-perms

echo -n "$VAULT_ROOT_TOKEN" > "$VAULT_TOKEN_FILE"
chmod 0600 "$VAULT_TOKEN_FILE"
echo "==> token written to $VAULT_TOKEN_FILE"

echo ""
echo "==> Vault ready on port ${VAULT_PORT}"
echo "    transit keys: testkey-1, testkey-2"
echo ""
echo "To stop: $0 --stop"
