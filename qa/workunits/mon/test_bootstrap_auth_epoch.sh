#!/usr/bin/env bash
set -ex

# Setup temporary test environment
TEST_DIR=$(mktemp -d)
trap "rm -rf $TEST_DIR" EXIT

export CEPH_ARGS="-c $TEST_DIR/ceph.conf"

# 1. Create a minimal ceph.conf mimicking a Rook configuration style
cat <<EOF > "$TEST_DIR/ceph.conf"
[global]
fsid = $(uuidgen)
mon_initial_members = a
mon_host = 127.0.0.1:7139
auth_cluster_required = cephx
auth_service_required = cephx
auth_client_required = cephx
ms_bind_ipv4 = true
ms_bind_ipv6 = false

# Sandbox directory overrides to prevent running into root privilege blocks
run_dir = $TEST_DIR
admin_socket = $TEST_DIR/\$cluster-\$name.asok
pid_file = $TEST_DIR/\$cluster-\$name.pid
EOF

# Create monitor keyring
ceph-authtool --create-keyring "$TEST_DIR/keyring" --key-type aes256k --gen-key -n mon. --cap mon 'allow *'
ceph-authtool "$TEST_DIR/keyring" --key-type aes256k --gen-key -n client.admin --cap mon 'allow *' --cap osd 'allow *' --cap mgr 'allow *'

# 2. Bootstrap the monitor WITHOUT a monmap file (--mon-host config branch)
ceph-mon --mkfs -i a --mon-data "$TEST_DIR/mon.a" --keyring "$TEST_DIR/keyring"

export CEPH_KEYRING="$TEST_DIR/keyring"

# 3. Start the monitor daemon
ceph-mon -f -i a --mon-data "$TEST_DIR/mon.a" --public-addr 127.0.0.1:7139 &

# Wait for monitor to become responsive
for i in {1..10}; do
    if ceph mon dump; then
        break
    fi
    sleep 1
done

# 4. Trigger the auth_epoch bump
# BEFORE THE PATCH: This command will crash the monitor due to the UINT32_MAX assertion
# AFTER THE PATCH: This command will complete successfully and fix the epoch
ceph auth wipe-rotating-service-keys

# Clean up daemon
kill %1 || true
echo "SUCCESS: Cluster successfully advanced past the initial bootstrap auth_epoch!"
