#!/bin/bash -ex

# Cross-namespace copy test using the NVMe Simple Copy command (TP 4065).
#
# The test connects to the NVMe-oF gateway, discovers a second NVMe device
# (sorted by namespace index), then issues an `nvme copy` command that copies
# one LBA range from a source namespace into that device at the destination
# LBA offset.
#
# Expected outcome: the nvme-cli reports "NVMe Copy: success".

source /etc/ceph/nvmeof.env

SPDK_CONTROLLER="Ceph bdev Controller"
DISCOVERY_PORT="8009"

# Ensure we are connected to all subsystems so that nvme list sees all devices.
# basic_tests.sh (which runs before this script on client.1) leaves a
# connect-all session open, but namespace_test.sh may have changed device
# counts.  Re-running connect-all is idempotent.
sudo nvme connect-all --traddr="$NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS" --transport=tcp -l 3600
sleep 5

# Pick the second SPDK device sorted by namespace number.
# nvmeof_namespaces.yaml connects all namespaces so there are many devices
# available; we just need any valid second one for the copy destination.
target_device=$(sudo nvme list --output-format=json |
    jq -r '[.Devices[] | select(.ModelNumber == "'"$SPDK_CONTROLLER"'")] | sort_by(.NameSpace) | .[1] | .DevicePath')

if [ -z "$target_device" ] || [ "$target_device" = "null" ]; then
    echo "[nvmeof.copy] ERROR: could not find a second NVMe device to use as copy destination"
    sudo nvme list
    exit 1
fi

echo "[nvmeof.copy] Using target device: $target_device"

copy_test() {
    # nvme-cli flag names differ by version:
    #   2.13: --slba, --nlb, --snsid  (singular)
    #   2.16+: --slbs, --blocks, --snsids  (plural, comma-separated list)
    # Detect by checking which flag the installed binary accepts.
    local nvme_ver
    nvme_ver=$(nvme version 2>/dev/null | head -n1 | awk '{print $3}')
    local major minor
    major=$(echo "$nvme_ver" | cut -d. -f1)
    minor=$(echo "$nvme_ver" | cut -d. -f2)

    if [ "$major" -gt 2 ] || { [ "$major" -eq 2 ] && [ "$minor" -ge 16 ]; }; then
        # nvme-cli >= 2.16: plural list flags
        # Format 2 is required for cross-namespace copy (NVMe TP4065);
        # formats 0 and 1 are same-namespace only and nvme-cli 2.16 enforces this.
        output=$(sudo nvme copy "$target_device" \
            --sdlba=1000 \
            --slbs=5000 \
            --blocks=99 \
            --snsids=1 \
            --format=2 2>&1)
    else
        # nvme-cli < 2.16 (e.g. 2.13): singular flags
        output=$(sudo nvme copy "$target_device" \
            --sdlba=1000 \
            --slba=5000 \
            --nlb=99 \
            --snsid=1 \
            --format=2 2>&1)
    fi
    echo "$output"
    if ! echo "$output" | grep -q "NVMe Copy: success"; then
        echo "[nvmeof.copy] copy_test FAILED — expected 'NVMe Copy: success' in output"
        sudo dmesg -T > "$TESTDIR/archive/dmesg-copy_test.log" 2>/dev/null || true
        return 1
    fi
}

echo "[nvmeof.copy] Running NVMe copy test..."
copy_test
echo "[nvmeof.copy] NVMe copy test passed!"
