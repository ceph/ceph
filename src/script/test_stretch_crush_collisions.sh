#!/bin/bash
# Test script to detect CRUSH retry exhaustion in stretch mode configurations
# Tests whether unbiased stretch rules with exactly 2 datacenters experience
# collision retry exhaustion (hitting the 50-try limit)

set -e

# Find the script directory and repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default to build directory at repo root, but allow override
BUILD_DIR="${BUILD_DIR:-$REPO_ROOT/build}"

# Find crushtool
if [ -n "$CRUSHTOOL" ]; then
    # User specified CRUSHTOOL, use it
    :
elif [ -x "$BUILD_DIR/bin/crushtool" ]; then
    CRUSHTOOL="$BUILD_DIR/bin/crushtool"
else
    echo "Error: Cannot find crushtool. Please set BUILD_DIR or CRUSHTOOL environment variable."
    exit 1
fi

OUTPUT_DIR="${OUTPUT_DIR:-./crush_test_results}"
NUM_PGS="${NUM_PGS:-100000}"

mkdir -p "$OUTPUT_DIR"

# Function to create a CRUSH map with 2 datacenters
create_crush_map() {
    local filename=$1

    cat > "$filename" <<'EOF'
# CRUSH map for stretch mode with 2 datacenters
# Matches real cluster structure

# devices
device 0 osd.0
device 1 osd.1
device 2 osd.2
device 3 osd.3
device 4 osd.4
device 5 osd.5
device 6 osd.6
device 7 osd.7

# types
type 0 osd
type 1 host
type 2 datacenter
type 3 root

# buckets
host host1 {
    id -9
    alg straw2
    hash 0
    item osd.0 weight 1.0
    item osd.1 weight 1.0
}

host host2 {
    id -10
    alg straw2
    hash 0
    item osd.2 weight 1.0
    item osd.3 weight 1.0
}

host host3 {
    id -11
    alg straw2
    hash 0
    item osd.4 weight 1.0
    item osd.5 weight 1.0
}

host host4 {
    id -12
    alg straw2
    hash 0
    item osd.6 weight 1.0
    item osd.7 weight 1.0
}

datacenter dc1 {
    id -5
    alg straw2
    hash 0
    item host1 weight 2.0
    item host2 weight 2.0
}

datacenter dc2 {
    id -7
    alg straw2
    hash 0
    item host3 weight 2.0
    item host4 weight 2.0
}

root default {
    id -1
    alg straw2
    hash 0
    item dc1 weight 4.0
    item dc2 weight 4.0
}

# CRUSH rules
rule stretch_replicated_rule {
    id 0
    type replicated
    step take default
    step choose firstn 0 type datacenter
    step chooseleaf firstn 2 type host
    step emit
}
EOF
}

# Function to test a CRUSH map
test_crush_map() {
    local map_txt=$1
    local map_bin=$2
    local rule_id=$3
    local rule_name=$4
    local iteration=$5
    
    if $CRUSHTOOL -i "$map_bin" --test \
        --min-x 1 \
        --max-x "$NUM_PGS" \
        --rule "$rule_id" \
        --num-rep 4 \
        --show-statistics \
        --set-choose-total-tries 50 \
        --show-retry-exhaustion 2>&1 | grep -q "WARNING: Retry exhaustion detected!"; then
        return 1  # Retry exhaustion detected - failure
    else
        return 0  # No retry exhaustion - success
    fi
}

echo "Testing Unbiased Rule with 2 Datacenters"
echo "Running 100 iterations with $NUM_PGS PGs each..."
echo ""

create_crush_map "$OUTPUT_DIR/crush_2dc.txt"

echo "Compiling CRUSH map..."
$CRUSHTOOL -c "$OUTPUT_DIR/crush_2dc.txt" -o "$OUTPUT_DIR/crush_2dc.bin"

for i in $(seq 1 100); do
    echo -n "  Iteration $i/100: "
    
    if test_crush_map \
        "$OUTPUT_DIR/crush_2dc.txt" \
        "$OUTPUT_DIR/crush_2dc.bin" \
        0 \
        "stretch_replicated_rule" \
        "$i"; then
        echo "OK"
    else
        echo "RETRY EXHAUSTION DETECTED"
        exit 1
    fi
done


