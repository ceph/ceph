#!/bin/bash

. $(dirname $0)/../../standalone/ceph-helpers.sh

set -x

function test_dump_metrics() {
    # Ensure can get dump_metrics full
    ceph tell osd.0 dump_metrics 2>/dev/null > /tmp/dump_metrics_full.json || true

    # Ensure can get dump_metrics reactor_utilization: can be done with each metric name individually
    ceph tell osd.0 dump_metrics reactor_utilization 2>/dev/null > /tmp/dump_metrics_reactor.json || true

    # The number of occurrences must match (must be greater than zero)
    full_count=$(grep -c reactor_utilization /tmp/dump_metrics_full.json)
    reactor_count=$(grep -c reactor_utilization /tmp/dump_metrics_reactor.json)
    
    [ $full_count -gt 0 ] || return 1
    [ $reactor_count -gt 0 ] || return 1
    [ $full_count -eq $reactor_count ] || return 1

    # Remove auxiliary files
    rm -f /tmp/dump_metrics_full.json /tmp/dump_metrics_reactor.json
    return 0 # success
}

# A more complete test: traverse the "metrics" section and ensure each metric contains the expected attributes

test_dump_metrics || { echo "test_dump_metrics failed"; exit 1; }

echo "OK"

