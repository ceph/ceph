#!/usr/bin/env bash

set -ex

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
: ${CEPH_ROOT:=$SCRIPTPATH/../../}

sudo docker run --rm \
         -v "$CEPH_ROOT":/ceph \
         --name=promtool \
         --network=host \
         dnanexus/promtool:2.9.2 \
         test rules /ceph/monitoring/prometheus/alerts/test_alerts.yml
