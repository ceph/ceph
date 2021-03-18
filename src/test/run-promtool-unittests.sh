#!/usr/bin/env bash

set -ex

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
: ${CEPH_ROOT:=$SCRIPTPATH/../../}

source /etc/os-release

case "$ID" in
fedora|centos|rhel)
    sudo dnf -y install golang-github-prometheus
esac

case "$ID" in
opensuse)
    sudo dnf -y install golang-github-prometheus-prometheus
esac

case "$ID" in
debian|ubuntu)
    sudo apt install -y prometheus
esac

promtool test rules $CEPH_ROOT/monitoring/prometheus/alerts/test_alerts.yml
