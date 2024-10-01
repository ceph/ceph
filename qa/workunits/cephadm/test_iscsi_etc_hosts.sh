#!/bin/bash

# checks if the container and host's /etc/hosts files match
# Necessary to avoid potential bugs caused by podman making
# edits to /etc/hosts file in the container
# exits with code 1 if host and iscsi container /etc/hosts do no match

set -ex

ISCSI_DAEMON=$(sudo /home/ubuntu/cephtest/cephadm ls | jq -r '.[] | select(.service_name == "iscsi.foo") | .name')
sudo /home/ubuntu/cephtest/cephadm enter --name $ISCSI_DAEMON -- cat /etc/hosts > iscsi_daemon_etc_hosts.txt
if cmp --silent /etc/hosts iscsi_daemon_etc_hosts.txt; then
  echo "Daemon and host /etc/hosts files successfully matched"
else
  echo "ERROR: /etc/hosts on host did not match /etc/hosts in the iscsi container!"
  echo "Host /etc/hosts:"
  cat /etc/hosts
  echo "Iscsi container /etc/hosts:"
  cat iscsi_daemon_etc_hosts.txt
  exit 1
fi
