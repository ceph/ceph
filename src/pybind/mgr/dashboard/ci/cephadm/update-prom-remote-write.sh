#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Error: Please provide the remote_write URL and target node as arguments."
    echo "Usage: $0 <REMOTE_WRITE_URL> <TARGET_NODE>"
    echo "Example: $0 http://192.168.0.233:9190 ceph-node-00"
    exit 1
fi

REMOTE_WRITE_URL="$1"
TEMPLATE_FILE="../../../cephadm/templates/services/prometheus/prometheus.yml.j2"
TARGET_NODE="$2"

if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "Error: Cannot find $TEMPLATE_FILE in the current directory."
    exit 1
fi

if grep -q "^remote_write:" "$TEMPLATE_FILE"; then
    echo "'remote_write:' block already exists in $TEMPLATE_FILE. Skipping insertion."
    exit 1
else
    echo "adding remote_write block to $TEMPLATE_FILE..."
    
    cat << EOF >> "$TEMPLATE_FILE"

remote_write:
  - url: '$REMOTE_WRITE_URL/api/v1/write'
    tls_config:
      insecure_skip_verify: true
    write_relabel_configs:
      - source_labels: [__name__]
        regex: 'ALERTS|ALERTS_FOR_STATE'
        action: drop
EOF
    echo "updated template file"
fi

echo "redeploying Prometheus"

kcli ssh -u root "$TARGET_NODE" "cephadm shell -- ceph orch redeploy prometheus"

if [ $? -eq 0 ]; then
    echo "Prometheus redeploy command executed successfully."
else
    echo "Error: Failed to redeploy Prometheus."
    exit 1
fi
