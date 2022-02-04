#!/usr/bin/bash
set -x
hostname=$(hostname)
payload="{\"name\": \"$hostname\", \"machine_type\": \"testnode\", \"up\": false}"
for i in $(seq 1 5); do
    echo "attempt $i"
    curl -s -f -X PUT -d "$payload" http://paddles:8080/nodes/$hostname/ && break
    sleep 1
done
pkill sshd