#!/usr/bin/bash
set -x
echo "$SSH_PUBKEY" > /root/.ssh/authorized_keys
echo "$SSH_PUBKEY" > /home/ubuntu/.ssh/authorized_keys
chown ubuntu /home/ubuntu/.ssh/authorized_keys
payload="{\"name\": \"$(hostname)\", \"machine_type\": \"testnode\", \"up\": true, \"locked\": false, \"os_type\": \"ubuntu\", \"os_version\": \"20.04\"}"
for i in $(seq 1 5); do
    echo "attempt $i"
    curl -v -f -d "$payload" http://paddles:8080/nodes/ && break
    sleep 1
done
mkdir -p /run/sshd
exec /usr/sbin/sshd -D
