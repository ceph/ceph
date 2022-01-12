#!/usr/bin/bash
set -ex
echo "$SSH_PUBKEY" > /root/.ssh/authorized_keys
echo "$SSH_PUBKEY" > /home/ubuntu/.ssh/authorized_keys
chown ubuntu /home/ubuntu/.ssh/authorized_keys
curl -X POST -d "{\"name\": \"$(hostname)\", \"machine_type\": \"testnode\", \"up\": true, \"locked\": false}" http://paddles:8080/nodes/
exec /usr/sbin/sshd -D
