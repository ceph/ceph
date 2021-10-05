#!/usr/bin/env bash

# SSH
if [[ ! -f "/root/.ssh/id_rsa" ]]; then
	mkdir -p ~/.ssh
	chmod 700 ~/.ssh
	ssh-keygen -b 2048 -t rsa -f ~/.ssh/id_rsa -q -N ""
fi

cat ~/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
if [[ ! -f "/root/.ssh/known_hosts" ]]; then
	ssh-keygen -A
fi
dnf install -y openssh-server
/usr/sbin/sshd

# no password
echo "root:" | chpasswd
echo "PermitRootLogin yes" >> /etc/ssh/sshd_config
