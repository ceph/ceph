#!/usr/bin/env bash

set -e

function run-sshd() {
	echo "Creating sshd server on $(hostname):$(hostname -i)"
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

	# change password
	echo "root:root" | chpasswd
	echo "PermitRootLogin yes" >> /etc/ssh/sshd_config
	echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config

	/usr/sbin/sshd
	echo "sshd finished"
}

function copy-cluster-ssh-key() {
	echo "Adding cluster ssh key to all hosts: ${HOST_IPS}"
	HOST_IPS=$(echo $HOST_IPS)
	for ip in $(echo $HOST_IPS)
	do
		if [[ ! $ip == $(hostname -i) ]]
		then
			echo $ip
			# copy cluster key
			sshpass -p "root" ssh-copy-id -f -o StrictHostKeyChecking=no -i /etc/ceph/ceph.pub "root@${ip}"
		fi
	done
	echo "Finished adding keys, you can now add existing hosts containers to the cluster!"
}

case $1 in
	run-sshd)
		run-sshd
		;;
	copy-cluster-ssh-key)
		copy-cluster-ssh-key
		;;
esac
