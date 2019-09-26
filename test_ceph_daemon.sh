#!/bin/bash -ex

fsid=0a464092-dfd0-11e9-b903-002590e526e8

for f in `podman ps | awk '{print $1}' | grep -v CONT ` ; do podman kill $f ; done
for f in `systemctl | grep ceph | awk '{print $1}'` ; do systemctl stop $f ; systemctl disable $f ; done

rm -rf /var/lib/ceph/$fsid/*
rm -rf /var/log/ceph/$fsid/*

../src/ceph-daemon bootstrap \
		   --fsid $fsid \
		   --mon-ip 10.3.64.23 \
		   --output-keyring k \
		   --output-conf c
chmod 644 k c

bin/ceph -c c -k k -s
