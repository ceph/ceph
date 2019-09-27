#!/bin/bash -ex

fsid=0a464092-dfd0-11e9-b903-002590e526e8

../src/ceph-daemon rm-cluster --fsid $fsid --force

../src/ceph-daemon bootstrap \
		   --fsid $fsid \
		   --mon-ip 10.3.64.23 \
		   --output-keyring k \
		   --output-conf c
chmod 644 k c

bin/ceph -c c -k k -s
