#!/bin/bash -ex

fsid=0a464092-dfd0-11e9-b903-002590e526e8

../src/ceph-daemon rm-cluster --fsid $fsid --force

cat <<EOF > c
[global]
log to file = true
EOF

../src/ceph-daemon bootstrap \
		   --mon-id a \
		   --mgr-id x \
		   --fsid $fsid \
		   --mon-ip 10.3.64.23 \
		   --config c \
		   --output-keyring k \
		   --output-config c
chmod 644 k c

# mon.b
../src/ceph-daemon deploy --name mon.b \
		   --fsid $fsid \
		   --mon-ip 10.3.64.27 \
		   --keyring /var/lib/ceph/$fsid/mon.a/keyring \
		   --config c

# mgr.b
bin/ceph -c c -k k auth get-or-create mgr.y \
	 mon 'allow profile mgr' \
	 osd 'allow *' \
	 mds 'allow *' > k-mgr.y
../src/ceph-daemon deploy --name mgr.y \
		   --fsid $fsid \
		   --keyring k-mgr.y \
		   --config c


bin/ceph -c c -k k -s
