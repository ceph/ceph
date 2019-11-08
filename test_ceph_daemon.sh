#!/bin/bash -ex

fsid='00000000-0000-0000-0000-0000deadbeef'
image='ceph/daemon-base:latest-master'
[ -z "$ip" ] && ip=127.0.0.1

CEPH_DAEMON=../src/ceph-daemon/ceph-daemon

#A="-d"

$CEPH_DAEMON $A rm-cluster --fsid $fsid --force

cat <<EOF > c
[global]
log to file = true
EOF

$CEPH_DAEMON $A \
    --image $image \
    bootstrap \
    --mon-id a \
    --mgr-id x \
    --fsid $fsid \
    --mon-ip $ip \
    --config c \
    --output-keyring k \
    --output-config c
chmod 644 k c

if [ -n "$ip2" ]; then
    # mon.b
    $CEPH_DAEMON $A \
    --image $image \
    deploy --name mon.b \
    --fsid $fsid \
    --mon-addrv "[v2:$ip2:3300,v1:$ip2:6789]" \
    --keyring /var/lib/ceph/$fsid/mon.a/keyring \
    --config c
fi

# mgr.b
bin/ceph -c c -k k auth get-or-create mgr.y \
	 mon 'allow profile mgr' \
	 osd 'allow *' \
	 mds 'allow *' > k-mgr.y
$CEPH_DAEMON $A \
    --image $image \
    deploy --name mgr.y \
    --fsid $fsid \
    --keyring k-mgr.y \
    --config c

# mds.{k,j}
for id in k j; do
    bin/ceph -c c -k k auth get-or-create mds.$id \
	     mon 'allow profile mds' \
	     mgr 'allow profile mds' \
	     osd 'allow *' \
	     mds 'allow *' > k-mds.$id
    $CEPH_DAEMON $A \
	--image $image \
	deploy --name mds.$id \
	--fsid $fsid \
	--keyring k-mds.$id \
	--config c
done

bin/ceph -c c -k k -s
