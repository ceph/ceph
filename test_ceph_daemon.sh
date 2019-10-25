#!/bin/bash -ex

fsid=2a833e3f-53e4-49a7-a7a0-bd89d193ab62
image=ceph/daemon-base:latest-master-devel
[ -z "$ip" ] && ip=127.0.0.1

#A="-d"

../src/ceph-daemon $A rm-cluster --fsid $fsid --force

cat <<EOF > c
[global]
log to file = true
EOF

../src/ceph-daemon $A \
    --image $image \
    bootstrap \
    --mon-id a \
    --mgr-id x \
    --fsid $fsid \
    --mon-ip $ip \
    --config c \
    --output-keyring k \
    --output-config c \
    --skip-ssh
chmod 644 k c

if [ -n "$ip2" ]; then
    # mon.b
    ../src/ceph-daemon $A \
    --image $image \
    deploy --name mon.b \
    --fsid $fsid \
    --mon-ip $ip2 \
    --keyring /var/lib/ceph/$fsid/mon.a/keyring \
    --config c
fi

# mgr.b
bin/ceph -c c -k k auth get-or-create mgr.y \
	 mon 'allow profile mgr' \
	 osd 'allow *' \
	 mds 'allow *' > k-mgr.y
../src/ceph-daemon $A \
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
    ../src/ceph-daemon $A \
	--image $image \
	deploy --name mds.$id \
	--fsid $fsid \
	--keyring k-mds.$id \
	--config c
done

bin/ceph -c c -k k -s
