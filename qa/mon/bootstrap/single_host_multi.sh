#!/bin/sh -ex

cwd=`pwd`
cat > conf <<EOF
[global]

[mon]
admin socket = 
log file = $cwd/\$name.log
debug mon = 20
debug ms = 1
mon host = 127.0.0.1:6789 127.0.0.1:6790 127.0.0.1:6791
EOF

rm -f mm
fsid=`uuidgen`

rm -f keyring
ceph-authtool --create-keyring keyring --gen-key -n client.admin
ceph-authtool keyring --gen-key -n mon.

ceph-mon -c conf -i a --mkfs --fsid $fsid --mon-data mon.a -k keyring --public-addr 127.0.0.1:6789
ceph-mon -c conf -i b --mkfs --fsid $fsid --mon-data mon.b -k keyring --public-addr 127.0.0.1:6790
ceph-mon -c conf -i c --mkfs --fsid $fsid --mon-data mon.c -k keyring --public-addr 127.0.0.1:6791

ceph-mon -c conf -i a --mon-data mon.a
ceph-mon -c conf -i b --mon-data mon.b
ceph-mon -c conf -i c --mon-data mon.c

ceph -c conf -k keyring health -m 127.0.0.1
while true; do
    if ceph -c conf -k keyring -m 127.0.0.1 mon stat | grep 'quorum 0,1,2'; then
	break
    fi
    sleep 1
done

killall ceph-mon
echo OK