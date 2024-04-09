#!/bin/sh -ex

cwd=`pwd`
cat > conf <<EOF
[global]
mon host = 127.0.0.1:6789

[mon]
admin socket = 
log file = $cwd/\$name.log
debug mon = 20
debug ms = 1
EOF

rm -f mm
fsid=`uuidgen`

rm -f keyring
ceph-authtool --create-keyring keyring --gen-key -n client.admin
ceph-authtool keyring --gen-key -n mon.

ceph-mon -c conf -i a --mkfs --fsid $fsid --mon-data mon.a -k keyring

ceph-mon -c conf -i a --mon-data $cwd/mon.a

ceph -c conf -k keyring health

killall ceph-mon
echo OK