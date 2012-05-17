#!/bin/sh -ex

cwd=`pwd`
cat > conf <<EOF
[mon]
admin socket = 
log file = $cwd/\$name.log
debug mon = 20
debug ms = 1
mon initial members = a,b,d
EOF

rm -f mm
monmaptool --create mm \
    --add a 127.0.0.1:6789 \
    --add b 127.0.0.1:6790 \
    --add c 127.0.0.1:6791

rm -f keyring
ceph-authtool --create-keyring keyring --gen-key -n client.admin
ceph-authtool keyring --gen-key -n mon.

ceph-mon -c conf -i a --mkfs --monmap mm --mon-data mon.a -k keyring
ceph-mon -c conf -i b --mkfs --monmap mm --mon-data mon.b -k keyring
ceph-mon -c conf -i c --mkfs --monmap mm --mon-data mon.c -k keyring

ceph-mon -c conf -i a --mon-data mon.a
ceph-mon -c conf -i c --mon-data mon.b
ceph-mon -c conf -i b --mon-data mon.c

ceph -c conf -k keyring --monmap mm health

ceph -c conf -k keyring --monmap mm health
if ceph -c conf -k keyring --monmap mm mon stat | grep a= | grep b= | grep c= ; then
    break
fi

killall ceph-mon
echo OK
