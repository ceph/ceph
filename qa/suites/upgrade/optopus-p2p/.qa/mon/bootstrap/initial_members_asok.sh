#!/bin/sh -ex

cwd=`pwd`
cat > conf <<EOF
[mon]
log file = $cwd/\$name.log
debug mon = 20
debug ms = 1
debug asok = 20
mon initial members = a,b,d
admin socket = $cwd/\$name.asok
EOF

rm -f mm
fsid=`uuidgen`

rm -f keyring
ceph-authtool --create-keyring keyring --gen-key -n client.admin
ceph-authtool keyring --gen-key -n mon.

ceph-mon -c conf -i a --mkfs --fsid $fsid --mon-data $cwd/mon.a -k keyring
ceph-mon -c conf -i b --mkfs --fsid $fsid --mon-data $cwd/mon.b -k keyring
ceph-mon -c conf -i c --mkfs --fsid $fsid --mon-data $cwd/mon.c -k keyring

ceph-mon -c conf -i a --mon-data $cwd/mon.a --public-addr 127.0.0.1:6789
ceph-mon -c conf -i b --mon-data $cwd/mon.c --public-addr 127.0.0.1:6790
ceph-mon -c conf -i c --mon-data $cwd/mon.b --public-addr 127.0.0.1:6791

sleep 1

if timeout 5 ceph -c conf -k keyring -m localhost mon stat | grep "a,b,c" ; then
    echo WTF
    exit 1
fi

ceph --admin-daemon mon.a.asok add_bootstrap_peer_hint 127.0.0.1:6790

while true; do
    if ceph -c conf -k keyring -m 127.0.0.1 mon stat | grep 'a,b'; then
	break
    fi
    sleep 1
done

ceph --admin-daemon mon.c.asok add_bootstrap_peer_hint 127.0.0.1:6790

while true; do
    if ceph -c conf -k keyring -m 127.0.0.1 mon stat | grep 'a,b,c'; then
	break
    fi
    sleep 1
done

ceph-mon -c conf -i d --mkfs --fsid $fsid --mon-data $cwd/mon.d -k keyring
ceph-mon -c conf -i d --mon-data $cwd/mon.d --public-addr 127.0.0.1:6792
ceph --admin-daemon mon.d.asok add_bootstrap_peer_hint 127.0.0.1:6790

while true; do
    if ceph -c conf -k keyring -m 127.0.0.1 mon stat | grep 'a,b,c,d'; then
	break
    fi
    sleep 1
done

killall ceph-mon
echo OK
