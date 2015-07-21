#!/bin/sh -ex

cleanup()
{
	echo "*** Restoring to old state"
	sudo rm -rf mnt.admin/foo1 mnt.admin/foo2 mnt.admin/foo3 mnt.admin/foo4 mnt.admin/foo5 mnt.admin/foo6
	fusermount -u mnt.admin
	fusermount -u mnt.foo
	fusermount -u mnt.grp
	rmdir mnt.admin mnt.foo mnt.grp
	rm keyring.foo keyring.grp
}
trap cleanup INT TERM EXIT

echo "*** Creating directories for mount"
mkdir -p mnt.admin mnt.foo mnt.grp


echo "*** Trying mount as admin"
./ceph-fuse mnt.admin


echo "*** Trying mount as client.foo"
UID="$(id -u)"
OTH_UID=$((UID+1))
AUTH_TEMPLATE='./ceph-authtool -C keyring.foo -n client.foo --cap osd "allow rw" --cap mon "allow rw" --cap mds "allow rw uid=UID" --gen-key'
AUTH="$(echo $AUTH_TEMPLATE | sed -e 's/UID/'$UID'/g')"
eval $AUTH
./ceph auth import -i keyring.foo
./ceph-fuse mnt.foo -n client.foo -k keyring.foo


echo "*** Trying mount as client.grp"
GID="$(id -g)"
GRP_AUTH_TEMPLATE='./ceph-authtool -C keyring.grp -n client.grp --cap osd "allow rw" --cap mon "allow rw" --cap mds "allow rw uid=UID gids=GID" --gen-key'
GRP_AUTH="$(echo $GRP_AUTH_TEMPLATE | sed -e 's/UID/'$UID'/g')"
GRP_AUTH="$(echo $GRP_AUTH | sed -e 's/GID/'$GID'/g')"
eval $GRP_AUTH
./ceph auth import -i keyring.grp
./ceph-fuse mnt.grp -n client.grp -k keyring.grp


echo "*** Creating directories for client.admin"
sudo mkdir -m 777 mnt.admin/foo1
sudo mkdir -m 700 mnt.admin/foo2
sudo mkdir -m 755 mnt.admin/foo3
sudo mkdir -m 755 mnt.admin/foo4
sudo mkdir -m 775 mnt.admin/foo5
sudo mkdir -m 755 mnt.admin/foo6

echo "*** Granting ownership of directories to other users"
sudo chown $USER mnt.admin/foo1
sudo chown $USER mnt.admin/foo2
sudo chown $USER mnt.admin/foo3
sudo chown $OTH_UID mnt.admin/foo4
sudo chgrp $GID mnt.admin/foo5
sudo chgrp $GID mnt.admin/foo6

echo "*** Testing auth checks"
expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}
mkdir mnt.foo/foo1/asdf
expect_false mkdir mnt.foo/foo2/asdf
mkdir mnt.foo/foo3/asdf
expect_false mkdir mnt.foo/foo4/asdf
mkdir mnt.grp/foo5/asdf
expect_false mkdir mnt.grp/foo6/asdf


