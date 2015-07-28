#!/bin/sh -ex

cleanup()
{
	echo "*** Restoring to old state"
	sudo rm -rf mnt.admin/foo1 mnt.admin/foo2 mnt.admin/foo3 mnt.admin/foo4 mnt.admin/foo5 mnt.admin/foo6 mnt.admin/foo7
	fusermount -u mnt.admin
	fusermount -u mnt.foo
	rmdir mnt.admin mnt.foo
	rm keyring.foo
}
trap cleanup INT TERM EXIT

echo "*** Creating directories for mount"
mkdir -p mnt.admin mnt.foo


echo "*** Trying mount as admin"
./ceph-fuse mnt.admin


echo "*** Trying mount as client.foo"
UID="$(id -u)"
OTH_UID=$((UID+1))
GID="$(id -g)"
GRP_AUTH_TEMPLATE='./ceph-authtool -C keyring.foo -n client.foo --cap osd "allow rw" --cap mon "allow rw" --cap mds "allow rw uid=UID gids=GID" --gen-key'
GRP_AUTH="$(echo $GRP_AUTH_TEMPLATE | sed -e 's/UID/'$UID'/g')"
GRP_AUTH="$(echo $GRP_AUTH | sed -e 's/GID/'$GID'/g')"
eval $GRP_AUTH
./ceph auth import -i keyring.foo
./ceph-fuse mnt.foo -n client.foo -k keyring.foo


echo "*** Creating directories for client.admin"
sudo mkdir -m 777 mnt.admin/foo1
sudo mkdir -m 755 mnt.admin/foo2
sudo mkdir -m 755 mnt.admin/foo3
sudo mkdir -m 755 mnt.admin/foo4
sudo mkdir -m 775 mnt.admin/foo5
sudo mkdir -m 755 mnt.admin/foo6
sudo mkdir -m 557 mnt.admin/foo7


ls -lsv mnt.admin

echo "*** Granting ownership of directories to other users"
sudo chgrp $GID mnt.admin/foo1
sudo chown 0 mnt.admin/foo1
sudo chgrp $GID mnt.admin/foo2
sudo chown 0 mnt.admin/foo2

sudo chgrp $GID mnt.admin/foo3
sudo chown $USER mnt.admin/foo3
sudo chgrp $GID mnt.admin/foo4
sudo chown $OTH_UID mnt.admin/foo4

sudo chgrp $GID mnt.admin/foo5
sudo chgrp $GID mnt.admin/foo6

sudo chgrp $GID mnt.admin/foo7

ls -lsv mnt.foo

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
mkdir mnt.foo/foo5/asdf
expect_false mkdir mnt.foo/foo6/asdf
expect_false mkdir mnt.foo/foo7/asdf
