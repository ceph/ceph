#!/bin/sh -ex

cleanup()
{
	echo "*** Restoring to old state"
	sudo rm -rf mnt.admin/foo1 mnt.admin/foo2 mnt.admin/foo3 mnt.admin/foo4
	fusermount -u mnt.admin
	fusermount -u mnt.foo
	rmdir mnt.admin mnt.foo
}
trap cleanup INT TERM EXIT

echo "*** Creating directories for mount"
mkdir -p mnt.admin mnt.foo


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

echo "*** Creating directories for client.admin"
sudo mkdir -m 777 mnt.admin/foo1
sudo mkdir -m 700 mnt.admin/foo2
sudo mkdir -m 755 mnt.admin/foo3
sudo mkdir -m 755 mnt.admin/foo4

echo "*** Granting ownership of directories to other users"
sudo chown $USER mnt.admin/foo1
sudo chown $USER mnt.admin/foo2
sudo chown $USER mnt.admin/foo3
sudo chown $OTH_UID mnt.admin/foo4

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


