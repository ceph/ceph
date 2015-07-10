#!/bin/sh -ex

mkdir mnt.admin mnt.foo

./ceph-fuse -n client.admin mnt.admin
mkdir mnt.admin/foo mnt.admin/bar

./ceph auth get-or-create client.foo mon 'allow r' mds 'allow r, allow rw path=/foo' osd 'allow rwx' >> keyring
./ceph-fuse -n client.foo mnt.foo

cleanup()
{
	rm -rf mnt.admin/foo mnt.admin/bar
	fusermount -u mnt.foo
	fusermount -u mnt.admin
	rmdir mnt.admin mnt.foo
}

trap cleanup INT TERM EXIT

expect_false()
{
	if "$@"; then return 1;
	else return 0;
	fi
}

# things in /foo/ are allowed
mkdir mnt.foo/foo/x
mkdir mnt.foo/foo/y

# everything else is not
expect_false mkdir mnt.foo/bar/x
expect_false mkdir mnt.foo/food