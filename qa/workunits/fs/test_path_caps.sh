#!/bin/sh -ex

mkdir -p mnt.admin mnt.foo

./ceph-fuse -n client.admin mnt.admin
mkdir -p mnt.admin/foo/p mnt.admin/bar/q
touch mnt.admin/foo/r mnt.admin/bar/s

./ceph auth get-or-create client.foo mon 'allow r' mds 'allow r, allow rw path=/foo' osd 'allow rwx' >> keyring
./ceph-fuse -n client.foo mnt.foo

cleanup()
{
	rm -rf mnt.admin/foo mnt.admin/bar
	fusermount -u mnt.foo
	fusermount -u mnt.admin
	rm -rf mnt.admin mnt.foo
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
ln -s symlink mnt.foo/foo/x
ln -s symlink mnt.foo/foo/y
unlink mnt.foo/foo/r
rmdir mnt.foo/foo/p

# everything else is not
expect_false mkdir mnt.foo/bar/x
expect_false mkdir mnt.foo/food
expect_false ln -s symlink mnt.foo/x
expect_false ln -s symlink mnt.foo/food
expect_false unlink mnt.foo/bar/s
expect_false rmdir mnt.foo/bar/q

echo PASS
