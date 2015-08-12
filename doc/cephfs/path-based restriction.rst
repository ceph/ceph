================================
 Restrict Access to a Directory
================================

CephFS mostly assumes a controlled environment where clients are not restricted
in what paths they are allowed to mount. And if they do mount a subdirectory,
e.g., /home/user, the MDS does not currently verify that subsequent operations
are ‘locked’ within that directory. Path-based restriction allows us to restrict
a client to a particular directory in the file system.

Syntax
======

To grant rw access to the specified directory only, we mention the specified
directory while creating key for a client following the undermentioned syntax. ::

./ceph auth get-or-create client.*client_name* mon 'allow r' mds 'allow r, allow rw path=/*specified_directory*' osd 'allow rwx'

for example, to restrict client ``foo`` to ``bar`` directory, we will use. ::

./ceph auth get-or-create client.foo mon 'allow r' mds 'allow r, allow rw path=/bar' osd 'allow rwx'


To restrict a client to the specfied sub-directory only, we mention the specified
directory while mounting following the undermentioned syntax. ::

./ceph-fuse -n client.*client_name* *mount_path* -r *directory_to_be_mounted*

for example, to restrict client ``foo`` to ``mnt/bar`` directory, we will use. ::

./ceph-fuse -n client.foo mnt -r /bar
