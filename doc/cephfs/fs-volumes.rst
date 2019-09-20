.. _fs-volumes-and-subvolumes:

FS volumes and subvolumes
=========================

A  single source of truth for CephFS exports is implemented in the volumes
module of the :term:`Ceph Manager` daemon (ceph-mgr). The OpenStack shared
file system service (manila_), Ceph Containter Storage Interface (CSI_),
storage administrators among others can use the common CLI provided by the
ceph-mgr volumes module to manage the CephFS exports.

The ceph-mgr volumes module implements the following file system export
abstactions:

* FS volumes, an abstraction for CephFS file systems

* FS subvolumes, an abstraction for independent CephFS directory trees

* FS subvolume groups, an abstraction for a directory level higher than FS
  subvolumes to effect policies (e.g., :doc:`/cephfs/file-layouts`) across a
  set of subvolumes

Some possible use-cases for the export abstractions:

* FS subvolumes used as manila shares or CSI volumes

* FS subvolume groups used as manila share groups

Requirements
------------

* Nautilus (14.2.x) or a later version of Ceph

* Cephx client user (see :doc:`/rados/operations/user-management`) with
  the following minimum capabilities::

    mon 'allow r'
    mgr 'allow rw'


FS Volumes
----------

Create a volume using::

    $ ceph fs volume create <vol_name>

This creates a CephFS file sytem and its data and metadata pools. It also tries
to create MDSes for the filesytem using the enabled ceph-mgr orchestrator
module  (see :doc:`/mgr/orchestrator_cli`) , e.g., rook.

Remove a volume using::

    $ ceph fs volume rm <vol_name> [--yes-i-really-mean-it]

This removes a file system and its data and metadata pools. It also tries to
remove MDSes using the enabled ceph-mgr orchestrator module.

List volumes using::

    $ ceph fs volume ls

FS Subvolume groups
-------------------

Create a subvolume group using::

    $ ceph fs subvolumegroup create <vol_name> <group_name> [--mode <octal_mode> --pool_layout <data_pool_name>]

The command succeeds even if the subvolume group already exists.

When creating a subvolume group you can specify its data pool layout (see
:doc:`/cephfs/file-layouts`), and file mode in octal numerals. By default, the
subvolume group is created with an octal file mode '755', and data pool layout
of its parent directory.


Remove a subvolume group using::

    $ ceph fs subvolumegroup rm <vol_name> <group_name> [--force]

The removal of a subvolume group fails if it is not empty, e.g., has subvolumes
or snapshots, or is non-existent. Using the '--force' flag allows the command
to succeed even if the subvolume group is non-existent.


Fetch the absolute path of a subvolume group using::

    $ ceph fs subvolumegroup getpath <vol_name> <group_name>

Create a snapshot (see :doc:`/cephfs/experimental-features`) of a
subvolume group using::

    $ ceph fs subvolumegroup snapshot create <vol_name> <group_name> <snap_name>

This implicitly snapshots all the subvolumes under the subvolume group.

Remove a snapshot of a subvolume group using::

    $ ceph fs subvolumegroup snapshot rm <vol_name> <group_name> <snap_name> [--force]

Using the '--force' flag allows the command to succeed that would otherwise
fail if the snapshot did not exist.


FS Subvolumes
-------------

Create a subvolume using::

    $ ceph fs subvolume create <vol_name> <subvol_name> [--group_name <subvol_group_name> --mode <octal_mode> --pool_layout <data_pool_name> --size <size_in_bytes>]


The command succeeds even if the subvolume already exists.

When creating a subvolume you can specify its subvolume group, data pool layout,
file mode in octal numerals, and size in bytes. The size of the subvolume is
specified by setting a quota on it (see :doc:`/cephfs/quota`). By default a
subvolume is created within the default subvolume group, and with an octal file
mode '755', data pool layout of its parent directory and no size limit.


Remove a subvolume group using::

    $ ceph fs subvolume rm <vol_name> <subvol_name> [--group_name <subvol_group_name> --force]


The command removes the subvolume and its contents. It does this in two steps.
First, it move the subvolume to a trash folder, and then asynchronously purges
its contents.

The removal of a subvolume fails if it has snapshots, or is non-existent.
Using the '--force' flag allows the command to succeed even if the subvolume is
non-existent.


Fetch the absolute path of a subvolume using::

    $ ceph fs subvolume getpath <vol_name> <subvol_name> [--group_name <subvol_group_name>]


Create a snapshot of a subvolume using::

    $ ceph fs subvolume snapshot create <vol_name> <subvol_name> <snap_name> [--group_name <subvol_group_name>]


Remove a snapshot of a subvolume using::

    $ ceph fs subvolume snapshot rm <vol_name> <subvol_name> <snap_name> [--group_name <subvol_group_name> --force]

Using the '--force' flag allows the command to succeed that would otherwise
fail if the snapshot did not exist.

.. _manila: https://github.com/openstack/manila
.. _CSI: https://github.com/ceph/ceph-csi
