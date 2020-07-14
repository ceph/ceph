.. _fs-volumes-and-subvolumes:

FS volumes and subvolumes
=========================

A  single source of truth for CephFS exports is implemented in the volumes
module of the :term:`Ceph Manager` daemon (ceph-mgr). The OpenStack shared
file system service (manila_), Ceph Container Storage Interface (CSI_),
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

    $ ceph fs volume create <vol_name> [<placement>]

This creates a CephFS file system and its data and metadata pools. It also tries
to create MDSes for the filesystem using the enabled ceph-mgr orchestrator
module  (see :doc:`/mgr/orchestrator`) , e.g., rook.

Remove a volume using::

    $ ceph fs volume rm <vol_name> [--yes-i-really-mean-it]

This removes a file system and its data and metadata pools. It also tries to
remove MDSes using the enabled ceph-mgr orchestrator module.

List volumes using::

    $ ceph fs volume ls

FS Subvolume groups
-------------------

Create a subvolume group using::

    $ ceph fs subvolumegroup create <vol_name> <group_name> [--pool_layout <data_pool_name> --uid <uid> --gid <gid> --mode <octal_mode>]

The command succeeds even if the subvolume group already exists.

When creating a subvolume group you can specify its data pool layout (see
:doc:`/cephfs/file-layouts`), uid, gid, and file mode in octal numerals. By default, the
subvolume group is created with an octal file mode '755', uid '0', gid '0' and data pool
layout of its parent directory.


Remove a subvolume group using::

    $ ceph fs subvolumegroup rm <vol_name> <group_name> [--force]

The removal of a subvolume group fails if it is not empty or non-existent.
'--force' flag allows the non-existent subvolume group remove command to succeed.


Fetch the absolute path of a subvolume group using::

    $ ceph fs subvolumegroup getpath <vol_name> <group_name>

List subvolume groups using::

    $ ceph fs subvolumegroup ls <vol_name>

Create a snapshot (see :doc:`/cephfs/experimental-features`) of a
subvolume group using::

    $ ceph fs subvolumegroup snapshot create <vol_name> <group_name> <snap_name>

This implicitly snapshots all the subvolumes under the subvolume group.

Remove a snapshot of a subvolume group using::

    $ ceph fs subvolumegroup snapshot rm <vol_name> <group_name> <snap_name> [--force]

Using the '--force' flag allows the command to succeed that would otherwise
fail if the snapshot did not exist.

List snapshots of a subvolume group using::

    $ ceph fs subvolumegroup snapshot ls <vol_name> <group_name>


FS Subvolumes
-------------

Create a subvolume using::

    $ ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes> --group_name <subvol_group_name> --pool_layout <data_pool_name> --uid <uid> --gid <gid> --mode <octal_mode> --namespace-isolated]


The command succeeds even if the subvolume already exists.

When creating a subvolume you can specify its subvolume group, data pool layout,
uid, gid, file mode in octal numerals, and size in bytes. The size of the subvolume is
specified by setting a quota on it (see :doc:`/cephfs/quota`). The subvolume can be
created in a separate RADOS namespace by specifying --namespace-isolated option. By
default a subvolume is created within the default subvolume group, and with an octal file
mode '755', uid of its subvolume group, gid of its subvolume group, data pool layout of
its parent directory and no size limit.

Remove a subvolume using::

    $ ceph fs subvolume rm <vol_name> <subvol_name> [--group_name <subvol_group_name> --force]


The command removes the subvolume and its contents. It does this in two steps.
First, it move the subvolume to a trash folder, and then asynchronously purges
its contents.

The removal of a subvolume fails if it has snapshots, or is non-existent.
'--force' flag allows the non-existent subvolume remove command to succeed.

Resize a subvolume using::

    $ ceph fs subvolume resize <vol_name> <subvol_name> <new_size> [--group_name <subvol_group_name>] [--no_shrink]

The command resizes the subvolume quota using the size specified by 'new_size'.
'--no_shrink' flag prevents the subvolume to shrink below the current used size of the subvolume.

The subvolume can be resized to an infinite size by passing 'inf' or 'infinite' as the new_size.

Fetch the absolute path of a subvolume using::

    $ ceph fs subvolume getpath <vol_name> <subvol_name> [--group_name <subvol_group_name>]

Fetch the metadata of a subvolume using::

    $ ceph fs subvolume info <vol_name> <subvol_name> [--group_name <subvol_group_name>]

The output format is json and contains fields as follows.

* atime: access time of subvolume path in the format "YYYY-MM-DD HH:MM:SS"
* mtime: modification time of subvolume path in the format "YYYY-MM-DD HH:MM:SS"
* ctime: change time of subvolume path in the format "YYYY-MM-DD HH:MM:SS"
* uid: uid of subvolume path
* gid: gid of subvolume path
* mode: mode of subvolume path
* mon_addrs: list of monitor addresses
* bytes_pcent: quota used in percentage if quota is set, else displays "undefined"
* bytes_quota: quota size in bytes if quota is set, else displays "infinite"
* bytes_used: current used size of the subvolume in bytes
* created_at: time of creation of subvolume in the format "YYYY-MM-DD HH:MM:SS"
* data_pool: data pool the subvolume belongs to
* path: absolute path of a subvolume
* type: subvolume type indicating whether it's clone or subvolume
* pool_namespace: RADOS namespace of the subvolume
* features: features supported by the subvolume

The subvolume "features" are based on the internal version of the subvolume and is a list containing
a subset of the following features,

* "snapshot-clone": supports cloning using a subvolumes snapshot as the source
* "snapshot-autoprotect": supports automatically protecting snapshots, that are active clone sources, from deletion

List subvolumes using::

    $ ceph fs subvolume ls <vol_name> [--group_name <subvol_group_name>]

Create a snapshot of a subvolume using::

    $ ceph fs subvolume snapshot create <vol_name> <subvol_name> <snap_name> [--group_name <subvol_group_name>]


Remove a snapshot of a subvolume using::

    $ ceph fs subvolume snapshot rm <vol_name> <subvol_name> <snap_name> [--group_name <subvol_group_name> --force]

Using the '--force' flag allows the command to succeed that would otherwise
fail if the snapshot did not exist.

List snapshots of a subvolume using::

    $ ceph fs subvolume snapshot ls <vol_name> <subvol_name> [--group_name <subvol_group_name>]

Fetch the metadata of a snapshot using::

    $ ceph fs subvolume snapshot info <vol_name> <subvol_name> <snap_name> [--group_name <subvol_group_name>]

The output format is json and contains fields as follows.

* created_at: time of creation of snapshot in the format "YYYY-MM-DD HH:MM:SS:ffffff"
* data_pool: data pool the snapshot belongs to
* has_pending_clones: "yes" if snapshot clone is in progress otherwise "no"
* size: snapshot size in bytes

Cloning Snapshots
-----------------

Subvolumes can be created by cloning subvolume snapshots. Cloning is an asynchronous operation involving copying
data from a snapshot to a subvolume. Due to this bulk copy nature, cloning is currently inefficient for very huge
data sets.

.. note:: Removing a snapshot (source subvolume) would fail if there are pending or in progress clone operations.

Protecting snapshots prior to cloning was a pre-requisite in the Nautilus release, and the commands to protect/unprotect
snapshots were introduced for this purpose. This pre-requisite, and hence the commands to protect/unprotect, is being
deprecated in mainline CephFS, and may be removed from a future release.

The commands being deprecated are:
  $ ceph fs subvolume snapshot protect <vol_name> <subvol_name> <snap_name> [--group_name <subvol_group_name>]
  $ ceph fs subvolume snapshot unprotect <vol_name> <subvol_name> <snap_name> [--group_name <subvol_group_name>]

.. note:: Using the above commands would not result in an error, but they serve no useful function.

.. note:: Use subvolume info command to fetch subvolume metadata regarding supported "features" to help decide if protect/unprotect of snapshots is required, based on the "snapshot-autoprotect" feature availability.

To initiate a clone operation use::

  $ ceph fs subvolume snapshot clone <vol_name> <subvol_name> <snap_name> <target_subvol_name>

If a snapshot (source subvolume) is a part of non-default group, the group name needs to be specified as per::

  $ ceph fs subvolume snapshot clone <vol_name> <subvol_name> <snap_name> <target_subvol_name> --group_name <subvol_group_name>

Cloned subvolumes can be a part of a different group than the source snapshot (by default, cloned subvolumes are created in default group). To clone to a particular group use::

  $ ceph fs subvolume snapshot clone <vol_name> <subvol_name> <snap_name> <target_subvol_name> --target_group_name <subvol_group_name>

Similar to specifying a pool layout when creating a subvolume, pool layout can be specified when creating a cloned subvolume. To create a cloned subvolume with a specific pool layout use::

  $ ceph fs subvolume snapshot clone <vol_name> <subvol_name> <snap_name> <target_subvol_name> --pool_layout <pool_layout>

To check the status of a clone operation use::

  $ ceph fs clone status <vol_name> <clone_name> [--group_name <group_name>]

A clone can be in one of the following states:

#. `pending`     : Clone operation has not started
#. `in-progress` : Clone operation is in progress
#. `complete`    : Clone operation has successfully finished
#. `failed`      : Clone operation has failed

Sample output from an `in-progress` clone operation::

  $ ceph fs subvolume snapshot clone cephfs subvol1 snap1 clone1
  $ ceph fs clone status cephfs clone1
  {
    "status": {
      "state": "in-progress",
      "source": {
        "volume": "cephfs",
        "subvolume": "subvol1",
        "snapshot": "snap1"
      }
    }
  }

(NOTE: since `subvol1` is in default group, `source` section in `clone status` does not include group name)

.. note:: Cloned subvolumes are accessible only after the clone operation has successfully completed.

For a successful clone operation, `clone status` would look like so::

  $ ceph fs clone status cephfs clone1
  {
    "status": {
      "state": "complete"
    }
  }

or `failed` state when clone is unsuccessful.

On failure of a clone operation, the partial clone needs to be deleted and the clone operation needs to be retriggered.
To delete a partial clone use::

  $ ceph fs subvolume rm <vol_name> <clone_name> [--group_name <group_name>] --force

.. note:: Cloning only synchronizes directories, regular files and symbolic links. Also, inode timestamps (access and
          modification times) are synchronized upto seconds granularity.

An `in-progress` or a `pending` clone operation can be canceled. To cancel a clone operation use the `clone cancel` command::

  $ ceph fs clone cancel <vol_name> <clone_name> [--group_name <group_name>]

On successful cancelation, the cloned subvolume is moved to `canceled` state::

  $ ceph fs subvolume snapshot clone cephfs subvol1 snap1 clone1
  $ ceph fs clone cancel cephfs clone1
  $ ceph fs clone status cephfs clone1
  {
    "status": {
      "state": "canceled",
      "source": {
        "volume": "cephfs",
        "subvolume": "subvol1",
        "snapshot": "snap1"
      }
    }
  }

.. note:: The canceled cloned can be deleted by using --force option in `fs subvolume rm` command.

.. _manila: https://github.com/openstack/manila
.. _CSI: https://github.com/ceph/ceph-csi
