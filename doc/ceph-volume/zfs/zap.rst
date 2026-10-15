.. _ceph-volume-zfs-zap:

``zap``
=======

The ``zap`` subcommand wipes a disk that has been used for a Ceph OSD so that
it can be reused. By default the command is a **dry run** — it prints what
would be done without touching anything. Pass ``--force`` to actually execute.

Dry run (safe, prints commands only)::

    ceph-volume zfs zap /dev/ada1

Actually wipe the disk::

    ceph-volume zfs zap --force /dev/ada1

Multiple devices can be zapped at once::

    ceph-volume zfs zap --force /dev/ada1 /dev/ada2

What zap does
-------------

Without ``--destroy``, zap removes the GPT partition table and any ZFS labels
from the disk, leaving it in a pristine state with no partition scheme. The
sequence per disk is:

#. Refuse if any partition is mounted or is an active swap device.
#. Refuse if the disk is a member of a currently-imported ZFS pool (unless
   ``--destroy`` is used for Ceph-managed pools).
#. Delete each partition by index using ``gpart delete``.
#. Destroy the partition scheme with ``gpart destroy -F``.
#. Clear ZFS labels with ``zpool labelclear -f``.

Destroying Ceph-managed pools
------------------------------

To also destroy any ``ceph-volume-zfs``-managed ZFS pools on the disk, use
``--destroy``::

    ceph-volume zfs zap --force --destroy /dev/ada1

``--destroy`` only removes pools tagged with
``ceph:managed_by = ceph-volume-zfs``. Pools not created by
``ceph-volume zfs prepare`` are never touched, even with ``--destroy``.

Safety
------

The following conditions always cause zap to refuse, with no override:

* A partition on the disk is currently mounted.
* A partition is configured as swap (active or in ``/etc/fstab``).
* The disk is a member of an imported ZFS pool that is not Ceph-managed
  (without ``--destroy``).

Options
-------

``--force``
    Actually execute the zap. Without this flag, only print the commands
    that would be run.

``--destroy``
    Before zapping, export and destroy any ZFS pools on the disk that are
    tagged with ``ceph:managed_by = ceph-volume-zfs``.
