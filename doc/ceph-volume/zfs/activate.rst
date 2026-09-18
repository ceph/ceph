.. _ceph-volume-zfs-activate:

``activate``
============

After :ref:`ceph-volume-zfs-prepare` has completed, the OSD can be activated.

Activation rebuilds the OSD data directory on a tmpfs mount by reading
BlueStore's own labels from the zvol, then starts the OSD daemon. Because the
durable state lives on the zvol itself, losing the tmpfs on reboot is harmless
— activation can be re-run at any time.

Activate a specific OSD by ID::

    ceph-volume zfs activate 0

Activate all OSDs prepared by ``ceph-volume zfs``::

    ceph-volume zfs activate --all

This is the command to use in ``/etc/rc.local`` or a FreeBSD startup script to
bring up all OSDs at boot time.

Discovery
---------

Activation discovers OSDs by scanning all imported ZFS pools for the
``ceph:managed_by = ceph-volume-zfs`` property. For each matching pool it:

#. Identifies the block zvol via the ``ceph:zvol_name`` pool property.
#. Creates the OSD data directory at
   ``/var/lib/ceph/osd/<cluster>-<osd_id>/`` as a tmpfs mount.
#. Runs ``ceph-bluestore-tool prime-osd-dir`` against the block zvol to
   repopulate the directory from BlueStore's on-disk labels.
#. Starts ``ceph-osd`` for the OSD.

Idempotency
-----------

Activation is fully idempotent. Running it on an already-active OSD reports
the OSD as running and skips it safely, so ``--all`` can be placed in a
startup script without risk.

Summary
-------

To recap the ``activate`` process:

#. Scan imported ZFS pools for ``ceph:managed_by = ceph-volume-zfs``.
#. For each matching OSD (or the specified ID), create a tmpfs at the OSD
   data directory.
#. Run ``ceph-bluestore-tool prime-osd-dir`` to reconstruct the directory
   from the zvol's BlueStore labels.
#. Start the ``ceph-osd`` daemon.
