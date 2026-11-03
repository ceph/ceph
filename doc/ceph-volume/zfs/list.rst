.. _ceph-volume-zfs-list:

``list``
========

The ``list`` subcommand reports all Ceph OSDs on the host that were deployed
by ``ceph-volume zfs``. Discovery is done entirely through ZFS user properties
(``ceph:*``); no running Ceph cluster is required.

List all OSDs::

    ceph-volume zfs list

List a specific OSD by ID::

    ceph-volume zfs list 0

List by pool name::

    ceph-volume zfs list ceph-osd-0

List by zvol device path::

    ceph-volume zfs list /dev/zvol/ceph-osd-0/osd-block-<fsid>

JSON output::

    ceph-volume zfs list -j
    ceph-volume zfs list --format json-pretty

Output
------

Plain output shows one entry per OSD with the pool name, zvol path, OSD ID,
OSD fsid, objectstore type, and the backing vdev(s)::

    ====== osd.0 =======

      [block]    /dev/zvol/ceph-osd-0/osd-block-3b4e1a2c-...

          osd id                    0
          osd fsid                  3b4e1a2c-...
          cluster fsid              ce454d91-...
          type                      block
          objectstore               bluestore
          pool                      ceph-osd-0
          block device              /dev/zvol/ceph-osd-0/osd-block-3b4e1a2c-...
          vdev                      /dev/ada1

If db or wal zvols exist they are shown as additional entries under the same
OSD heading.

JSON output includes all ``ceph:*`` ZFS properties as stored on the pool and
zvol, unmodified.
