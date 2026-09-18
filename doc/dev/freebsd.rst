==============================
FreeBSD Implementation details
==============================


Disk and ZFS layout
-------------------

``ceph-volume zfs`` creates the following ZFS structure per OSD:

Pool
~~~~

A dedicated zpool per OSD, named ``ceph-osd-<osd_id>``, created with
``-m none`` to suppress the automatic root dataset mountpoint::

    zpool create -f -m none ceph-osd-0 /dev/ada1

The pool carries these ZFS user properties (set via ``zpool set``)::

    ceph:osd_id       = 0
    ceph:osd_fsid     = <osd_fsid>
    ceph:managed_by   = ceph-volume-zfs
    ceph:type         = block
    ceph:zvol_name    = osd-block-<osd_fsid>

Block zvol
~~~~~~~~~~

A zvol inside the pool, named ``osd-block-<osd_fsid>``, sized to 95%
of the pool's available space (leaving headroom for ZFS metadata)::

    zfs create -V <size> ceph-osd-0/osd-block-<osd_fsid>

Exposed as ``/dev/zvol/ceph-osd-0/osd-block-<osd_fsid>`` and used as
the BlueStore block device. By default thick-provisioned (full
``refreservation``); pass ``--thin`` for sparse zvols.

The zvol carries these ZFS user properties (set via ``zfs set``)::

    ceph:osd_id             = 0
    ceph:osd_fsid           = <osd_fsid>
    ceph:cluster_name       = ceph
    ceph:cluster_fsid       = <cluster_fsid>
    ceph:type               = block
    ceph:objectstore        = bluestore
    ceph:crush_device_class = <class or empty>
    ceph:block_device       = /dev/zvol/ceph-osd-0/osd-block-<osd_fsid>

Optional db/wal zvols
~~~~~~~~~~~~~~~~~~~~~

If ``--block.db`` or ``--block.wal`` is given, additional zvols are
created **before** the block zvol so sizing uses genuinely remaining
space. Two forms are accepted:

* ``--block.db same-pool`` — db zvol carved from the same pool as the
  block device (requires ``--block-db-size``). Convenient but provides
  no I/O separation.
* ``--block.db /dev/ada2`` — dedicated pool ``ceph-osd-0-db`` on a
  separate (typically faster) device.

The db/wal zvols carry the same ``ceph:*`` properties as the block
zvol, plus cross-links so any zvol names the others::

    ceph:type         = db   (or wal)
    ceph:block_device = /dev/zvol/ceph-osd-0/osd-block-<osd_fsid>

The block zvol in turn gets::

    ceph:db_device    = /dev/zvol/<db-pool>/osd-db-<osd_fsid>
    ceph:wal_device   = /dev/zvol/<wal-pool>/osd-wal-<osd_fsid>

OSD data directory
~~~~~~~~~~~~~~~~~~

The OSD data directory (``/var/lib/ceph/osd/ceph-<id>``) is a tmpfs
mount rebuilt at activate time from the zvol's BlueStore labels::

    mount -t tmpfs tmpfs /var/lib/ceph/osd/ceph-0
    ceph-bluestore-tool prime-osd-dir \
        --path /var/lib/ceph/osd/ceph-0 \
        --dev /dev/zvol/ceph-osd-0/osd-block-<osd_fsid>

The durable state lives on the zvol itself; losing the tmpfs on reboot
is harmless and expected.


Automated OSD management (ceph-volume zfs)
------------------------------------------

``ceph-volume zfs`` is the FreeBSD-native OSD lifecycle tool. It uses ZFS
pools and zvols as the underlying block storage, replacing the Linux LVM/raw
backend with FreeBSD-native equivalents.

Each OSD consists of:

* A ZFS pool named ``ceph-osd-<id>`` created with ``-m none`` (no root dataset
  mountpoint) on a dedicated disk or partition.
* A zvol named ``osd-block-<osd_fsid>`` inside that pool, used as the
  BlueStore block device.
* Optional db and wal zvols (``osd-db-<osd_fsid>`` / ``osd-wal-<osd_fsid>``)
  either in the same pool (``--block.db same-pool``) or on a dedicated device.

All pools and zvols are tagged with ZFS user properties for discovery without
requiring a running Ceph cluster.

Subcommands::

    ceph-volume zfs inventory          # list available disks
    ceph-volume zfs prepare --data /dev/ada1
    ceph-volume zfs list               # list Ceph OSDs
    ceph-volume zfs activate --all     # activate all OSDs at boot
    ceph-volume zfs zap /dev/ada1      # dry-run wipe
    ceph-volume zfs zap --force /dev/ada1

Global flags ``-v``/``--verbose``, ``-j``/``--json``, ``--format``,
``--debug``, and ``-n``/``--dry-run`` are accepted before or after the
subcommand.

Thick-provisioned zvols are the default (prevents pool oversubscription).
Pass ``--thin`` to use sparse zvols instead.


Configuration
-------------

As per FreeBSD default, extra software installs under ``/usr/local/``.
The default Ceph configuration path is ``/usr/local/etc/ceph/ceph.conf``.
Create a symlink so the standard ``/etc/ceph`` path works::

    ln -s /usr/local/etc/ceph /etc/ceph

A sample configuration file is at
``/usr/local/share/doc/ceph/sample.ceph.conf``.


Build system
------------

FreeBSD builds use clang (the system compiler). Several GNU-ld-specific
linker flags are automatically disabled when building with clang or lld:

* ``--copy-dt-needed-entries`` is skipped (guarded by ``CMAKE_CXX_COMPILER_ID``).
* ``--no-undefined-version`` is the lld default; stale version-script entries
  are pruned from ``librados.map``.
* ``HAVE_ASM_SYMVER`` and ``HAVE_ATTR_SYMVER`` are forced off on FreeBSD to
  prevent empty-version ``sym@@`` symbols that lld rejects.

The build is driven by ``do_freebsd.sh``, which sets FreeBSD-specific cmake
options and calls ninja. Key options:

* ``WITH_LIBURING=OFF`` — io_uring is Linux-only; POSIX AIO is used instead.
* ``WITH_LIBCEPHFS_PROXY=OFF`` — Linux fscrypt dependency, disabled on FreeBSD.
* ``WITH_BLUESTORE=ON``, ``WITH_RBD=ON`` — fully supported.
* ``WITH_CEPHFS=ON`` — CephFS client (FUSE-based only; no kernel module).


MON creation
------------

Monitors are created by following the manual creation steps at::

    https://docs.ceph.com/en/latest/install/manual-freebsd-deployment/


OSD creation
------------

Use ``ceph-volume zfs prepare`` for automated OSD creation (recommended).


Running a test cluster (vstart)
--------------------------------

``vstart.sh`` works on FreeBSD. BlueStore is the default objectstore and
works correctly via POSIX AIO — no special flags are needed::

    cd build
    MON=1 OSD=1 MDS=0 MGR=0 RGW=0 ../src/vstart.sh -n -x

For a full cluster with all daemons::

    MON=1 OSD=1 MDS=1 MGR=1 RGW=1 ../src/vstart.sh -n -x

BlueStore and POSIX AIO
-----------------------

On FreeBSD, BlueStore's async I/O uses ``SIGEV_KEVENT`` via the POSIX AIO
interface (``aio_write()`` / ``aio_read()``) with kqueue for completion
notification. This is the ``HAVE_POSIXAIO`` path in ``blk/aio/``.

This is the same mechanism that Darwin declares but does not implement —
``aio_write()`` with ``SIGEV_KEVENT`` returns ``EINVAL`` on macOS, and
``kern.aioprocmax`` is capped at 16. On FreeBSD both work correctly:
``SIGEV_KEVENT`` delivers completions to a kqueue, and there is no
artificial limit on outstanding AIO requests.

The practical result is that BlueStore-backed OSDs with persistent storage
work on FreeBSD. io_uring is Linux-only and not used.


Known limitations
-----------------

* No cephadm support — cephadm is Linux-only (container-based deployment).
  Manual deployment is required.
* No RBD kernel module — use ``rbd-ggate`` (userspace via GEOM Gate) instead.
* No CephFS kernel client — use ``ceph-fuse`` instead.

