.. _manual-freebsd-deployment:

==============================
 Manual Deployment on FreeBSD
==============================

.. note:: cephadm is not available on FreeBSD, so manual deployment is
   necessary on that platform. Note that FreeBSD is not supported by the core
   Ceph effort. On Linux, the recommended method is
   :ref:`cephadm <cephadm_deploying_new_cluster>` instead of the procedures
   described here.

This page describes how to bring up a Ceph cluster on FreeBSD: a first
monitor, a manager, and OSDs backed by ZFS. Daemons are started by the
``rc.d`` scripts installed with the ``net/ceph21`` port, and OSD disks are
prepared with :ref:`ceph-volume zfs <ceph-volume-zfs>`.

There is no ``cephadm``, no containers and no systemd on FreeBSD. Everything
below is done with the plain Ceph tools and ``service(8)``.

Overview
========

A running cluster consists of these pieces:

* ``ceph_mon``: the monitors. Start these first, they form the quorum.
* ``ceph_mgr``: the manager daemons.
* ``ceph_osd``: the object storage daemons. Each OSD lives on its own ZFS
  pool, created by ``ceph-volume zfs prepare``.
* ``ceph_mds`` and ``ceph_radosgw``: optional, for CephFS and the S3/Swift
  gateway.

Each daemon type has one ``rc.d`` script that manages any number of daemons
(*instances*) of that type on the host. Instances are identified by the
daemon id: ``service ceph_osd start 12`` starts ``osd.12`` only, while
``service ceph_osd start`` starts all of them.

Requirements
============

* FreeBSD with ZFS, and ``zfs_enable="YES"`` in ``/etc/rc.conf``, so the OSD
  pools are imported at boot.
* The ``net/ceph21`` port (or package) installed.
* Working time synchronisation, for example ``ntpd_enable="YES"`` and
  ``ntpd_sync_on_start="YES"``. Monitors complain about clock skew.
* Hostnames that resolve on every node. By convention the monitor id is the
  short hostname.
* The following ports open between the nodes: 3300 and 6789 (monitors) and
  6800-7300 (managers, OSDs, MDS).
* An empty disk for every OSD. ``ceph-volume zfs`` refuses to use a device
  that has partitions, is mounted, is used as swap or belongs to a zpool.

File locations
==============

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Path
     - Contents
   * - ``/usr/local/etc/ceph/``
     - ``ceph.conf`` and the admin keyring. Optionally symlink
       ``/etc/ceph`` to it.
   * - ``/var/lib/ceph/<type>/<cluster>-<id>/``
     - Data directory of one daemon, for example
       ``/var/lib/ceph/mon/ceph-node1``. OSD directories are tmpfs and are
       rebuilt at every boot.
   * - ``/var/lib/ceph/bootstrap-osd/``
     - Keyring used to register new OSDs.
   * - ``/var/run/ceph/``
     - Pid files and admin sockets.
   * - ``/var/log/ceph/``
     - Ceph's own log files. The ``rc.d`` scripts also log to syslog.

The ceph user
=============

The daemons run as the unprivileged user ``ceph``. The package creates this
user and the group ``ceph`` when it is installed, so there is nothing to
create by hand.

The package also creates the directory tree below ``/var/lib/ceph``
(``mon``, ``mgr``, ``osd``, ``mds``, ``radosgw``, ``tmp`` and the
``bootstrap-*`` directories) with the right ownership, so the daemons can
create their own data directories there.

The ``rc.d`` scripts create ``/var/run/ceph`` and ``/var/log/ceph`` themselves.

Cluster configuration
=====================

Generate a cluster id:

.. prompt:: bash #

   uuidgen

Create ``/usr/local/etc/ceph/ceph.conf``, replacing ``<fsid>`` with the
generated id, ``node1`` with the short hostname of the first monitor and the
addresses with your own:

.. code-block:: ini

   [global]
   fsid = <fsid>
   mon_initial_members = node1
   mon_host = 192.0.2.11
   public_network = 192.0.2.0/24
   auth_cluster_required = cephx
   auth_service_required = cephx
   auth_client_required = cephx
   osd_objectstore = bluestore

.. note:: For a single-node test cluster add ``osd_pool_default_size = 1``
   and ``osd_crush_chooseleaf_type = 0`` to the ``[global]`` section.

Keyrings and monitor map
========================

Create the keyring of the monitors, the admin keyring and the keyring that is
used to register new OSDs, and combine them:

.. prompt:: bash #

   ceph-authtool --create-keyring /tmp/ceph.mon.keyring --gen-key -n mon. --cap mon 'allow *'
   ceph-authtool --create-keyring /usr/local/etc/ceph/ceph.client.admin.keyring --gen-key -n client.admin --cap mon 'allow *' --cap osd 'allow *' --cap mds 'allow *' --cap mgr 'allow *'
   ceph-authtool --create-keyring /var/lib/ceph/bootstrap-osd/ceph.keyring --gen-key -n client.bootstrap-osd --cap mon 'profile bootstrap-osd' --cap mgr 'allow r'
   ceph-authtool /tmp/ceph.mon.keyring --import-keyring /usr/local/etc/ceph/ceph.client.admin.keyring
   ceph-authtool /tmp/ceph.mon.keyring --import-keyring /var/lib/ceph/bootstrap-osd/ceph.keyring

Create the initial monitor map, again with your own ``<fsid>``, hostname and
address:

.. prompt:: bash #

   monmaptool --create --add node1 192.0.2.11 --fsid <fsid> /tmp/monmap
   chown ceph:ceph /tmp/ceph.mon.keyring /tmp/monmap /var/lib/ceph/bootstrap-osd/ceph.keyring

First monitor
=============

Create the monitor's data store and start it:

.. prompt:: bash #

   ceph-mon --mkfs -i node1 --monmap /tmp/monmap --keyring /tmp/ceph.mon.keyring --setuser ceph --setgroup ceph
   sysrc ceph_mon_enable=YES
   service ceph_mon start

Check that it formed a quorum of one:

.. prompt:: bash #

   ceph -s

The cluster reports ``HEALTH_WARN`` until a manager and OSDs exist.

Remove the temporary files once all monitors are created:

.. prompt:: bash #

   rm /tmp/ceph.mon.keyring /tmp/monmap

.. _freebsd-copy-config:

Copying the configuration to other nodes
========================================

Every node that runs a Ceph daemon, or that you use to run ``ceph`` commands,
needs the cluster configuration. Copy it from the first node with ``scp``.
Use ``-p`` to keep the file modes. The package has already created
``/usr/local/etc/ceph`` on the new node.

.. prompt:: bash #

   scp -p /usr/local/etc/ceph/ceph.conf root@node2:/usr/local/etc/ceph/
   scp -p /usr/local/etc/ceph/ceph.client.admin.keyring root@node2:/usr/local/etc/ceph/

A node that prepares OSDs with ``ceph-volume zfs`` also needs the bootstrap-osd
keyring:

.. prompt:: bash #

   scp -p /var/lib/ceph/bootstrap-osd/ceph.keyring root@node2:/var/lib/ceph/bootstrap-osd/

On the new node, give the bootstrap keyring to the ``ceph`` user, as on the
first node:

.. prompt:: bash #

   chown ceph:ceph /var/lib/ceph/bootstrap-osd/ceph.keyring

When you run ``scp`` as another user than ``root``, for example because
``sshd`` does not allow root logins, copy the files to that user's home
directory first and move them into place on the new node.

.. warning:: The admin keyring gives full control over the cluster, and the
   bootstrap-osd keyring can register new OSDs. Only copy them to nodes that
   need them, and keep them readable by ``root`` only, which is what
   ``scp -p`` does when the source files are ``0600``. A node that only runs
   OSDs needs ``ceph.conf`` and, to prepare OSDs, the bootstrap-osd keyring.
   It does not need the admin keyring: the start-up quorum check then uses the
   key of a local OSD (see :ref:`freebsd-osd-startup`).

``ceph.conf`` is the same on every node. After you change it, for example when
a monitor is added to ``mon_host``, copy it to all nodes again:

.. prompt:: bash #

   scp -p /usr/local/etc/ceph/ceph.conf root@node2:/usr/local/etc/ceph/
   scp -p /usr/local/etc/ceph/ceph.conf root@node3:/usr/local/etc/ceph/

Adding more monitors
====================

Run a production cluster with three monitors, because the monitors need a
majority to form a quorum. On each additional node, with ``ceph.conf`` and the
admin keyring in place (see :ref:`freebsd-copy-config`):

.. prompt:: bash #

   ceph auth get mon. -o /tmp/ceph.mon.keyring
   ceph mon getmap -o /tmp/monmap
   chown ceph:ceph /tmp/ceph.mon.keyring /tmp/monmap
   ceph-mon --mkfs -i node2 --monmap /tmp/monmap --keyring /tmp/ceph.mon.keyring --setuser ceph --setgroup ceph
   sysrc ceph_mon_enable=YES
   service ceph_mon start

Then list every monitor in ``mon_host`` in ``ceph.conf``, and copy that file
to all nodes.

Manager
=======

Create a key for the manager and start it:

.. prompt:: bash #

   mkdir -p /var/lib/ceph/mgr/ceph-node1
   ceph auth get-or-create mgr.node1 mon 'allow profile mgr' osd 'allow *' mds 'allow *' -o /var/lib/ceph/mgr/ceph-node1/keyring
   chown -R ceph:ceph /var/lib/ceph/mgr
   sysrc ceph_mgr_enable=YES
   service ceph_mgr start

.. _freebsd_adding_osds:

OSDs
====

OSDs are prepared with ``ceph-volume zfs``. For every disk it creates a zpool
named ``ceph-osd-<id>`` (not mounted), a zvol named ``osd-block-<osd fsid>``
inside it that serves as the raw block device for BlueStore, and tags both
with ``ceph:*`` ZFS properties. These properties are how the OSD is found
again after a reboot, without contacting a monitor.

Inspect the disks
-----------------

.. prompt:: bash #

   ceph-volume zfs inventory

Add ``--format json`` for machine readable output. A disk that is partitioned,
mounted, used as swap or part of a zpool is not available. Clear it with
``zap``, which only prints what it would do unless ``--force`` is given:

.. prompt:: bash #

   ceph-volume zfs zap /dev/ada1
   ceph-volume zfs zap /dev/ada1 --force

``--destroy`` additionally destroys the zpool when it is a Ceph-managed one.

Prepare an OSD
--------------

First look at the plan. ``--dry-run`` prints the OSD id, the pool and zvol
names and every command that would run, without changing anything and without
contacting a monitor:

.. prompt:: bash #

   ceph-volume zfs prepare --data /dev/ada1 --dry-run

Then run it for real:

.. prompt:: bash #

   ceph-volume zfs prepare --data /dev/ada1

This creates the zpool and zvol, registers a new OSD with the cluster
(``ceph osd new``, using the bootstrap-osd keyring created above) and runs
``ceph-osd --mkfs``. The OSD is not started by this command.

Useful options:

``--block.db <device>`` and ``--block.wal <device>``
   Put the BlueStore RocksDB or write-ahead log on a separate device, for
   example an NVMe disk. The device gets its own zpool. ``same-pool`` carves
   it from the main pool, which is allowed but gains nothing.

``--block-db-size`` and ``--block-wal-size``
   Size of those zvols. Required with ``same-pool``, otherwise 95% of the
   pool.

``--zvol-size``
   Size of the data zvol, for example ``1T``. Default is 95% of the pool.

``--thin``
   Create sparse zvols. By default zvols are space reserved, so the pool
   cannot be oversubscribed. A full pool shows up as write errors in
   BlueStore, so only use this if you monitor free space.

``--osd-id`` and ``--osd-fsid``
   Reuse an existing OSD id, or choose the OSD uuid.

``--crush-device-class``
   Set the CRUSH device class of the OSD.

``--no-tmpfs``
   Keep the OSD directory on disk instead of tmpfs. Set
   ``ceph_osd_tmpfs="NO"`` in ``rc.conf`` as well when you use this.

.. note:: ``--test`` creates the real zpool and zvol but skips everything that
   needs a Ceph binary or a monitor. It does not produce a usable OSD and is
   meant for testing the ZFS side only.

List the prepared OSDs:

.. prompt:: bash #

   ceph-volume zfs list

Start the OSDs
--------------

.. prompt:: bash #

   sysrc ceph_osd_enable=YES rcshutdown_timeout=300
   service ceph_osd start
   service ceph_osd status
   ceph osd tree

``service ceph_osd start`` returns immediately. The OSDs are started in the
background once the monitors have a quorum, see :ref:`freebsd-osd-startup`.

Verify the cluster
==================

.. prompt:: bash #

   ceph -s
   ceph health detail
   ceph osd tree

Reboot the node at least once before relying on the setup, to see that the
daemons come back on their own.

The rc.d services
=================

The scripts are ``ceph_mon``, ``ceph_mgr``, ``ceph_osd``, ``ceph_mds`` and
``ceph_radosgw``. All of them accept the usual ``start``, ``stop``,
``restart`` and ``status``, plus ``reload``, which sends ``SIGHUP`` to the
daemon so it reopens its log file. Add an id to act on one instance, for
example ``service ceph_mon restart node1``.

Every daemon runs under ``daemon(8)``, which restarts it ten seconds after it
exits. There is no limit on the number of restarts, so a daemon that fails at
once keeps being restarted, and keeps logging. Look at ``/var/log/messages``
and ``/var/log/ceph/`` when a daemon does not stay up. Output of the
supervisor goes to syslog with the tag ``<cluster>-<type>.<id>``.

Configuration in ``/etc/rc.conf``
---------------------------------

Settings shared by all daemons; the values shown are the defaults.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Variable
     - Meaning
   * - ``ceph_cluster="ceph"``
     - Cluster name.
   * - ``ceph_conf``
     - Default ``/usr/local/etc/ceph/${ceph_cluster}.conf``.
   * - ``ceph_datadir="/var/lib/ceph"``
     - Base of the data directories.
   * - ``ceph_rundir="/var/run/ceph"``
     - Pid files and admin sockets.
   * - ``ceph_logdir="/var/log/ceph"``
     - Log directory.
   * - ``ceph_user="ceph"``, ``ceph_group="ceph"``
     - The daemons switch to this user and group.
   * - ``ceph_restart_delay="10"``
     - Seconds before an exited daemon is restarted.

Settings per daemon type. ``<type>`` is ``mon``, ``mgr``, ``osd``, ``mds`` or
``radosgw``.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Variable
     - Meaning
   * - ``ceph_<type>_enable="NO"``
     - Enable the service at boot.
   * - ``ceph_<type>_instances``
     - Daemon ids to manage. Default: every directory
       ``${ceph_datadir}/<type>/${ceph_cluster}-<id>`` that holds a keyring.
   * - ``ceph_<type>_flags``
     - Extra arguments for the Ceph daemon itself.
   * - ``ceph_<type>_limits="-n 1048576"``
     - ``limits(1)`` arguments, here the number of open files.

.. _freebsd-osd-startup:

OSD start-up
------------

An OSD can only work when the monitors have a quorum, and a node that boots
with its monitors on other machines would otherwise start OSDs that fail
and restart. For that reason ``service ceph_osd start`` works in two steps:

#. It rebuilds the OSD directories. They live on tmpfs, so this is needed
   after every boot. By default ``ceph-volume zfs activate --all`` does this,
   using the ``ceph:*`` ZFS properties; no monitor is needed.
#. It starts a background job that polls ``ceph quorum_status`` every ten
   seconds and starts the OSDs as soon as a quorum exists.

``service ceph_osd stop`` also cancels a start that is still waiting, and
``service ceph_osd status`` reports it. Starting a single OSD by id,
``service ceph_osd start 12``, does not wait.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Variable
     - Meaning
   * - ``ceph_osd_activate="YES"``
     - Rebuild the OSD directories before starting.
   * - ``ceph_osd_activate_method="zfs"``
     - ``zfs`` uses ``ceph-volume zfs activate``. ``label`` scans devices for
       BlueStore labels, for OSDs that were not made with ``ceph-volume zfs``.
       ``none`` does nothing.
   * - ``ceph_osd_devices``
     - Only for method ``label``: devices or globs to scan. Default: all
       disks, their partitions and slices.
   * - ``ceph_osd_tmpfs="YES"``
     - Keep the OSD directories on tmpfs.
   * - ``ceph_osd_wait_quorum="YES"``
     - Start the OSDs in the background after the quorum is reached. ``NO``
       starts them immediately.
   * - ``ceph_osd_quorum_timeout="0"``
     - Give up waiting after this many seconds and start anyway. ``0`` waits
       forever.
   * - ``ceph_osd_quorum_args``
     - Identity used for the quorum check. Default: ``client.admin`` when its
       keyring is readable, otherwise the key of the first local OSD.
   * - ``ceph_osd_aio_unsafe="YES"``
     - Set ``vfs.aio.enable_unsafe=1``, which BlueStore needs on FreeBSD.

.. warning:: ``service ceph_osd activate`` re-primes the OSD directories. Do
   not run it by hand while OSDs are running. ``service ceph_osd start`` skips
   the activation by itself when an OSD is already running.

Boot order
----------

``rcorder(8)`` starts the services in this order, and stops them in reverse
at shutdown:

.. code-block:: none

   ceph_mon  ->  ceph_mgr
             ->  ceph_osd  ->  ceph_mds, ceph_radosgw

``ceph_osd`` returns right away, so the OSDs may come up later than the
services behind it. MDS and gateway daemons retry on their own until the OSDs
are there.

Stopping many OSDs takes a while. Raise ``rcshutdown_timeout`` in
``rc.conf``, 300 seconds is a reasonable start; the default of 90 is short.

Example ``rc.conf``
-------------------

A node with a monitor, a manager and OSDs:

.. code-block:: sh

   zfs_enable="YES"
   ntpd_enable="YES"
   ntpd_sync_on_start="YES"
   rcshutdown_timeout="300"

   ceph_mon_enable="YES"
   ceph_mgr_enable="YES"
   ceph_osd_enable="YES"

A node with OSDs only, while the monitors run elsewhere. The OSDs wait in
the background until those monitors have a quorum:

.. code-block:: sh

   zfs_enable="YES"
   ntpd_enable="YES"
   ntpd_sync_on_start="YES"
   rcshutdown_timeout="300"

   ceph_osd_enable="YES"

This node needs the same ``ceph.conf``, with ``mon_host`` set, and either the
admin keyring or a readable OSD keyring for the quorum check.

Metadata servers and gateways
=============================

Create a key and a data directory per daemon, enable the service, and start
it. For an MDS with id ``node1``:

.. prompt:: bash #

   mkdir -p /var/lib/ceph/mds/ceph-node1
   ceph auth get-or-create mds.node1 mon 'profile mds' mds 'allow *' osd 'allow *' -o /var/lib/ceph/mds/ceph-node1/keyring
   chown -R ceph:ceph /var/lib/ceph/mds
   sysrc ceph_mds_enable=YES
   service ceph_mds start

For a RADOS gateway with id ``rgw.node1``, which runs as ``client.rgw.node1``:

.. prompt:: bash #

   mkdir -p /var/lib/ceph/radosgw/ceph-rgw.node1
   ceph auth get-or-create client.rgw.node1 mon 'allow rw' osd 'allow rwx' -o /var/lib/ceph/radosgw/ceph-rgw.node1/keyring
   chown -R ceph:ceph /var/lib/ceph/radosgw
   sysrc ceph_radosgw_enable=YES
   service ceph_radosgw start

Creating a CephFS file system or an object store zone is not specific to
FreeBSD and is described in the general documentation.

Troubleshooting
===============

An OSD is not started after a boot
   Run ``service ceph_osd status``. If it says the start is waiting for a
   quorum, check ``ceph -s`` from a node that does have one, and that this
   node can reach the monitors listed in ``mon_host``. Set
   ``ceph_osd_quorum_timeout`` if you want it to start anyway.

``ceph-volume zfs list`` shows nothing after a boot
   Check that ``zfs_enable="YES"`` is set and that ``zpool list`` shows the
   ``ceph-osd-<id>`` pools.

A daemon keeps restarting
   Look in ``/var/log/messages`` and in ``/var/log/ceph/``. The usual causes
   are a missing keyring in the data directory, a data directory that is not
   owned by ``ceph``, and a ``ceph.conf`` that is not readable.

Monitors report clock skew
   Check that ``ntpd`` is running and synchronised with ``ntpq -p``.

See also
========

* :ref:`ceph-volume-zfs`
* :ref:`ceph-volume-zfs-inventory`
* :doc:`/dev/freebsd`
