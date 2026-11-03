=====================
 Operating a Cluster
=====================

.. index:: systemd; operating a cluster


.. _rados-operating-daemons:

Starting and stopping daemons
=============================

How you start, stop, and restart Ceph daemons depends on how the cluster was
deployed. Other pages in this documentation link here instead of repeating
these commands. The examples use ``osd.12``: substitute the name of your
daemon.

.. list-table::
   :header-rows: 1
   :widths: 24 38 38

   * - Task
     - Cluster managed by cephadm
     - Cluster installed from packages, on the daemon's host
   * - Start one daemon
     - ``ceph orch daemon start osd.12``
     - ``sudo systemctl start ceph-osd@12``
   * - Stop one daemon
     - ``ceph orch daemon stop osd.12``
     - ``sudo systemctl stop ceph-osd@12``
   * - Restart one daemon
     - ``ceph orch daemon restart osd.12``
     - ``sudo systemctl restart ceph-osd@12``
   * - Show whether one daemon is running
     - ``ceph orch ps --daemon_type osd --daemon_id 12``
     - ``sudo systemctl status ceph-osd@12``
   * - Stop every Ceph daemon on a host
     - Put the host into maintenance mode (see :ref:`cephadm-maintenance-mode`),
       or run ``sudo systemctl stop ceph-{fsid}.target`` on the host
     - ``sudo systemctl stop ceph\*.service ceph\*.target``
   * - Start every Ceph daemon on a host
     - Take the host out of maintenance mode, or run
       ``sudo systemctl start ceph-{fsid}.target`` on the host
     - ``sudo systemctl start ceph.target``
   * - Start a daemon at boot
     - cephadm does this for every daemon that it deploys
     - ``sudo systemctl enable ceph-osd@12``

In a cluster managed by cephadm, ``ceph orch ps`` lists the names of the
daemons. See :ref:`cephadm-daemon-control`, which also covers starting and
stopping all of the daemons of a service.

In a cluster installed from packages, the unit of a daemon is
``ceph-{type}@{id}``, where ``{id}`` is the part of the daemon's name after the
type: ``ceph-osd@12`` for ``osd.12``, and ``ceph-mon@a`` for ``mon.a``. The
Ceph Object Gateway is the exception: the unit of ``client.rgw.{name}`` is
``ceph-radosgw@rgw.{name}``. The sections below give the details.

On FreeBSD, ``rc.d`` scripts manage the Ceph daemons: see
:doc:`/install/manual-freebsd-deployment`.


Running Ceph with systemd
=========================

In a cluster installed from packages, Ceph daemons behave like any other
services controlled by the ``systemctl`` command, as in the following examples.
The unit names in this section are those that the Ceph packages install.

.. prompt:: bash $

   sudo systemctl start ceph.target       # start all daemons
   sudo systemctl status ceph-osd@12      # check status of osd.12

To show the status of the Ceph units that systemd has loaded on a node, run the
following command. A unit that is stopped and disabled is usually not loaded,
and is then not shown:

.. prompt:: bash $

   sudo systemctl status ceph\*.service ceph\*.target


Starting all daemons
--------------------

To start all of the daemons on a Ceph node (regardless of their type), run the
following command:

.. prompt:: bash $

   sudo systemctl start ceph.target


Stopping all daemons
--------------------

To stop all of the daemons on a Ceph node (regardless of their type), run the
following command:

.. prompt:: bash $

   sudo systemctl stop ceph\*.service ceph\*.target


Starting all daemons by type
----------------------------

To start all of the daemons of a particular type on a Ceph node, run one of the
following commands:

.. prompt:: bash $

   sudo systemctl start ceph-osd.target
   sudo systemctl start ceph-mon.target
   sudo systemctl start ceph-mds.target


Stopping all daemons by type
----------------------------

To stop all of the daemons of a particular type on a Ceph node, run one of the
following commands:

.. prompt:: bash $

   sudo systemctl stop ceph-osd\*.service ceph-osd.target
   sudo systemctl stop ceph-mon\*.service ceph-mon.target
   sudo systemctl stop ceph-mds\*.service ceph-mds.target


Starting a daemon
-----------------

To start a specific daemon instance on a Ceph node, run one of the
following commands:

.. prompt:: bash $

   sudo systemctl start ceph-osd@{id}
   sudo systemctl start ceph-mon@{mon-id}
   sudo systemctl start ceph-mds@{mds-id}

The name after ``@`` is the daemon's ID, the part of its name after the type:
``12`` for ``osd.12``, ``a`` for ``mon.a``. Many deployments use the short host
name (the output of ``hostname -s``) as the ID of a Monitor or MDS.

For example:

.. prompt:: bash $

   sudo systemctl start ceph-osd@1
   sudo systemctl start ceph-mon@ceph-server
   sudo systemctl start ceph-mds@ceph-server


Stopping a daemon
-----------------

To stop a specific daemon instance on a Ceph node, run one of the
following commands:

.. prompt:: bash $

   sudo systemctl stop ceph-osd@{id}
   sudo systemctl stop ceph-mon@{mon-id}
   sudo systemctl stop ceph-mds@{mds-id}

For example:

.. prompt:: bash $

   sudo systemctl stop ceph-osd@1
   sudo systemctl stop ceph-mon@ceph-server
   sudo systemctl stop ceph-mds@ceph-server
