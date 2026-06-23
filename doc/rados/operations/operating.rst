=====================
 Operating a Cluster
=====================

.. index:: systemd; operating a cluster


Running Ceph with systemd
=========================

In deployments managed by systemd, Ceph daemons behave like any other
daemons that can be controlled by the ``systemctl`` command, as in
the following examples. Note that this applies to Podman-based container
deployments. Docker-based deployments do not integrate with systemd in the
same way and may require different management commands.

.. prompt:: bash $

   sudo systemctl start ceph.target       # start all daemons
   sudo systemctl status ceph-osd@12      # check status of osd.12

To list all of the Ceph systemd units on a node, run the following command:

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
   sudo systemctl start ceph-mon@{hostname}
   sudo systemctl start ceph-mds@{hostname}

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
   sudo systemctl stop ceph-mon@{hostname}
   sudo systemctl stop ceph-mds@{hostname}

For example:

.. prompt:: bash $

   sudo systemctl stop ceph-osd@1
   sudo systemctl stop ceph-mon@ceph-server
   sudo systemctl stop ceph-mds@ceph-server


.. _Valgrind: http://www.valgrind.org/
