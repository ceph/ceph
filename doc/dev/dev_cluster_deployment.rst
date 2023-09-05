.. _dev_deploying_a_development_cluster:

=================================
 Deploying a development cluster
=================================

In order to develop on ceph, a Ceph utility,
*vstart.sh*, allows you to deploy fake local cluster for development purpose.

Usage
=====

It allows to deploy a fake local cluster on your machine for development purpose. It starts rgw, mon, osd and/or mds, or all of them if not specified.

To start your development cluster, type the following::

	vstart.sh [OPTIONS]...

In order to stop the cluster, you can type::

	./stop.sh

Options
=======

.. option:: -b, --bluestore

    Use bluestore as the objectstore backend for osds.

.. option:: --cache <pool>

    Set a cache-tier for the specified pool.

.. option:: -d, --debug

    Launch in debug mode.

.. option:: -e

    Create an erasure pool.

.. option:: --hitset <pool> <hit_set_type>

    Enable hitset tracking.

.. option:: -i ip_address

    Bind to the specified *ip_address* instead of guessing and resolve from hostname.

.. option:: -k

    Keep old configuration files instead of overwriting these.

.. option:: -K, --kstore

    Use kstore as the osd objectstore backend.

.. option:: -l, --localhost

    Use localhost instead of hostname.

.. option:: -m ip[:port]

    Specifies monitor *ip* address and *port*.

.. option:: --memstore

    Use memstore as the objectstore backend for osds

.. option:: --multimds <count>

    Allow multimds with maximum active count.

.. option:: -n, --new

    Create a new cluster.

.. option:: -N, --not-new

    Reuse existing cluster config (default).

.. option:: --nodaemon

    Use ceph-run as wrapper for mon/osd/mds.

.. option:: --nolockdep

    Disable lockdep

.. option:: -o <config>

    Add *config* to all sections in the ceph configuration.

.. option:: --rgw_port <port>

    Specify ceph rgw http listen port.

.. option:: --rgw_frontend <frontend>

    Specify the rgw frontend configuration (default is civetweb).

.. option:: --rgw_compression <compression_type>

    Specify the rgw compression plugin (default is disabled).

.. option:: --smallmds

    Configure mds with small limit cache size.

.. option:: --short

    Short object names only; necessary for ext4 dev

.. option:: --valgrind[_{osd,mds,mon}] 'valgrind_toolname [args...]'

    Launch the osd/mds/mon/all the ceph binaries using valgrind with the specified tool and arguments.

.. option:: --without-dashboard

    Do not run using mgr dashboard.

.. option:: -x

    Enable cephx (on by default).

.. option:: -X

    Disable cephx.


Environment variables
=====================

{OSD,MDS,MON,RGW}

These environment variables will contains the number of instances of the desired ceph process you want to start.

Example: ::

	OSD=3 MON=3 RGW=1 vstart.sh


============================================================
 Deploying multiple development clusters on the same machine
============================================================

In order to bring up multiple ceph clusters on the same machine, *mstart.sh* a
small wrapper around the above *vstart* can help.

Usage
=====

To start multiple clusters, you would run mstart for each cluster you would want
to deploy, and it will start monitors, rgws for each cluster on different ports
allowing you to run multiple mons, rgws etc. on the same cluster. Invoke it in
the following way::

  mstart.sh <cluster-name> <vstart options>

For eg::

  ./mstart.sh cluster1 -n


For stopping the cluster, you do::

  ./mstop.sh <cluster-name>
