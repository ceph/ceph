=================================
 Deploying a development cluster
=================================

In order to develop on ceph, a Ceph utility,
*vstart.sh*, allows you to deploy fake local cluster for development purpose.

Usage
=====

It allows to deploy a fake local cluster on your machine for development purpose. It starts rgw, mon, osd and/or mds, or all of them if not specified.

To start your development cluster, type the following::

	vstart.sh [OPTIONS]... [mon] [osd] [mds]

In order to stop the cluster, you can type::

	./stop.sh

Options
=======

.. option:: -i ip_address

    Bind to the specified *ip_address* instead of guessing and resolve from hostname.

.. option:: -k

    Keep old configuration files instead of overwritting theses.

.. option:: -l, --localhost

    Use localhost instead of hostanme.

.. option:: -m ip[:port]

    Specifies monitor *ip* address and *port*.

.. option:: -n, --new

    Create a new cluster.

.. option:: -o config

    Add *config* to all sections in the ceph configuration.

.. option:: -r

    Start radosgw (ceph needs to be compiled with --radosgw), create an apache2 configuration file, and start apache2 with it (needs apache2 with mod_fastcgi) on port starting from 8000.

.. option:: --nodaemon

    Use ceph-run as wrapper for mon/osd/mds.

.. option:: --smallmds

    Configure mds with small limit cache size.

.. option:: -x

    Enable Cephx (on by default).

.. option:: -X

    Disable Cephx.

.. option:: -d, --debug

    Launch in debug mode

.. option:: --valgrind[_{osd,mds,mon}] 'valgrind_toolname [args...]'

    Launch the osd/mds/mon/all the ceph binaries using valgrind with the specified tool and arguments.

.. option:: --{mon,osd,mds}_num

    Set the count of mon/osd/mds daemons

.. option:: --bluestore

    Use bluestore as the objectstore backend for osds

.. option:: --memstore

    Use memstore as the objectstore backend for osds

.. option:: --cache <pool>

    Set a cache-tier for the specified pool


Environment variables
=====================

{OSD,MDS,MON,RGW}

Theses environment variables will contains the number of instances of the desired ceph process you want to start.

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

  ./mstart.sh cluster1 -n -r


For stopping the cluster, you do::

  ./mstop.sh <cluster-name>
