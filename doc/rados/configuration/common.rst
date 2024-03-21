.. _ceph-conf-common-settings:

Common Settings
===============

The `Hardware Recommendations`_ section provides some hardware guidelines for
configuring a Ceph Storage Cluster. It is possible for a single :term:`Ceph
Node` to run multiple daemons. For example, a single node with multiple drives
ususally runs one ``ceph-osd`` for each drive. Ideally, each node will be
assigned to a particular type of process. For example, some nodes might run
``ceph-osd`` daemons, other nodes might run ``ceph-mds`` daemons, and still
other nodes might run ``ceph-mon`` daemons.

Each node has a name. The name of a node can be found in its ``host`` setting.
Monitors also specify a network address and port (that is, a domain name or IP
address) that can be found in the ``addr`` setting. A basic configuration file
typically specifies only minimal settings for each instance of monitor daemons.
For example:




.. code-block:: ini

    [global]
    mon_initial_members = ceph1
    mon_host = 10.0.0.1

.. important:: The ``host`` setting's value is the short name of the node. It
   is not an FQDN. It is **NOT** an IP address. To retrieve the name of the
   node, enter ``hostname -s`` on the command line. Unless you are deploying
   Ceph manually, do not use ``host`` settings for anything other than initial
   monitor setup.  **DO NOT** specify the ``host`` setting under individual
   daemons when using deployment tools like ``chef`` or ``cephadm``. Such tools
   are designed to enter the appropriate values for you in the cluster map.


.. _ceph-network-config:

Networks
========

For more about configuring a network for use with Ceph, see the `Network
Configuration Reference`_ .


Monitors
========

Ceph production clusters typically provision at least three :term:`Ceph
Monitor` daemons to ensure availability in the event of a monitor instance
crash. A minimum of three :term:`Ceph Monitor` daemons ensures that the Paxos
algorithm is able to determine which version of the :term:`Ceph Cluster Map` is
the most recent. It makes this determination by consulting a majority of Ceph
Monitors in the quorum.

.. note:: You may deploy Ceph with a single monitor, but if the instance fails,
   the lack of other monitors might interrupt data-service availability.

Ceph Monitors normally listen on port ``3300`` for the new v2 protocol, and on
port ``6789`` for the old v1 protocol.

By default, Ceph expects to store monitor data on the following path::

    /var/lib/ceph/mon/$cluster-$id

You or a deployment tool (for example, ``cephadm``) must create the
corresponding directory. With metavariables fully expressed and a cluster named
"ceph", the path specified in the above example evaluates to::

    /var/lib/ceph/mon/ceph-a

For additional details, see the `Monitor Config Reference`_.

.. _Monitor Config Reference: ../mon-config-ref


.. _ceph-osd-config:

Authentication
==============

.. versionadded:: Bobtail 0.56

Authentication is explicitly enabled or disabled in the ``[global]`` section of
the Ceph configuration file, as shown here:

.. code-block:: ini

    auth_cluster_required = cephx
    auth_service_required = cephx
    auth_client_required = cephx

In addition, you should enable message signing. For details, see `Cephx Config
Reference`_.

.. _Cephx Config Reference: ../auth-config-ref


.. _ceph-monitor-config:


OSDs
====

When Ceph production clusters deploy :term:`Ceph OSD Daemons`, the typical
arrangement is that one node has one OSD daemon running Filestore on one
storage device. BlueStore is now the default back end, but when using Filestore
you must specify a journal size. For example:

.. code-block:: ini

    [osd]
    osd_journal_size = 10000

    [osd.0]
    host = {hostname} #manual deployments only.


By default, Ceph expects to store a Ceph OSD Daemon's data on the following
path::

    /var/lib/ceph/osd/$cluster-$id

You or a deployment tool (for example, ``cephadm``) must create the
corresponding directory. With metavariables fully expressed and a cluster named
"ceph", the path specified in the above example evaluates to::

    /var/lib/ceph/osd/ceph-0

You can override this path using the ``osd_data`` setting. We recommend that
you do not change the default location. To create the default directory on your
OSD host, run the following commands:

.. prompt:: bash $

    ssh {osd-host}
    sudo mkdir /var/lib/ceph/osd/ceph-{osd-number}

The ``osd_data`` path must lead to a device that is not shared with the
operating system. To use a device other than the device that contains the
operating system and the daemons, prepare it for use with Ceph and mount it on
the directory you just created by running commands of the following form:

.. prompt:: bash $

    ssh {new-osd-host}
    sudo mkfs -t {fstype} /dev/{disk}
    sudo mount -o user_xattr /dev/{disk} /var/lib/ceph/osd/ceph-{osd-number}

We recommend using the ``xfs`` file system when running :command:`mkfs`. (The
``btrfs`` and ``ext4`` file systems are not recommended and are no longer
tested.)

For additional configuration details, see `OSD Config Reference`_.


Heartbeats
==========

During runtime operations, Ceph OSD Daemons check up on other Ceph OSD Daemons
and report their findings to the Ceph Monitor. This process does not require
you to provide any settings. However, if you have network latency issues, you
might want to modify the default settings.

For additional details, see `Configuring Monitor/OSD Interaction`_.


.. _ceph-logging-and-debugging:

Logs / Debugging
================

You might sometimes encounter issues with Ceph that require you to use Ceph's
logging and debugging features. For details on log rotation, see `Debugging and
Logging`_.

.. _Debugging and Logging: ../../troubleshooting/log-and-debug


Example ceph.conf
=================

.. literalinclude:: demo-ceph.conf
   :language: ini

.. _ceph-runtime-config:



Naming Clusters (deprecated)
============================

Each Ceph cluster has an internal name. This internal name is used as part of
configuration, and as part of "log file" names as well as part of directory
names and as part of mountpoint names. This name defaults to "ceph". Previous
releases of Ceph allowed one to specify a custom name instead, for example
"ceph2". This option was intended to facilitate the running of multiple logical
clusters on the same physical hardware, but in practice it was rarely
exploited. Custom cluster names should no longer be attempted. Old
documentation might lead readers to wrongly think that unique cluster names are
required to use ``rbd-mirror``. They are not required.

Custom cluster names are now considered deprecated and the ability to deploy
them has already been removed from some tools, although existing custom-name
deployments continue to operate. The ability to run and manage clusters with
custom names might be progressively removed by future Ceph releases, so **it is
strongly recommended to deploy all new clusters with the default name "ceph"**.

Some Ceph CLI commands accept a ``--cluster`` (cluster name) option. This
option is present only for the sake of backward compatibility. New tools and
deployments cannot be relied upon to accommodate this option.

If you need to allow multiple clusters to exist on the same host, use
:ref:`cephadm`, which uses containers to fully isolate each cluster.

.. _Hardware Recommendations: ../../../start/hardware-recommendations
.. _Network Configuration Reference: ../network-config-ref
.. _OSD Config Reference: ../osd-config-ref
.. _Configuring Monitor/OSD Interaction: ../mon-osd-interaction
