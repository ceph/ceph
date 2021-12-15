===========
MON Service
===========

.. _deploy_additional_monitors:

Deploying additional monitors 
=============================

A typical Ceph cluster has three or five monitor daemons that are spread
across different hosts.  We recommend deploying five monitors if there are
five or more nodes in your cluster.

.. _CIDR: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing#CIDR_notation

Ceph deploys monitor daemons automatically as the cluster grows and Ceph
scales back monitor daemons automatically as the cluster shrinks. The
smooth execution of this automatic growing and shrinking depends upon
proper subnet configuration.

The cephadm bootstrap procedure assigns the first monitor daemon in the
cluster to a particular subnet. ``cephadm`` designates that subnet as the
default subnet of the cluster. New monitor daemons will be assigned by
default to that subnet unless cephadm is instructed to do otherwise. 

If all of the ceph monitor daemons in your cluster are in the same subnet,
manual administration of the ceph monitor daemons is not necessary.
``cephadm`` will automatically add up to five monitors to the subnet, as
needed, as new hosts are added to the cluster.

By default, cephadm will deploy 5 daemons on arbitrary hosts. See
:ref:`orchestrator-cli-placement-spec` for details of specifying
the placement of daemons.

Designating a Particular Subnet for Monitors
--------------------------------------------

To designate a particular IP subnet for use by ceph monitor daemons, use a
command of the following form, including the subnet's address in `CIDR`_
format (e.g., ``10.1.2.0/24``):

  .. prompt:: bash #

     ceph config set mon public_network *<mon-cidr-network>*

  For example:

  .. prompt:: bash #

     ceph config set mon public_network 10.1.2.0/24

Cephadm deploys new monitor daemons only on hosts that have IP addresses in
the designated subnet.

You can also specify two public networks by using a list of networks:

  .. prompt:: bash #

     ceph config set mon public_network *<mon-cidr-network1>,<mon-cidr-network2>*

  For example:

  .. prompt:: bash #

     ceph config set mon public_network 10.1.2.0/24,192.168.0.1/24


Deploying Monitors on a Particular Network 
------------------------------------------

You can explicitly specify the IP address or CIDR network for each monitor and
control where each monitor is placed.  To disable automated monitor deployment,
run this command:

  .. prompt:: bash #

    ceph orch apply mon --unmanaged

  To deploy each additional monitor:

  .. prompt:: bash #

    ceph orch daemon add mon *<host1:ip-or-network1>

  For example, to deploy a second monitor on ``newhost1`` using an IP
  address ``10.1.2.123`` and a third monitor on ``newhost2`` in
  network ``10.1.2.0/24``, run the following commands:

  .. prompt:: bash #

    ceph orch apply mon --unmanaged
    ceph orch daemon add mon newhost1:10.1.2.123
    ceph orch daemon add mon newhost2:10.1.2.0/24

  Now, enable automatic placement of Daemons

  .. prompt:: bash #

    ceph orch apply mon --placement="newhost1,newhost2,newhost3" --dry-run

  See :ref:`orchestrator-cli-placement-spec` for details of specifying
  the placement of daemons.

  Finally apply this new placement by dropping ``--dry-run``

  .. prompt:: bash #

    ceph orch apply mon --placement="newhost1,newhost2,newhost3"


Moving Monitors to a Different Network
--------------------------------------

To move Monitors to a new network, deploy new monitors on the new network and
subsequently remove monitors from the old network. It is not advised to
modify and inject the ``monmap`` manually.

First, disable the automated placement of daemons:

  .. prompt:: bash #

    ceph orch apply mon --unmanaged

To deploy each additional monitor:

  .. prompt:: bash #

    ceph orch daemon add mon *<newhost1:ip-or-network1>*

For example, to deploy a second monitor on ``newhost1`` using an IP
address ``10.1.2.123`` and a third monitor on ``newhost2`` in
network ``10.1.2.0/24``, run the following commands:

  .. prompt:: bash #

    ceph orch apply mon --unmanaged
    ceph orch daemon add mon newhost1:10.1.2.123
    ceph orch daemon add mon newhost2:10.1.2.0/24

  Subsequently remove monitors from the old network:

  .. prompt:: bash #

    ceph orch daemon rm *mon.<oldhost1>*

  Update the ``public_network``:

  .. prompt:: bash #

     ceph config set mon public_network *<mon-cidr-network>*

  For example:

  .. prompt:: bash #

     ceph config set mon public_network 10.1.2.0/24

  Now, enable automatic placement of Daemons

  .. prompt:: bash #

    ceph orch apply mon --placement="newhost1,newhost2,newhost3" --dry-run

  See :ref:`orchestrator-cli-placement-spec` for details of specifying
  the placement of daemons.

  Finally apply this new placement by dropping ``--dry-run``

  .. prompt:: bash #

    ceph orch apply mon --placement="newhost1,newhost2,newhost3" 

Further Reading
===============

* :ref:`rados-operations`
* :ref:`rados-troubleshooting-mon`
* :ref:`cephadm-restore-quorum`

