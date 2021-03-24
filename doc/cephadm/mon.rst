===========
MON Service
===========

.. _deploy_additional_monitors:

Deploying additional monitors 
-----------------------------

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

Changing the number of monitors from the default
------------------------------------------------

If you want to adjust the default of 5 monitors, run this command:

  .. prompt:: bash #

     ceph orch apply mon *<number-of-monitors>*

Deploying monitors only to specific hosts
-----------------------------------------

To deploy monitors on a specific set of hosts, run this command:

  .. prompt:: bash #

    ceph orch apply mon *<host1,host2,host3,...>*

  Be sure to include the first (bootstrap) host in this list.

Using Host Labels
-----------------

You can control which hosts the monitors run on by making use of host labels.
To set the ``mon`` label to the appropriate hosts, run this command:
  
  .. prompt:: bash #

    ceph orch host label add *<hostname>* mon

  To view the current hosts and labels, run this command:

  .. prompt:: bash #

    ceph orch host ls

  For example:

  .. prompt:: bash #

    ceph orch host label add host1 mon
    ceph orch host label add host2 mon
    ceph orch host label add host3 mon
    ceph orch host ls

  .. code-block:: bash

    HOST   ADDR   LABELS  STATUS
    host1         mon
    host2         mon
    host3         mon
    host4
    host5

  Tell cephadm to deploy monitors based on the label by running this command:

  .. prompt:: bash #

    ceph orch apply mon label:mon

See also :ref:`host labels <orchestrator-host-labels>`.

Deploying Monitors on a Particular Network 
------------------------------------------

You can explicitly specify the IP address or CIDR network for each monitor and
control where each monitor is placed.  To disable automated monitor deployment,
run this command:

  .. prompt:: bash #

    ceph orch apply mon --unmanaged

  To deploy each additional monitor:

  .. prompt:: bash #

    ceph orch daemon add mon *<host1:ip-or-network1> [<host1:ip-or-network-2>...]*

  For example, to deploy a second monitor on ``newhost1`` using an IP
  address ``10.1.2.123`` and a third monitor on ``newhost2`` in
  network ``10.1.2.0/24``, run the following commands:

  .. prompt:: bash #

    ceph orch apply mon --unmanaged
    ceph orch daemon add mon newhost1:10.1.2.123
    ceph orch daemon add mon newhost2:10.1.2.0/24

  .. note::
     The **apply** command can be confusing. For this reason, we recommend using
     YAML specifications. 

     Each ``ceph orch apply mon`` command supersedes the one before it. 
     This means that you must use the proper comma-separated list-based 
     syntax when you want to apply monitors to more than one host. 
     If you do not use the proper syntax, you will clobber your work 
     as you go.

     For example:

     .. prompt:: bash #
        
          ceph orch apply mon host1
          ceph orch apply mon host2
          ceph orch apply mon host3

     This results in only one host having a monitor applied to it: host 3.

     (The first command creates a monitor on host1. Then the second command
     clobbers the monitor on host1 and creates a monitor on host2. Then the
     third command clobbers the monitor on host2 and creates a monitor on 
     host3. In this scenario, at this point, there is a monitor ONLY on
     host3.)

     To make certain that a monitor is applied to each of these three hosts,
     run a command like this:
     
     .. prompt:: bash #
       
       ceph orch apply mon "host1,host2,host3"

     There is another way to apply monitors to multiple hosts: a ``yaml`` file
     can be used. Instead of using the "ceph orch apply mon" commands, run a
     command of this form:
     
     .. prompt:: bash #

        ceph orch apply -i file.yaml

     Here is a sample **file.yaml** file::

          service_type: mon
          placement:
            hosts:
             - host1
             - host2
             - host3
