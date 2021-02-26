===========
MON Service
===========

.. _deploy_additional_monitors:

Deploy additional monitors
==========================

A typical Ceph cluster has three or five monitor daemons spread
across different hosts.  We recommend deploying five
monitors if there are five or more nodes in your cluster.

.. _CIDR: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing#CIDR_notation

When Ceph knows what IP subnet the monitors should use it can automatically
deploy and scale monitors as the cluster grows (or contracts).  By default,
Ceph assumes that other monitors should use the same subnet as the first
monitor's IP.

If your Ceph monitors (or the entire cluster) live on a single subnet,
then by default cephadm automatically adds up to 5 monitors as you add new
hosts to the cluster. No further steps are necessary.

* If there is a specific IP subnet that should be used by monitors, you
  can configure that in `CIDR`_ format (e.g., ``10.1.2.0/24``) with:

  .. prompt:: bash #

     ceph config set mon public_network *<mon-cidr-network>*

  For example:

  .. prompt:: bash #

     ceph config set mon public_network 10.1.2.0/24

  Cephadm deploys new monitor daemons only on hosts that have IPs
  configured in the configured subnet.

* If you want to adjust the default of 5 monitors, run this command:

  .. prompt:: bash #

     ceph orch apply mon *<number-of-monitors>*

* To deploy monitors on a specific set of hosts, run this command:

  .. prompt:: bash #

    ceph orch apply mon *<host1,host2,host3,...>*

  Be sure to include the first (bootstrap) host in this list.

* You can control which hosts the monitors run on by making use of
  host labels.  To set the ``mon`` label to the appropriate
  hosts, run this command:
  
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

* You can explicitly specify the IP address or CIDR network for each monitor
  and control where it is placed.  To disable automated monitor deployment, run
  this command:

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
