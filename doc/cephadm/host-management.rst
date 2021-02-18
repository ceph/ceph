.. _orchestrator-cli-host-management:

===============
Host Management
===============

To list hosts associated with the cluster:

.. prompt:: bash #

    ceph orch host ls [--format yaml]

.. _cephadm-adding-hosts:    
    
Adding Hosts
============

To add each new host to the cluster, perform two steps:

#. Install the cluster's public SSH key in the new host's root user's ``authorized_keys`` file:

   .. prompt:: bash #

    ssh-copy-id -f -i /etc/ceph/ceph.pub root@*<new-host>*

   For example:

   .. prompt:: bash #

      ssh-copy-id -f -i /etc/ceph/ceph.pub root@host2
      ssh-copy-id -f -i /etc/ceph/ceph.pub root@host3

#. Tell Ceph that the new node is part of the cluster:

   .. prompt:: bash #

     ceph orch host add *newhost*

   For example:

   .. prompt:: bash #

     ceph orch host add host2
     ceph orch host add host3
     
.. _cephadm-removing-hosts:

Removing Hosts
==============

If the node that want you to remove is running OSDs, make sure you remove the OSDs from the node.

To remove a host from a cluster, do the following:

For all Ceph service types, except for ``node-exporter`` and ``crash``, remove
the host from the placement specification file (for example, cluster.yml).
For example, if you are removing the host named host2, remove all occurrences of
``- host2`` from all ``placement:`` sections.

Update:

.. code-block:: yaml

  service_type: rgw
  placement:
    hosts:
    - host1
    - host2

To:

.. code-block:: yaml


  service_type: rgw
  placement:
    hosts:
    - host1

Remove the host from cephadm's environment:

.. prompt:: bash #

  ceph orch host rm host2


If the host is running ``node-exporter`` and crash services, remove them by running
the following command on the host:

.. prompt:: bash #

  cephadm rm-daemon --fsid CLUSTER_ID --name SERVICE_NAME

  
Maintenance Mode
================

Place a host in and out of maintenance mode (stops all Ceph daemons on host)::

    ceph orch host maintenance enter <hostname> [--force]
    ceph orch host maintenace exit <hostname>

Where the force flag when entering maintenance allows the user to bypass warnings (but not alerts)

See also :ref:`cephadm-fqdn`

Host Specification
==================

Many hosts can be added at once using
``ceph orch apply -i`` by submitting a multi-document YAML file::

    ---
    service_type: host
    addr: node-00
    hostname: node-00
    labels:
    - example1
    - example2
    ---
    service_type: host
    addr: node-01
    hostname: node-01
    labels:
    - grafana
    ---
    service_type: host
    addr: node-02
    hostname: node-02

This can be combined with service specifications (below) to create a cluster spec
file to deploy a whole cluster in one command.  see ``cephadm bootstrap --apply-spec``
also to do this during bootstrap. Cluster SSH Keys must be copied to hosts prior to adding them.
