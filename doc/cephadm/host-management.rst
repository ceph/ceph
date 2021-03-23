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

Hosts must have these :ref:`cephadm-host-requirements` installed.
Hosts without all the necessary requirements will fail to be added to the cluster.

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

.. _orchestrator-host-labels:

Host labels
===========

The orchestrator supports assigning labels to hosts. Labels
are free form and have no particular meaning by itself and each host
can have multiple labels. They can be used to specify placement
of daemons. See :ref:`orch-placement-by-labels`

Labels can be added when adding a host with the ``--labels`` flag::

  ceph orch host add my_hostname --labels=my_label1
  ceph orch host add my_hostname --labels=my_label1,my_label2

To add a label a existing host, run::

  ceph orch host label add my_hostname my_label

To remove a label, run::

  ceph orch host label rm my_hostname my_label

  
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

SSH Configuration
=================

Cephadm uses SSH to connect to remote hosts.  SSH uses a key to authenticate
with those hosts in a secure way.


Default behavior
----------------

Cephadm stores an SSH key in the monitor that is used to
connect to remote hosts.  When the cluster is bootstrapped, this SSH
key is generated automatically and no additional configuration
is necessary.

A *new* SSH key can be generated with::

  ceph cephadm generate-key

The public portion of the SSH key can be retrieved with::

  ceph cephadm get-pub-key

The currently stored SSH key can be deleted with::

  ceph cephadm clear-key

You can make use of an existing key by directly importing it with::

  ceph config-key set mgr/cephadm/ssh_identity_key -i <key>
  ceph config-key set mgr/cephadm/ssh_identity_pub -i <pub>

You will then need to restart the mgr daemon to reload the configuration with::

  ceph mgr fail

Configuring a different SSH user
----------------------------------

Cephadm must be able to log into all the Ceph cluster nodes as an user
that has enough privileges to download container images, start containers
and execute commands without prompting for a password. If you do not want
to use the "root" user (default option in cephadm), you must provide
cephadm the name of the user that is going to be used to perform all the
cephadm operations. Use the command::

  ceph cephadm set-user <user>

Prior to running this the cluster ssh key needs to be added to this users
authorized_keys file and non-root users must have passwordless sudo access.


Customizing the SSH configuration
---------------------------------

Cephadm generates an appropriate ``ssh_config`` file that is
used for connecting to remote hosts.  This configuration looks
something like this::

  Host *
  User root
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null

There are two ways to customize this configuration for your environment:

#. Import a customized configuration file that will be stored
   by the monitor with::

     ceph cephadm set-ssh-config -i <ssh_config_file>

   To remove a customized SSH config and revert back to the default behavior::

     ceph cephadm clear-ssh-config

#. You can configure a file location for the SSH configuration file with::

     ceph config set mgr mgr/cephadm/ssh_config_file <path>

   We do *not recommend* this approach.  The path name must be
   visible to *any* mgr daemon, and cephadm runs all daemons as
   containers. That means that the file either need to be placed
   inside a customized container image for your deployment, or
   manually distributed to the mgr data directory
   (``/var/lib/ceph/<cluster-fsid>/mgr.<id>`` on the host, visible at
   ``/var/lib/ceph/mgr/ceph-<id>`` from inside the container).
   
.. _cephadm-fqdn:

Fully qualified domain names vs bare host names
===============================================

cephadm has very minimal requirements when it comes to resolving host
names etc. When cephadm initiates an ssh connection to a remote host,
the host name  can be resolved in four different ways:

-  a custom ssh config resolving the name to an IP
-  via an externally maintained ``/etc/hosts``
-  via explicitly providing an IP address to cephadm: ``ceph orch host add <hostname> <IP>``
-  automatic name resolution via DNS.

Ceph itself uses the command ``hostname`` to determine the name of the
current host.

.. note::

  cephadm demands that the name of the host given via ``ceph orch host add`` 
  equals the output of ``hostname`` on remote hosts.

Otherwise cephadm can't be sure, the host names returned by
``ceph * metadata`` match the hosts known to cephadm. This might result
in a :ref:`cephadm-stray-host` warning.

When configuring new hosts, there are two **valid** ways to set the 
``hostname`` of a host:

1. Using the bare host name. In this case:

-  ``hostname`` returns the bare host name.
-  ``hostname -f`` returns the FQDN.

2. Using the fully qualified domain name as the host name. In this case:

-  ``hostname`` returns the FQDN
-  ``hostname -s`` return the bare host name

Note that ``man hostname`` recommends ``hostname`` to return the bare
host name:

    The FQDN (Fully Qualified Domain Name) of the system is the
    name that the resolver(3) returns for the host name, such as,
    ursula.example.com. It is usually the hostname followed by the DNS
    domain name (the part after the first dot). You can check the FQDN
    using ``hostname --fqdn`` or the domain name using ``dnsdomainname``.

    .. code-block:: none

          You cannot change the FQDN with hostname or dnsdomainname.

          The recommended method of setting the FQDN is to make the hostname
          be an alias for the fully qualified name using /etc/hosts, DNS, or
          NIS. For example, if the hostname was "ursula", one might have
          a line in /etc/hosts which reads

                 127.0.1.1    ursula.example.com ursula

Which means, ``man hostname`` recommends ``hostname`` to return the bare
host name. This in turn means that Ceph will return the bare host names
when executing ``ceph * metadata``. This in turn means cephadm also
requires the bare host name when adding a host to the cluster: 
``ceph orch host add <bare-name>``.

..
  TODO: This chapter needs to provide way for users to configure
  Grafana in the dashboard, as this is right no very hard to do.
