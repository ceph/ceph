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

     ceph orch host add *<newhost>* [*<ip>*] [*<label1> ...*]

   For example:

   .. prompt:: bash #

     ceph orch host add host2 10.10.0.102
     ceph orch host add host3 10.10.0.103

   It is best to explicitly provide the host IP address.  If an IP is
   not provided, then the host name will be immediately resolved via
   DNS and that IP will be used.

   One or more labels can also be included to immediately label the
   new host.  For example, by default the ``_admin`` label will make
   cephadm maintain a copy of the ``ceph.conf`` file and a
   ``client.admin`` keyring file in ``/etc/ceph``:

   .. prompt:: bash #

       ceph orch host add host4 10.10.0.104 --labels _admin

.. _cephadm-removing-hosts:

Removing Hosts
==============

A host can safely be removed from a the cluster once all daemons are removed from it.

To drain all daemons from a host do the following:

.. prompt:: bash #

  ceph orch host drain *<host>*

The '_no_schedule' label will be applied to the host. See :ref:`cephadm-special-host-labels`

All osds on the host will be scheduled to be removed. You can check osd removal progress with the following:

.. prompt:: bash #

  ceph orch osd rm status

see :ref:`cephadm-osd-removal` for more details about osd removal

You can check if there are no deamons left on the host with the following:

.. prompt:: bash #

  ceph orch ps <host> 

Once all daemons are removed you can remove the host with the following:

.. prompt:: bash #

  ceph orch host rm <host>

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


.. _cephadm-special-host-labels:

Special host labels
-------------------

The following host labels have a special meaning to cephadm.  All start with ``_``.

* ``_no_schedule``: *Do not schedule or deploy daemons on this host*.

  This label prevents cephadm from deploying daemons on this host.  If it is added to
  an existing host that already contains Ceph daemons, it will cause cephadm to move
  those daemons elsewhere (except OSDs, which are not removed automatically).

* ``_no_autotune_memory``: *Do not autotune memory on this host*.

  This label will prevent daemon memory from being tuned even when the
  ``osd_memory_target_autotune`` or similar option is enabled for one or more daemons
  on that host.

* ``_admin``: *Distribute client.admin and ceph.conf to this host*.

  By default, an ``_admin`` label is applied to the first host in the cluster (where
  bootstrap was originally run), and the ``client.admin`` key is set to be distributed
  to that host via the ``ceph orch client-keyring ...`` function.  Adding this label
  to additional hosts will normally cause cephadm to deploy config and keyring files
  in ``/etc/ceph``.

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
    hostname: node-00
    addr: 192.168.0.10
    labels:
    - example1
    - example2
    ---
    service_type: host
    hostname: node-01
    addr: 192.168.0.11
    labels:
    - grafana
    ---
    service_type: host
    hostname: node-02
    addr: 192.168.0.12

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

.. note::

  cephadm demands that the name of the host given via ``ceph orch host add`` 
  equals the output of ``hostname`` on remote hosts.

Otherwise cephadm can't be sure that names returned by
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
