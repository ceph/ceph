====================
cephadm orchestrator
====================

The cephadm orchestrator is an orchestrator module that does not rely on a separate
system such as Rook or Ansible, but rather manages nodes in a cluster by
establishing an SSH connection and issuing explicit management commands.

Orchestrator modules only provide services to other modules, which in turn
provide user interfaces.  To try out the cephadm module, you might like
to use the :ref:`Orchestrator CLI <orchestrator-cli-module>` module.

Requirements
------------

- The Python `remoto` library version 0.35 or newer

Configuration
-------------

The cephadm orchestrator can be configured to use an SSH configuration file. This is
useful for specifying private keys and other SSH connection options.

::

    # ceph config set mgr mgr/cephadm/ssh_config_file /path/to/config

An SSH configuration file can be provided without requiring an accessible file
system path as the method above does.

::

    # ceph cephadm set-ssh-config -i /path/to/config

To clear this value use the command:

::

    # ceph cephadm clear-ssh-config

Health checks
-------------

CEPHADM_STRAY_HOST
^^^^^^^^^^^^^^^^^^

One or more hosts have running Ceph daemons but are not registered as
hosts managed by *cephadm*.  This means that those services cannot
currently be managed by cephadm (e.g., restarted, upgraded, included
in `ceph orchestrator service ls`).

You can manage the host(s) with::

  ceph orchestrator host add *<hostname>*

Note that you may need to configure SSH access to the remote host
before this will work.

Alternatively, you can manually connect to the host and ensure that
services on that host are removed and/or migrated to a host that is
managed by *cephadm*.

You can also disable this warning entirely with::

  ceph config set mgr mgr/cephadm/warn_on_stray_hosts false

CEPHADM_STRAY_SERVICE
^^^^^^^^^^^^^^^^^^^^^

One or more Ceph daemons are running but not are not managed by
*cephadm*, perhaps because they were deploy using a different tool, or
were started manually.  This means that those services cannot
currently be managed by cephadm (e.g., restarted, upgraded, included
in `ceph orchestrator service ls`).

**FIXME:** We need to implement and document an adopt procedure here.

You can also disable this warning entirely with::

  ceph config set mgr mgr/cephadm/warn_on_stray_services false
