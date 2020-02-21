.. _cephadm:

====================
cephadm orchestrator
====================

The cephadm orchestrator is an orchestrator module that does not rely
on a separate system such as Rook or Ansible, but rather manages nodes
in a cluster by establishing an SSH connection and issuing explicit
management commands.

Orchestrator modules only provide services to other modules, which in turn
provide user interfaces.  To try out the cephadm module, you might like
to use the :ref:`Orchestrator CLI <orchestrator-cli-module>` module.

Requirements
============

- Python 3
- Podman or Docker
- LVM2

Configuration
=============

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
=============

CEPHADM_STRAY_HOST
------------------

One or more hosts have running Ceph daemons but are not registered as
hosts managed by *cephadm*.  This means that those services cannot
currently be managed by cephadm (e.g., restarted, upgraded, included
in `ceph orch ps`).

You can manage the host(s) with::

  ceph orch host add *<hostname>*

Note that you may need to configure SSH access to the remote host
before this will work.

Alternatively, you can manually connect to the host and ensure that
services on that host are removed and/or migrated to a host that is
managed by *cephadm*.

You can also disable this warning entirely with::

  ceph config set mgr mgr/cephadm/warn_on_stray_hosts false

CEPHADM_STRAY_DAEMON
--------------------

One or more Ceph daemons are running but not are not managed by
*cephadm*, perhaps because they were deploy using a different tool, or
were started manually.  This means that those services cannot
currently be managed by cephadm (e.g., restarted, upgraded, included
in `ceph orch ps`).

**FIXME:** We need to implement and document an adopt procedure here.

You can also disable this warning entirely with::

  ceph config set mgr mgr/cephadm/warn_on_stray_daemons false

CEPHADM_HOST_CHECK_FAILED
-------------------------

One or more hosts have failed the basic cephadm host check, which verifies
that (1) the host is reachable and cephadm can be executed there, and (2)
that the host satisfies basic prerequisites, like a working container
runtime (podman or docker) and working time synchronization.
If this test fails, cephadm will no be able to manage services on that host.

You can manually run this check with::

  ceph cephadm check-host *<hostname>*

You can remove a broken host from management with::

  ceph orch host rm *<hostname>*

You can disable this health warning with::

  ceph config set mgr mgr/cephadm/warn_on_failed_host_check false
