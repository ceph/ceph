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
