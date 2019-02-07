
.. _ansible-module:

====================
Ansible Orchestrator
====================

This module is a :ref:`Ceph orchestrator <orchestrator-modules>` module that uses `Ansible Runner Service <https://github.com/pcuzner/ansible-runner-service>`_ (a RESTful API server) to execute Ansible playbooks in order to satisfy the different operations supported.

These operations basically (and for the moment) are:

- Get an inventory of the Ceph cluster nodes and all the storage devices present in each node
- ...
- ...


Usage
=====

Enable the module:

::

    # ceph mgr module enable ansible

Disable the module

::

    # ceph mgr module disable ansible


Enable the Ansible orchestrator module and use it with the :ref:`CLI <orchestrator-cli-module>`:

::

    ceph mgr module enable ansible
    ceph orchestrator set backend ansible


Configuration
=============

Configuration must be set once the module is enabled by first time.

This can be done in one monitor node via the configuration key facility on a
cluster-wide level (so they apply to all manager instances) as follows::


    # ceph config set mgr mgr/ansible/server_addr <ip_address/server_name>
    # ceph config set mgr mgr/ansible/server_port <port>
    # ceph config set mgr mgr/ansible/username <username>
    # ceph config set mgr mgr/ansible/password <password>
    # ceph config set mgr mgr/ansible/verify_server <verify_server_value>

Where:

    * <ip_address/server_name>: Is the ip address/hostname of the server where the Ansible Runner Service is available.
    * <port>: The port number where the Ansible Runner Service is listening
    * <username>: The username of one authorized user in the Ansible Runner Service
    * <password>: The password of the authorized user.
    * <verify_server_value>: Either a boolean, in which case it controls whether the server's TLS certificate is verified, or a string, in which case it must be a path to a CA bundle to use in the verification. Defaults to ``True``.


Debugging
=========

Any kind of incident with this orchestrator module can be debugged using the Ceph manager logs:

Set the right log level in order to debug properly. Remember that the python log levels debug, info, warn, err are mapped into the Ceph severities 20, 4, 1 and 0 respectively.

And use the "active" manager node: ( "ceph -s" command in one monitor give you this information)

* Check current debug level::

    [@mgr0 ~]# ceph daemon mgr.mgr0 config show | grep debug_mgr
    "debug_mgr": "1/5",
    "debug_mgrc": "1/5",

* Change the log level to "debug"::

    [mgr0 ~]# ceph daemon mgr.mgr0 config set debug_mgr 20/5
    {
        "success": ""
    }

* Restore "info" log level::

    [mgr0 ~]# ceph daemon mgr.mgr0 config set debug_mgr 1/5
    {
        "success": ""
    }


Operations
==========

**Inventory:**

Get the list of storage devices installed in all the cluster nodes. The output format is::

  [host:
     device_name (type_of_device , size_in_bytes)]

Example::

  [root@mon0 ~]# ceph orchestrator device ls
  192.168.121.160:
    vda (hdd, 44023414784b)
    sda (hdd, 53687091200b)
    sdb (hdd, 53687091200b)
    sdc (hdd, 53687091200b)
  192.168.121.36:
    vda (hdd, 44023414784b)
  192.168.121.201:
    vda (hdd, 44023414784b)
  192.168.121.70:
    vda (hdd, 44023414784b)
    sda (hdd, 53687091200b)
    sdb (hdd, 53687091200b)
    sdc (hdd, 53687091200b)
