
.. _ansible-module:

====================
Ansible Orchestrator
====================

This module is a :ref:`Ceph orchestrator <orchestrator-modules>` module that uses `Ansible Runner Service <https://github.com/ansible/ansible-runner-service>`_ (a RESTful API server) to execute Ansible playbooks in order to satisfy the different operations supported.

These operations basically (and for the moment) are:

- Get an inventory of the Ceph cluster nodes and all the storage devices present in each node
- Hosts management
- Create/remove OSD's
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

The external Ansible Runner Service uses TLS mutual authentication to allow clients to use the API.
A client certificate and a key file should be provided by the Administrator of the Ansible Runner Service for each manager node.
These files should be copied in each of the manager nodes with read access for the ceph user.
The destination folder for this files and the name of the files must be the same always in all the manager nodes,
although the certificate/key content of these files logically will be different in each node.

Configuration must be set when the module is enabled for the first time.

This can be done in one monitor node via the configuration key facility on a
cluster-wide level (so they apply to all manager instances) as follows:

In first place, configure the Ansible Runner Service client certificate and key:

::

    If the provided client certificate is usable for all servers, apply it using:
    # ceph ansible set-ssl-certificate -i <location_of_the_crt_file>
    # ceph ansible set-ssl-certificate-key -i <location_of_the_key_file>


::

    If the client certificate provided is for an especific manager server use:
    # ceph ansible set-ssl-certificate <server> -i <location_of_the_crt_file>
    # ceph ansible set-ssl-certificate-key <server> -i <location_of_the_key_file>



After setting the client certificate and key files, finish the configuration as follows:

::

    # ceph config set mgr mgr/ansible/server_location <ip_address/server_name>:<port>
    # ceph config set mgr mgr/ansible/verify_server <False|True>
    # ceph config set mgr mgr/ansible/ca_bundle <path_to_ca_bundle_file>



Where:

    * <ip_address/server_name>: Is the ip address/hostname of the server where the Ansible Runner Service is available.
    * <port>: The port number where the Ansible Runner Service is listening
    * <verify_server_value>: boolean, it controls whether the Ansible Runner Service server's TLS certificate is verified. Defaults to ``True``.
    * <path_to_ca_bundle_file>: Path to a CA bundle to use in the verification.

In order to check that everything is OK, use the "status" orchestrator command.

    # ceph orchestrator status
    Backend: ansible
    Available: True

Any kind of problem connecting with the external Ansible Runner Service will be reported using this command.


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

To see the complete list of operations, use:
:ref:`CLI <orchestrator-cli-module>`
