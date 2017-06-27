Zabbix plugin
=============

The Zabbix plugin actively sends information to a Zabbix server like:

- Ceph status
- I/O operations
- I/O bandwidth
- OSD status
- Storage utilization

Requirements
============

The plugin requires that the *zabbix_sender* executable is present on *all*
machines running ceph-mgr. It can be installed on most distributions using
the package manager.

Dependencies
------------
Installing zabbix_sender can be done under Ubuntu or CentOS using either apt
or dnf.

On Ubuntu Xenial:

::

    apt install zabbix-agent

On Fedora:

::

    dnf install zabbix-sender


Enabling
========

Add this to your ceph.conf on nodes where you run ceph-mgr:

::

    [mgr]
        mgr modules = zabbix

If you use any other ceph-mgr modules, make sure they're in the list too.

Restart the ceph-mgr daemon after modifying the setting to load the module.


Configuration
=============

Two configuration keys are mandatory for the module to work:

- mgr/zabbix/zabbix_host
- mgr/zabbix/identifier

The parameter *zabbix_host* controls the hostname of the Zabbix server to which
*zabbix_sender* will send the items. This can be a IP-Address if required by
your installation.

The *identifier* parameter controls the identifier/hostname to use as source
when sending items to Zabbix. This should match the name of the *Host* in
your Zabbix server.

Additional configuration keys which can be configured and their default values:

- mgr/zabbix/zabbix_port: 10051
- mgr/zabbix/zabbix_sender: /usr/bin/zabbix_sender
- mgr/zabbix/interval: 60

Configurations keys
-------------------

Configuration keys can be set on any machine with the proper cephx credentials,
these are usually Monitors where the *client.admin* key is present.

::

    ceph config-key put <key> <value>

For example:

::

    ceph config-key put mgr/zabbix/zabbix_host zabbix.localdomain
    ceph config-key put mgr/zabbix/identifier ceph.eu-ams02.local

Debugging
=========

Should you want to debug the Zabbix module increase the logging level for
ceph-mgr and check the logs.

::

    [mgr]
        debug mgr = 20

With logging set to debug for the manager the plugin will print various logging
lines prefixed with *mgr[zabbix]* for easy filtering.

