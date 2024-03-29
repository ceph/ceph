Zabbix Module
=============

The Zabbix module actively sends information to a Zabbix server like:

- Ceph status
- I/O operations
- I/O bandwidth
- OSD status
- Storage utilization

Requirements
------------

The module requires that the *zabbix_sender* executable is present on *all*
machines running ceph-mgr. It can be installed on most distributions using
the package manager.

Dependencies
^^^^^^^^^^^^
Installing zabbix_sender can be done under Ubuntu or CentOS using either apt
or dnf.

On Ubuntu Xenial:

::

    apt install zabbix-agent

On Fedora:

::

    dnf install zabbix-sender


Enabling
--------
You can enable the *zabbix* module with:

::

    ceph mgr module enable zabbix

Configuration
-------------

Two configuration keys are vital for the module to work:

- zabbix_host
- identifier (optional)

The parameter *zabbix_host* controls the hostname of the Zabbix server to which
*zabbix_sender* will send the items. This can be a IP-Address if required by
your installation.

The *identifier* parameter controls the identifier/hostname to use as source
when sending items to Zabbix. This should match the name of the *Host* in
your Zabbix server.

When the *identifier* parameter is not configured the ceph-<fsid> of the cluster
will be used when sending data to Zabbix.

This would for example be *ceph-c4d32a99-9e80-490f-bd3a-1d22d8a7d354*

Additional configuration keys which can be configured and their default values:

- zabbix_port: 10051
- zabbix_sender: /usr/bin/zabbix_sender
- interval: 60
- discovery_interval: 100

Configuration keys
^^^^^^^^^^^^^^^^^^^

Configuration keys can be set on any machine with the proper cephx credentials,
these are usually Monitors where the *client.admin* key is present.

::

    ceph zabbix config-set <key> <value>

For example:

::

    ceph zabbix config-set zabbix_host zabbix.localdomain
    ceph zabbix config-set identifier ceph.eu-ams02.local

The current configuration of the module can also be shown:

::

   ceph zabbix config-show


Template
^^^^^^^^
A `template <https://raw.githubusercontent.com/ceph/ceph/master/src/pybind/mgr/zabbix/zabbix_template.xml>`_. 
(XML) to be used on the Zabbix server can be found in the source directory of the module.

This template contains all items and a few triggers. You can customize the triggers afterwards to fit your needs.


Multiple Zabbix servers
^^^^^^^^^^^^^^^^^^^^^^^
It is possible to instruct zabbix module to send data to multiple Zabbix servers.

Parameter *zabbix_host* can be set with multiple hostnames separated by commas.
Hostnames (or IP addresses) can be followed by colon and port number. If a port
number is not present module will use the port number defined in *zabbix_port*.

For example:

::

    ceph zabbix config-set zabbix_host "zabbix1,zabbix2:2222,zabbix3:3333"


Manually sending data
---------------------
If needed the module can be asked to send data immediately instead of waiting for
the interval.

This can be done with this command:

::

    ceph zabbix send

The module will now send its latest data to the Zabbix server.

Items discovery is accomplished also via zabbix_sender, and runs every `discovery_interval * interval` seconds. If you wish to launch discovery 
manually, this can be done with this command:

::

    ceph zabbix discovery


Debugging
---------

Should you want to debug the Zabbix module increase the logging level for
ceph-mgr and check the logs.

::

    [mgr]
        debug mgr = 20

With logging set to debug for the manager the module will print various logging
lines prefixed with *mgr[zabbix]* for easy filtering.

Installing zabbix-agent 2
-------------------------

Follow the instructions in the sections :ref:`mgr_zabbix_2_nodes`,
:ref:`mgr_zabbix_2_cluster`, and :ref:`mgr_zabbix_2_server` to install a Zabbix
server to monitor your Ceph cluster.

.. _mgr_zabbix_2_nodes:

Ceph MGR Nodes
^^^^^^^^^^^^^^

#. Download an appropriate Zabbix release from https://www.zabbix.com/download
   or install a package from the Zabbix repositories.
#. Use your package manager to remove any other Zabbix agents.
#. Install ``zabbix-agent 2`` using the instructions at
   https://www.zabbix.com/download.
#. Edit ``/etc/zabbix/zabbix-agent2.conf``. Add your Zabbix monitoring servers
   and your localhost to the ``Servers`` line of ``zabbix-agent2.conf``::

     Server=127.0.0.1,zabbix2.example.com,zabbix1.example.com
#. Start or restart the ``zabbix-agent2`` agent:

   .. prompt:: bash #

      systemctl restart zabbix-agent2

.. _mgr_zabbix_2_cluster:

Ceph Cluster
^^^^^^^^^^^^

#. Enable the ``restful`` module:

   .. prompt:: bash #

      ceph mgr module enable restful

#. Generate a self-signed certificate. This step is optional: 

   .. prompt:: bash #

      restful create-self-signed-cert

#. Create an API user called ``zabbix-monitor``:
   
   .. prompt:: bash #

      ceph restful create-key zabbix-monitor

   The output of this command, an API key, will look something like this::

      a4bb2019-XXXX-YYYY-ZZZZ-abcdefghij

#. Save the generated API key. It will be necessary later. 
#. Test API access by using ``zabbix-get``:

   .. note:: This step is optional. 


   .. prompt:: bash #

      zabbix_get -s 127.0.0.1 -k ceph.ping["${CEPH.CONNSTRING}","${CEPH.USER}","{CEPH.API.KEY}"

   Example:

   .. prompt:: bash #

      zabbix_get -s 127.0.0.1 -k ceph.ping["https://localhost:8003","zabbix-monitor","a4bb2019-XXXX-YYYY-ZZZZ-abcdefghij"]

   .. note:: You may need to install ``zabbix-get`` via your package manager. 

.. _mgr_zabbix_2_server:

Zabbix Server
^^^^^^^^^^^^^

#. Create a host for the Ceph monitoring servers.
#. Add the template ``Ceph by Zabbix agent 2`` to the host.
#. Inform the host of the keys:

   #. Go to “Macros” on the host. 
   #. Show “Inherited and host macros”. 
   #. Change ``${CEPH.API.KEY}`` and ``${CEPH.USER}`` to the values provided
      under ``ceph restful create-key``, above. Example:: 
   
        {$CEPH.API.KEY} a4bb2019-XXXX-YYYY-ZZZZ-abcdefghij
        {$CEPH.USER} zabbix-monitor

#. Update the host. Within a few cycles, data will populate the server.
