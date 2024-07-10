Zabbix Module
=============

The Zabbix module has not been supported since April 2020 at the latest. The
upstream Ceph community in April 2024 developed procedures for installing
Zabbix 2.

Discussion of the decisions around the lack of support for Zabbix can be found
here: https://github.com/ceph/ceph-container/issues/1651

Installing zabbix-agent 2
-------------------------

*The procedures that explain the installation of Zabbix 2 were developed by John Jasen.*

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
