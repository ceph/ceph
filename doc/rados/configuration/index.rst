===============
 Configuration
===============

Each Ceph process, daemon, or utility draws its configuration from several
sources on startup. Such sources can include (1) a local configuration, (2) the
monitors, (3) the command line, and (4) environment variables.

Configuration options can be set globally so that they apply (1) to all
daemons, (2) to all daemons or services of a particular type, or (3) to only a
specific daemon, process, or client.

.. raw:: html

	<table cellpadding="10"><colgroup><col width="50%"><col width="50%"></colgroup><tbody valign="top"><tr><td><h3>Configuring the Object Store</h3>

For general object store configuration, refer to the following:

.. toctree::
   :maxdepth: 1

   Storage devices <storage-devices>
   ceph-conf


.. raw:: html 

	</td><td><h3>Reference</h3>

To optimize the performance of your cluster, refer to the following:

.. toctree::
   :maxdepth: 1

   Common Settings <common>
   Network Settings <network-config-ref>
   Messenger v2 protocol <msgr2>
   Auth Settings <auth-config-ref>
   Monitor Settings <mon-config-ref>
   mon-lookup-dns
   Heartbeat Settings <mon-osd-interaction>
   OSD Settings <osd-config-ref>
   DmClock Settings <mclock-config-ref>
   BlueStore Settings <bluestore-config-ref>
   FileStore Settings <filestore-config-ref>
   Journal Settings <journal-ref>
   Pool, PG & CRUSH Settings <pool-pg-config-ref.rst>
   General Settings <general-config-ref>

   
.. raw:: html

	</td></tr></tbody></table>
