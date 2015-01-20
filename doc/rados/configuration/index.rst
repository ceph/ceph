===============
 Configuration
===============

Ceph can run with a cluster containing thousands of Object Storage Devices
(OSDs). A minimal system will have at least two OSDs for data replication. To
configure OSD clusters, you must provide settings in the configuration file.
Ceph provides default values for many settings, which you can override in the
configuration file. Additionally, you can make runtime modification to the
configuration using command-line utilities.

When Ceph starts, it activates three daemons:

- ``ceph-mon`` (mandatory)
- ``ceph-osd`` (mandatory)
- ``ceph-mds`` (mandatory for cephfs only)

Each process, daemon or utility loads the host's configuration file. A process
may have information about more than one daemon instance (*i.e.,* multiple
contexts). A daemon or utility only has information about a single daemon
instance (a single context).

.. note:: Ceph can run on a single host for evaluation purposes.


.. raw:: html

	<table cellpadding="10"><colgroup><col width="50%"><col width="50%"></colgroup><tbody valign="top"><tr><td><h3>Configuring the Object Store</h3>

For general object store configuration, refer to the following:

.. toctree::
   :maxdepth: 1

   Disks and Filesystems <filesystem-recommendations>
   ceph-conf


.. raw:: html 

	</td><td><h3>Reference</h3>

To optimize the performance of your cluster, refer to the following:

.. toctree::
	:maxdepth: 1

	Network Settings <network-config-ref>
	Auth Settings <auth-config-ref>
	Monitor Settings <mon-config-ref>
	Heartbeat Settings <mon-osd-interaction>
	OSD Settings <osd-config-ref>
	Filestore Settings <filestore-config-ref>
	Key/Value Store Settings <keyvaluestore-config-ref>
	Journal Settings <journal-ref>
	Pool, PG & CRUSH Settings <pool-pg-config-ref.rst>
	Messaging Settings <ms-ref>	
	General Settings <general-config-ref>

   
.. raw:: html

	</td></tr></tbody></table>
