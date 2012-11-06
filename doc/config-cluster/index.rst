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

- ``ceph-osd`` (mandatory)
- ``ceph-mon`` (mandatory)
- ``ceph-mds`` (mandatory for cephfs only)

Each process, daemon or utility loads the host's configuration file. A process
may have information about more than one daemon instance (*i.e.,* multiple
contexts). A daemon or utility only has information about a single daemon
instance (a single context).

.. note:: Ceph can run on a single host for evaluation purposes.


.. raw:: html

	<table cellpadding="10"><colgroup><col width="50%"><col width="50%"></colgroup><tbody valign="top"><tr><td><h3>Cluster Configuration</h3>

For general cluster configuration, refer to the following:

.. toctree::

   Disks and Filesystems <file-system-recommendations>
   ceph-conf


.. raw:: html 

	</td><td><h3>Configuration Reference</h3>

To optimize the performance of your cluster, refer to the following:

.. toctree::

	General Settings <general-config-ref>
	Monitor Settings <mon-config-ref>
	OSD Settings <osd-config-ref>
	Filestore Settings <filestore-config-ref>
	Journal Settings <journal-ref>
	Metadata Server Settings <mds-config-ref>
	librbd Cache Settings <rbd-config-ref>
	Log / Debug Settings <log-and-debug-ref>	


.. raw:: html

	</td></tr><tr><td><h3>Manual Deployment</h3>

To deploy a cluster manually (this is recommended for testing and development only), refer to the following:

.. toctree:: 

   Deploy with mkcephfs <mkcephfs>


.. raw:: html

	</td><td><h3>Chef Deployment</h3>

To deploy a cluster with chef, refer to the following: 

.. toctree:: 

   Deploy with Chef <chef>
   
.. raw:: html

	</td></tr></tbody></table>
