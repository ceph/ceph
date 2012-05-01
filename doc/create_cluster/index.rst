==========================
Creating a Storage Cluster
==========================
Ceph can run with a cluster containing thousands of Object Storage Devices (OSDs). 
A minimal system will have at least two OSDs for data replication. To configure OSD clusters, you must 
provide settings in the configuration file. Ceph provides default values for many settings, which you can 
override in the configuration file. Additionally, you can make runtime modification to the configuration 
using command-line utilities.

When Ceph starts, it activates three daemons: 

- ``ceph-osd`` (mandatory)
- ``ceph-mon`` (mandatory)
- ``ceph-mds`` (mandatory for cephfs only)

Each process, daemon or utility loads the host's configuration file. A process may have information about 
more than one daemon instance (*i.e.,* multiple contexts). A daemon or utility only has information 
about a single daemon instance (a single context).

.. note:: Ceph can run on a single host for evaluation purposes.

- :doc:`Ceph Configuration Files <ceph_conf>`
	- :doc:`OSD Configuration Settings <osd_configuration_settings>`
	- :doc:`Monitor Configuration Settings <mon_configuration_settings>`
	- :doc:`Metadata Server Configuration Settings <mds_configuration_settings>`
- :doc:`Deploying the Ceph Configuration <deploying_ceph_conf>`
- :doc:`Deploying Ceph with mkcephfs <deploying_ceph_with_mkcephfs>`
- :doc:`Deploying Ceph with Chef (coming soon) <deploying_with_chef>`

.. toctree::
   :hidden:

   Configuration <ceph_conf>
   [osd] Settings <osd_configuration_settings>
   [mon] Settings <mon_configuration_settings>
   [mds] Settings <mds_configuration_settings>
   Deploy Config <deploying_ceph_conf>
   deploying_ceph_with_mkcephfs
   Chef Coming Soon! <deploying_with_chef>