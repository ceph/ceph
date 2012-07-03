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

.. toctree::

   file-system-recommendations
   Configuration <ceph-conf>
   Deploy with mkcephfs <mkcephfs>
   Deploy with Chef <chef>
   Storage Pools <pools>
   Authentication <authentication>