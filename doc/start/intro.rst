===============
 Intro to Ceph
===============

Ceph can be used to provide :term:`Ceph Object Storage` to :term:`Cloud
Platforms` and Ceph can be used to provide :term:`Ceph Block Device` services
to :term:`Cloud Platforms`. Ceph can be used to deploy a :term:`Ceph File
System`.  All :term:`Ceph Storage Cluster` deployments begin with setting up
each :term:`Ceph Node` and then setting up the network. 

A Ceph Storage Cluster requires the following: at least one Ceph Monitor and at
least one Ceph Manager, and at least as many :term:`Ceph Object Storage
Daemon<Ceph OSD>`\s (OSDs) as there are copies of a given object stored in the
Ceph cluster (for example, if three copies of a given object are stored in the
Ceph cluster, then at least three OSDs must exist in that Ceph cluster).

The Ceph Metadata Server is necessary to run Ceph File System clients.

.. note::

   It is a best practice to have a Ceph Manager for each Monitor, but it is not
   necessary. 

.. ditaa::

            +---------------+ +------------+ +------------+ +---------------+
            |      OSDs     | | Monitors   | |  Managers  | |      MDSs     |
            +---------------+ +------------+ +------------+ +---------------+

- **Monitors**: A :term:`Ceph Monitor` (``ceph-mon``) maintains maps of the
  cluster state, including the :ref:`monitor map<display-mon-map>`, manager
  map, the OSD map, the MDS map, and the CRUSH map.  These maps are critical
  cluster state required for Ceph daemons to coordinate with each other.
  Monitors are also responsible for managing authentication between daemons and
  clients.  At least three monitors are normally required for redundancy and
  high availability.

- **Managers**: A :term:`Ceph Manager` daemon (``ceph-mgr``) is
  responsible for keeping track of runtime metrics and the current
  state of the Ceph cluster, including storage utilization, current
  performance metrics, and system load.  The Ceph Manager daemons also
  host python-based modules to manage and expose Ceph cluster
  information, including a web-based :ref:`mgr-dashboard` and
  `REST API`_.  At least two managers are normally required for high
  availability.

- **Ceph OSDs**: An Object Storage Daemon (:term:`Ceph OSD`,
  ``ceph-osd``) stores data, handles data replication, recovery,
  rebalancing, and provides some monitoring information to Ceph
  Monitors and Managers by checking other Ceph OSD Daemons for a
  heartbeat. At least three Ceph OSDs are normally required for 
  redundancy and high availability.

- **MDSs**: A :term:`Ceph Metadata Server` (MDS, ``ceph-mds``) stores
  metadata on behalf of the :term:`Ceph File System` (i.e., Ceph Block
  Devices and Ceph Object Storage do not use MDS). Ceph Metadata
  Servers allow POSIX file system users to execute basic commands (like
  ``ls``, ``find``, etc.) without placing an enormous burden on the
  Ceph Storage Cluster.

Ceph stores data as objects within logical storage pools. Using the
:term:`CRUSH` algorithm, Ceph calculates which placement group (PG) should
contain the object, and which OSD should store the placement group.  The
CRUSH algorithm enables the Ceph Storage Cluster to scale, rebalance, and
recover dynamically.

.. _REST API: ../../mgr/restful

.. container:: columns-2

   .. container:: column

      .. raw:: html

          <h3>Recommendations</h3>

      To begin using Ceph in production, you should review our hardware
      recommendations and operating system recommendations.

      .. toctree::
         :maxdepth: 2

         Hardware Recommendations <hardware-recommendations>
         OS Recommendations <os-recommendations>

   .. container:: column

      .. raw:: html

          <h3>Get Involved</h3>

      You can avail yourself of help or contribute documentation, source
      code or bugs by getting involved in the Ceph community.

      .. toctree::
         :maxdepth: 2

         get-involved
         documenting-ceph
