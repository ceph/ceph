=================
 Welcome to Ceph
=================

Ceph delivers **object, block, and file storage in one unified system**.

.. warning::

   :ref:`If this is your first time using Ceph, read the "Basic Workflow"
   page in the Ceph Developer Guide to learn how to contribute to the
   Ceph project. (Click anywhere in this paragraph to read the "Basic
   Workflow" page of the Ceph Developer Guide.) <basic workflow dev guide>`.

.. note::

   :ref:`If you want to make a commit to the documentation but you don't
   know how to get started, read the "Documenting Ceph" page. (Click anywhere
   in this paragraph to read the "Documenting Ceph" page.) <documenting_ceph>`.

.. container:: columns-3

   .. container:: column

      .. raw:: html

          <h3>Ceph Object Store</h3>

      - RESTful Interface
      - S3- and Swift-compliant APIs
      - S3-style subdomains
      - Unified S3/Swift namespace
      - User management
      - Usage tracking
      - Striped objects
      - Cloud solution integration
      - Multi-site deployment
      - Multi-site replication

   .. container:: column

      .. raw:: html

          <h3>Ceph Block Device</h3>

      - Thin-provisioned
      - Images up to 16 exabytes
      - Configurable striping
      - In-memory caching
      - Snapshots
      - Copy-on-write cloning
      - Kernel driver support
      - KVM/libvirt support
      - Back-end for cloud solutions
      - Incremental backup
      - Disaster recovery (multisite asynchronous replication)

   .. container:: column

      .. raw:: html

          <h3>Ceph File System</h3>

      - POSIX-compliant semantics
      - Separates metadata from data
      - Dynamic rebalancing
      - Subdirectory snapshots
      - Configurable striping
      - Kernel driver support
      - FUSE support
      - NFS/CIFS deployable
      - Use with Hadoop (replace HDFS)

.. container:: columns-3

   .. container:: column

      See `Ceph Object Store`_ for additional details.

   .. container:: column

      See `Ceph Block Device`_ for additional details.

   .. container:: column

      See `Ceph File System`_ for additional details.

Ceph is highly reliable, easy to manage, and free. The power of Ceph
can transform your company's IT infrastructure and your ability to manage vast
amounts of data. To try Ceph, see our `Getting Started`_ guides. To learn more
about Ceph, see our `Architecture`_ section.



.. _Ceph Object Store: radosgw
.. _Ceph Block Device: rbd
.. _Ceph File System: cephfs
.. _Getting Started: install
.. _Architecture: architecture

.. toctree::
   :maxdepth: 3
   :hidden:

   start/intro
   install/index
   cephadm/index
   rados/index
   cephfs/index
   rbd/index
   radosgw/index
   mgr/index
   mgr/dashboard
   monitoring/index
   api/index
   architecture
   Developer Guide <dev/developer_guide/index>
   dev/internals
   governance
   foundation
   ceph-volume/index
   Ceph Releases (general) <https://docs.ceph.com/en/latest/releases/general/>
   Ceph Releases (index) <https://docs.ceph.com/en/latest/releases/>
   security/index
   hardware-monitoring/index
   Glossary <glossary>
   Tracing <jaegertracing/index>
