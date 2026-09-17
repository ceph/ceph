=================
 Welcome to Ceph
=================

.. meta::
   :description: Ceph documentation. Ceph delivers object, block, and file storage in one unified system.
   :ceph-page-type: assembly
   :ceph-owner: docs

Ceph delivers **object, block, and file storage in one unified system**. It
runs on ordinary servers, keeps redundant copies of your data, and has no
single point of failure.

**New to Ceph?** Go to :ref:`Start Here <start-here>`. It explains what Ceph
is and walks you through a first cluster.

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - I want to
     - Go to
   * - Learn what Ceph is
     - :ref:`Start Here <start-here>`, :ref:`Architecture <architecture>`,
       :doc:`Glossary <glossary>`
   * - Deploy a cluster
     - :ref:`Installing Ceph <install-overview>`,
       :ref:`Cephadm <cephadm>`,
       :doc:`Upgrading <cephadm/upgrade>`
   * - Operate a cluster
     - :ref:`Cluster operations <rados-operations>`,
       :ref:`Manager modules <ceph-manager-daemon>`,
       :ref:`Dashboard <mgr-dashboard>`,
       :ref:`Monitoring <monitoring>`
   * - Use a storage service
     - :ref:`Block Device (RBD) <ceph_block_device>`,
       :ref:`File System (CephFS) <ceph-file-system>`,
       :ref:`Object Gateway (RGW) <object-gateway>`,
       :ref:`Kubernetes (CSI) <ceph-csi>`
   * - Fix a problem
     - :ref:`Troubleshooting <rados_troubleshooting>`,
       :doc:`Health checks <rados/operations/health-checks>`,
       :doc:`CephFS troubleshooting <cephfs/troubleshooting>`,
       :doc:`RGW troubleshooting <radosgw/troubleshooting>`
   * - Look something up
     - :doc:`Configuration <rados/configuration/index>`,
       :doc:`APIs <api/index>`,
       :ref:`Releases <ceph-releases-general>`,
       :doc:`Glossary <glossary>`
   * - Contribute
     - :doc:`Developer Guide <dev/developer_guide/index>`,
       :ref:`Documenting Ceph <documenting_ceph>`,
       :ref:`Get Involved <Get Involved>`

.. toctree::
   :maxdepth: 3
   :hidden:

   start/index
   install/index
   cephadm/index
   rados/index
   cephfs/index
   rbd/index
   radosgw/index
   csi/index
   mgr/index
   mgr/dashboard
   monitoring/index
   api/index
   architecture/index
   Developer Guide <dev/developer_guide/index>
   dev/internals
   governance
   Technical Charter <technical-charter>
   foundation
   ceph-volume/index
   crimson/crimson
   releases/general
   releases/index
   security/index
   hardware-monitoring/index
   Glossary <glossary>
   Tracing <jaegertracing/index>
