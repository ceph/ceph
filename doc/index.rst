=================
 Welcome to Ceph
=================

Ceph uniquely delivers **object, block, and file storage in one unified
system**.

Quick Links
===========

.. image:: images/button_octo_install.png
    :target: `Octopus Getting Started`_ 
    :width: 33 %
.. image:: images/button_dev_guide.png
    :target: `Developer Guide`_
    :width: 33 %
.. image:: images/button_bug_tracker.png
    :target: https://tracker.ceph.com
    :width: 33 %

To acquaint yourself with Ceph, use the **Octopus Getting Started Guide** to set up a basic Ceph cluster.

To learn how to make a contribution to the Ceph project, read the **Developer Guide**.

To report a bug, click the **Bug Tracker** link.


Features
========

.. raw:: html

	<style type="text/css">div.body h3{margin:5px 0px 0px 0px;}</style>
	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Ceph Object Store</h3>

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

.. raw:: html

	</td><td><h3>Ceph Block Device</h3>


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

.. raw:: html

	</td><td><h3>Ceph File System</h3>

- POSIX-compliant semantics
- Separates metadata from data
- Dynamic rebalancing
- Subdirectory snapshots
- Configurable striping
- Kernel driver support
- FUSE support
- NFS/CIFS deployable
- Use with Hadoop (replace HDFS)

.. raw:: html

	</td></tr><tr><td>

See `Ceph Object Store`_ for additional details.

.. raw:: html

	</td><td>

See `Ceph Block Device`_ for additional details.

.. raw:: html

	</td><td>

See `Ceph File System`_ for additional details.

.. raw::	html

	</td></tr></tbody></table>

Ceph is highly reliable, easy to manage, and free. The power of Ceph
can transform your company's IT infrastructure and your ability to manage vast
amounts of data. To try Ceph, see our `Getting Started`_ guides. To learn more
about Ceph, see our `Architecture`_ section.



.. _Ceph Object Store: radosgw
.. _Ceph Block Device: rbd
.. _Ceph File System: cephfs
.. _Getting Started: install
.. _Architecture: architecture
.. _Octopus Getting Started: cephadm/octopus_gsg
.. _Developer Guide: dev/developer_guide/intro

.. toctree::
   :maxdepth: 3
   :hidden:

   start/intro
   install/index
   cephadm/index
   cephadm/octopus_gsg
   rados/index
   cephfs/index
   rbd/index
   radosgw/index
   mgr/index
   mgr/dashboard
   api/index
   architecture
   Developer Guide <dev/developer_guide/index>
   dev/internals
   governance
   foundation
   ceph-volume/index
   releases/general
   releases/index
   Glossary <glossary>
