=================
 Welcome to Ceph
=================

Ceph uniquely delivers **object, block, and file storage in one unified
system**. 

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
- Disaster recovery

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
- Disaster recovery

.. raw:: html 

	</td><td><h3>Ceph Filesystem</h3>
	
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
	
See `Ceph Filesystem`_ for additional details.	
	
.. raw::	html 

	</td></tr></tbody></table>

Ceph is highly reliable, easy to manage, and free. The power of Ceph
can transform your company's IT infrastructure and your ability to manage vast
amounts of data. To try Ceph, see our `Getting Started`_ guides. To learn more
about Ceph, see our `Architecture`_ section.



.. _Ceph Object Store: radosgw
.. _Ceph Block Device: rbd/rbd
.. _Ceph Filesystem: cephfs
.. _Getting Started: start
.. _Architecture: architecture

.. toctree::
   :maxdepth: 1
   :hidden:

   start/intro
   start/index
   install/index
   rados/index
   cephfs/index
   rbd/rbd
   radosgw/index
   api/index
   architecture
   Development <dev/index>
   release-notes
   releases
   Glossary <glossary>
