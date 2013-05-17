=========
 Ceph FS
=========

The :term:`Ceph FS` file system is a POSIX-compliant file system that uses a
Ceph Storage Cluster to store its data. Ceph FS uses the same Ceph Storage
Cluster system as Ceph Block Devices, Ceph Object Storage with its S3 and Swift
APIs, or native bindings (librados).


.. ditaa::
            +-----------------------+  +------------------------+
            | CephFS Kernel Object  |  |      CephFS FUSE       |
            +-----------------------+  +------------------------+            

            +---------------------------------------------------+
            |            Ceph FS Library (libcephfs)            |
            +---------------------------------------------------+

            +---------------------------------------------------+
            |      Ceph Storage Cluster Protocol (librados)     |
            +---------------------------------------------------+

            +---------------+ +---------------+ +---------------+
            |      OSDs     | |      MDSs     | |    Monitors   |
            +---------------+ +---------------+ +---------------+


Using Ceph FS requires at least one :term:`Ceph Metadata Server` in your
Ceph Storage Cluster.



.. raw:: html

	<style type="text/css">div.body h3{margin:5px 0px 0px 0px;}</style>
	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Step 1: Metadata Server</h3>

To run Ceph FS, you must have a running Ceph Storage Cluster with at least
one :term:`Ceph Metadata Server` running.


.. toctree:: 
	:maxdepth: 1

	Add/Remove MDS <../../rados/deployment/ceph-deploy-mds>
	MDS Configuration <mds-config-ref>
	Journaler Configuration <journaler>
	Manpage ceph-mds <../../man/8/ceph-mds>

.. raw:: html 

	</td><td><h3>Step 2: Mount Ceph FS</h3>

Once you have a healthy Ceph Storage Cluster with at least
one Ceph Metadata Server, you may mount your Ceph FS filesystem.
Ensure that you client has network connectivity and the proper
authentication keyring.

.. toctree:: 
	:maxdepth: 1

	Mount Ceph FS <kernel>
	Mount Ceph FS as FUSE <fuse>
	Mount Ceph FS in fstab <fstab>
	Manpage cephfs <../../man/8/cephfs>
	Manpage ceph-fuse <../../man/8/ceph-fuse>
	Manpage mount.ceph <../../man/8/mount.ceph>


.. raw:: html 

	</td><td><h3>Additional Details</h3>

.. toctree:: 
	:maxdepth: 1

	Using Ceph with Hadoop <hadoop>
	libcephfs <../../api/libcephfs-java/>

.. raw:: html

	</td></tr></tbody></table>
