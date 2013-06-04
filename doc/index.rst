=================
 Welcome to Ceph
=================

Ceph uniquely delivers **object, block, and file storage in one unified
system**. Ceph is highly reliable, easy to manage, and free. The power of Ceph
can transform your company's IT infrastructure and your ability to manage vast
amounts of data. Ceph delivers extraordinary scalability--thousands of clients
accessing petabytes to exabytes of data. Ceph leverages commodity hardware and
intelligent daemons to accommodate large numbers of storage hosts, which
communicate with each other to replicate data, and redistribute data
dynamically. Ceph's cluster of monitors oversees the hosts in the Ceph storage
cluster to ensure that the storage hosts are running smoothly.

.. image:: images/stack.png

.. sidebar:: About Ceph
	
	**One Object Storage:** The Ceph Object Store, called RADOS, is the 
	object storage component for CephFS filesystems, Ceph RADOS Gateways, and 
	Ceph Block Devices.
	
	**Many Storage Interfaces:** You can use CephFS, Ceph RADOS Gateway, or Ceph 
	Block Devices in your deployment. You may also use all three interfaces 
	with the same Ceph Object Store cluster! There's no reason to build three 
	different storage clusters for three different types of storage interface!
	
	**Use Commodity Hardware!:** You can deploy Ceph with commodity hardware. 
	You don't need to purchase proprietary storage or networking hardware 
	commonly used in :abbr:`SAN (Storage Area Network)` systems.




.. toctree::
   :maxdepth: 1
   :hidden:

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
   Glossary <glossary>
