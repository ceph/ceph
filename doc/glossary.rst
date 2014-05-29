===============
 Ceph Glossary
===============

Ceph is growing rapidly. As firms deploy Ceph, the technical terms such as
"RADOS", "RBD," "RGW" and so forth require corresponding marketing terms
that explain what each component does. The terms in this glossary are 
intended to complement the existing technical terminology.

Sometimes more than one term applies to a definition. Generally, the first
term reflects a term consistent with Ceph's marketing, and secondary terms
reflect either technical terms or legacy ways of referring to Ceph systems.


.. glossary:: 

	Ceph Project
		The aggregate term for the people, software, mission and infrastructure 
		of Ceph.
		
	cephx
		The Ceph authentication protocol. Cephx operates like Kerberos, but it
		has no single point of failure.

	Ceph
	Ceph Platform
		All Ceph software, which includes any piece of code hosted at 
		`http://github.com/ceph`_.
		
	Ceph System
	Ceph Stack
		A collection of two or more components of Ceph.

	Ceph Node
	Node
	Host
		Any single machine or server in a Ceph System.
		
	Ceph Storage Cluster
	Ceph Object Store
	RADOS
	RADOS Cluster
	Reliable Autonomic Distributed Object Store
		The core set of storage software which stores the user's data (MON+OSD).

	Ceph Cluster Map
	cluster map
		The set of maps comprising the monitor map, OSD map, PG map, MDS map and 
		CRUSH map. See `Cluster Map`_ for details.

	Ceph Object Storage
		The object storage "product", service or capabilities, which consists
		essentially of a Ceph Storage Cluster and a Ceph Object Gateway.

	Ceph Object Gateway
	RADOS Gateway
	RGW
		The S3/Swift gateway component of Ceph.
				
	Ceph Block Device
	RBD
		The block storage component of Ceph.
		
	Ceph Block Storage
		The block storage "product," service or capabilities when used in 
		conjunction with ``librbd``, a hypervisor such as QEMU or Xen, and a
		hypervisor abstraction layer such as ``libvirt``.

	Ceph Filesystem
	CephFS
	Ceph FS
		The POSIX filesystem components of Ceph.

	Cloud Platforms
	Cloud Stacks
		Third party cloud provisioning platforms such as OpenStack, CloudStack, 
		OpenNebula, ProxMox, etc.

	Object Storage Device
	OSD
		A physical or logical storage unit (*e.g.*, LUN).
		Sometimes, Ceph users use the
		term "OSD" to refer to :term:`Ceph OSD Daemon`, though the
		proper term is "Ceph OSD".
		
	Ceph OSD Daemon
	Ceph OSD
		The Ceph OSD software, which interacts with a logical
		disk (:term:`OSD`). Sometimes, Ceph users use the
		term "OSD" to refer to "Ceph OSD Daemon", though the
		proper term is "Ceph OSD".
		
	Ceph Monitor
	MON
		The Ceph monitor software.
	
	Ceph Metadata Server
	MDS
		The Ceph metadata software.

	Ceph Clients
	Ceph Client
		The collection of Ceph components which can access a Ceph Storage 
		Cluster. These include the Ceph Object Gateway, the Ceph Block Device, 
		the Ceph Filesystem, and their corresponding libraries, kernel modules, 
		and FUSEs.

	Ceph Kernel Modules
		The collection of kernel modules which can be used to interact with the 
		Ceph System (e.g,. ``ceph.ko``, ``rbd.ko``).

	Ceph Client Libraries
		The collection of libraries that can be used to interact with components 
		of the Ceph System.

	Ceph Release
		Any distinct numbered version of Ceph.
	
	Ceph Point Release
		Any ad-hoc release that includes only bug or security fixes.

	Ceph Interim Release
		Versions of Ceph that have not yet been put through quality assurance
		testing, but may contain new features.

	Ceph Release Candidate
		A major version of Ceph that has undergone initial quality assurance 
		testing and is ready for beta testers.

	Ceph Stable Release
		A major version of Ceph where all features from the preceding interim 
		releases have been put through quality assurance testing successfully.

	Ceph Test Framework
	Teuthology
		The collection of software that performs scripted tests on Ceph.

	CRUSH
		Controlled Replication Under Scalable Hashing. It is the algorithm
		Ceph uses to compute object storage locations.
		
	ruleset
		A set of CRUSH data placement rules that applies to a particular pool(s).

	Pool
	Pools
		Pools are logical partitions for storing objects.

.. _http://github.com/ceph: http://github.com/ceph
.. _Cluster Map: ../architecture#cluster-map
