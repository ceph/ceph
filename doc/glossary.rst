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

	bluestore
                OSD BlueStore is a new back end for OSD daemons (kraken and
                newer versions). Unlike :term:`filestore` it stores objects
                directly on the Ceph block devices without any file system
                interface.

	Ceph
	Ceph Block Device
	Ceph Block Storage
                The block storage "product," service or capabilities when used
                in conjunction with ``librbd``, a hypervisor such as QEMU or
                Xen, and a hypervisor abstraction layer such as ``libvirt``.

	Ceph Client
                The collection of Ceph components which can access a Ceph
                Storage Cluster. These include the Ceph Object Gateway, the
                Ceph Block Device, the Ceph File System, and their
                corresponding libraries, kernel modules, and FUSEs.

	Ceph Client Libraries
                The collection of libraries that can be used to interact with
                components of the Ceph System.

	Ceph Cluster Map
	Ceph Dashboard
	Ceph File System
                See :term:`CephFS`

	CephFS
                The POSIX filesystem components of Ceph. Refer :ref:`CephFS
                Architecture <arch-cephfs>` and :ref:`ceph-file-system` for
                more details.

	Ceph Interim Release
                Versions of Ceph that have not yet been put through quality
                assurance testing, but may contain new features.

	Ceph Kernel Modules
                The collection of kernel modules which can be used to interact
                with the Ceph System (e.g., ``ceph.ko``, ``rbd.ko``).

	Ceph Manager
	Ceph Manager Dashboard
	Ceph Metadata Server
	Ceph Monitor
                A daemon that maintains a map of the state of the cluster. This
                "cluster state" includes the monitor map, the manager map, the
                OSD map, and the CRUSH map. A minimum of three monitors is
                required in order for the Ceph cluster to be both redundant and
                highly-available. Ceph monitors and the nodes on which they run
                are often referred to as "mon"s. **SEE** :ref:`Monitor Config
                Reference <monitor-config-reference>`.

	Ceph Node
	Ceph Object Gateway
	Ceph Object Storage
                The object storage "product", service or capabilities, which
                consists essentially of a Ceph Storage Cluster and a Ceph Object
                Gateway.

	Ceph Object Store
	Ceph OSD
		The Ceph OSD software, which interacts with a logical
		disk (:term:`OSD`). Sometimes, Ceph users use the
		term "OSD" to refer to "Ceph OSD Daemon", though the
		proper term is "Ceph OSD".

	Ceph OSD Daemon
	Ceph OSD Daemons
	Ceph Platform
                All Ceph software, which includes any piece of code hosted at
                `https://github.com/ceph`_.

	Ceph Point Release
		Any ad-hoc release that includes only bug or security fixes.

	Ceph Project
                The aggregate term for the people, software, mission and
                infrastructure of Ceph.

	Ceph Release
		Any distinct numbered version of Ceph.

	Ceph Release Candidate
                A major version of Ceph that has undergone initial quality
                assurance testing and is ready for beta testers.

	Ceph Stable Release
                A major version of Ceph where all features from the preceding
                interim releases have been put through quality assurance
                testing successfully.

	Ceph Stack
		A collection of two or more components of Ceph.

	Ceph Storage Cluster
	Ceph System
	Ceph Test Framework
	cephx
                The Ceph authentication protocol. Cephx operates like Kerberos,
                but it has no single point of failure.

	Cloud Platforms
	Cloud Stacks
                Third party cloud provisioning platforms such as OpenStack,
                CloudStack, OpenNebula, Proxmox VE, etc.

	Cluster Map
                The set of maps comprising the monitor map, OSD map, PG map,
                MDS map and CRUSH map. See `Cluster Map`_ for details.

	CRUSH
                Controlled Replication Under Scalable Hashing. It is the
                algorithm Ceph uses to compute object storage locations.

	CRUSH rule
                The CRUSH data placement rule that applies to a particular
                pool(s).

	Dashboard
                A built-in web-based Ceph management and monitoring application
                to administer various aspects and objects of the cluster. The
                dashboard is implemented as a Ceph Manager module. See
                :ref:`mgr-dashboard` for more details.

	Dashboard Module
	Dashboard Plugin
	filestore
                A back end for OSD daemons, where a Journal is needed and files
                are written to the filesystem.

	Host
		Any single machine or server in a Ceph System.

	LVM tags
                Extensible metadata for LVM volumes and groups. It is used to
                store Ceph-specific information about devices and its
                relationship with OSDs.

	MDS
		The Ceph metadata software.

	MGR
                The Ceph manager software, which collects all the state from
                the whole cluster in one place.

	MON
		The Ceph monitor software.

	Node
	Object Storage Device
	OSD
		A physical or logical storage unit (*e.g.*, LUN).
		Sometimes, Ceph users use the
		term "OSD" to refer to :term:`Ceph OSD Daemon`, though the
		proper term is "Ceph OSD".

	OSD fsid
                This is a unique identifier used to further improve the
                uniqueness of an OSD and it is found in the OSD path in a file
                called ``osd_fsid``. This ``fsid`` term is used interchangeably
                with ``uuid``

	OSD id
                The integer that defines an OSD. It is generated by the
                monitors as part of the creation of a new OSD.

	OSD uuid
                Just like the OSD fsid, this is the OSD unique identifier and
                is used interchangeably with ``fsid``

	Pool
	Pools
		Pools are logical partitions for storing objects.

	RADOS
	RADOS Cluster
	RADOS Gateway
	RBD
		The block storage component of Ceph.

	Reliable Autonomic Distributed Object Store
                The core set of storage software which stores the user's data
                (MON+OSD).

	RGW
		The S3/Swift gateway component of Ceph.

	systemd oneshot
                A systemd ``type`` where a command is defined in ``ExecStart``
                which will exit upon completion (it is not intended to
                daemonize)

	Teuthology
		The collection of software that performs scripted tests on Ceph.

.. _https://github.com/ceph: https://github.com/ceph
.. _Cluster Map: ../architecture#cluster-map
