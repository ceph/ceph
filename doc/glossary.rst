===============
 Ceph Glossary
===============

.. glossary::

	:ref:`BlueStore<rados_config_storage_devices_bluestore>`
                OSD BlueStore is a storage back end used by OSD daemons, and
                was designed specifically for use with Ceph. BlueStore was
                introduced in the Ceph Kraken release. In the Ceph Luminous
                release, BlueStore became Ceph's default storage back end,
                supplanting FileStore. Unlike :term:`filestore`, BlueStore
                stores objects directly on Ceph block devices without any file
                system interface. Since Luminous (12.2), BlueStore has been
                Ceph's default and recommended storage back end.

	Ceph
	Ceph Block Device
                A software instrument that orchestrates the storage of
                block-based data in Ceph. Ceph Block Device (also called "RBD",
                or "RADOS block device") splits block-based application data
                into "chunks". RADOS stores these chunks as objects. Ceph Block
                Device orchestrates the storage of those objects across the
                storage cluster. See also :term:`RBD`.

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
                :ref:`The Ceph Dashboard<mgr-dashboard>` is a built-in
                web-based Ceph management and monitoring application through
                which you can inspect and administer various resources within
                the cluster. It is implemented as a :ref:`ceph-manager-daemon`
                module.

	Ceph File System
                See :term:`CephFS`

	CephFS
                The POSIX filesystem components of Ceph. Refer :ref:`CephFS
                Architecture <arch-cephfs>` and :ref:`ceph-file-system` for
                more details.

	Ceph Interim Release
                A version of Ceph that has not yet been put through quality
                assurance testing. May contain new features.

	Ceph Kernel Modules
                The collection of kernel modules that can be used to interact
                with the Ceph System (for example: ``ceph.ko``, ``rbd.ko``).

	Ceph Manager
                The Ceph manager daemon (ceph-mgr) is a daemon that runs
                alongside monitor daemons to provide monitoring and interfacing
                to external monitoring and management systems. Since the
                Luminous release (12.x), the ceph-mgr daemon is required in
                order for the Ceph cluster to function properly.

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
                CloudStack, OpenNebula, and Proxmox VE.

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
                See :term:`OSD`.

	OSD
                **O**\bject **S**\torage **D**\evice. A physical or logical
                storage unit (for example: LUN). Ceph users sometimes use the
                term "OSD" to refer to :term:`Ceph OSD Daemon`, but this is
                incorrect. The proper expansion of "Ceph OSD" is "object
                storage device". In contrast, :term:`Ceph OSD Daemon` refers to
                :ref:`\``ceph-osd``<ceph_osd-daemon>`, not to "**O**\bject
                **S**\torage **D**\evice".

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
