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
                Ceph is a distributed network storage and file system with
                distributed metadata management and POSIX semantics.

	Ceph Block Device
                A software instrument that orchestrates the storage of
                block-based data in Ceph. Ceph Block Device (also called "RBD",
                or "RADOS block device") splits block-based application data
                into "chunks". RADOS stores these chunks as objects. Ceph Block
                Device orchestrates the storage of those objects across the
                storage cluster. See also :term:`RBD`.

	Ceph Block Storage
                One of the three kinds of storage supported by Ceph (the other
                two are object storage and file storage). Ceph Block Storage is
                the block storage "product", which refers to block-storage
                related services and capabilities when used in conjunction with
                the collection of (1) ``librbd`` (a python module that provides
                file-like access to :term:`RBD` images), (2) a hypervisor such
                as QEMU or Xen, and (3) a hypervisor abstraction layer such as
                ``libvirt``.

	Ceph Client
                The collection of Ceph components which can access a Ceph
                Storage Cluster. These include the Ceph Object Gateway, the
                Ceph Block Device, the Ceph File System, and their
                corresponding libraries, kernel modules, and FUSEs.

	Ceph Client Libraries
                The collection of libraries that can be used to interact with
                components of the Ceph System.

	Ceph Cluster Map
                See :term:`Cluster Map`

	Ceph Dashboard
                :ref:`The Ceph Dashboard<mgr-dashboard>` is a built-in
                web-based Ceph management and monitoring application through
                which you can inspect and administer various resources within
                the cluster. It is implemented as a :ref:`ceph-manager-daemon`
                module.

	Ceph File System
                See :term:`CephFS`

	:ref:`CephFS<ceph-file-system>`
                The Ceph File System, or CephFS, is a POSIX-compliant file
                system built on top of Ceph’s distributed object store, RADOS.
                See :ref:`CephFS Architecture <arch-cephfs>` for more details.

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
                See :term:`Ceph Dashboard`.

	Ceph Metadata Server
                See :term:`MDS`.

	Ceph Monitor
                A daemon that maintains a map of the state of the cluster. This
                "cluster state" includes the monitor map, the manager map, the
                OSD map, and the CRUSH map. A minimum of three monitors is
                required in order for the Ceph cluster to be both redundant and
                highly-available. Ceph monitors and the nodes on which they run
                are often referred to as "mon"s. **SEE** :ref:`Monitor Config
                Reference <monitor-config-reference>`.

	Ceph Node
	:ref:`Ceph Object Gateway<object-gateway>`
                An object storage interface built on top of librados. Ceph
                Object Gateway provides a RESTful gateway between applications
                and Ceph storage clusters.

	Ceph Object Storage
                The object storage "product", service or capabilities, which
                consists essentially of a Ceph Storage Cluster and a Ceph Object
                Gateway.

	Ceph Object Store
	:ref:`Ceph OSD<rados_configuration_storage-devices_ceph_osd>`
                Ceph **O**\bject **S**\torage **D**\aemon. The Ceph OSD
                software, which interacts with logical disks (:term:`OSD`).
                Around 2013, there was an attempt by "research and industry"
                (Sage's own words) to insist on using the term "OSD" to mean
                only "Object Storage Device", but the Ceph community has always
                persisted in using the term to mean "Object Storage Daemon" and
                no less an authority than Sage Weil himself confirms in
                November of 2022 that "Daemon is more accurate for how Ceph is
                built" (private correspondence between Zac Dover and Sage Weil,
                07 Nov 2022). 

	Ceph OSD Daemon
                See :term:`Ceph OSD`.

	Ceph OSD Daemons
                See :term:`Ceph OSD`.

	Ceph Platform
                All Ceph software, which includes any piece of code hosted at
                `https://github.com/ceph`_.

	Ceph Point Release
                Any ad hoc release that includes only bug fixes and security
                fixes.

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
	cephx
                The Ceph authentication protocol. Cephx operates like Kerberos,
                but it has no single point of failure.

	Cloud Platforms
	Cloud Stacks
                Third party cloud provisioning platforms such as OpenStack,
                CloudStack, OpenNebula, and Proxmox VE.

	Cluster Map
                The set of maps comprising the monitor map, OSD map, PG map,
                MDS map and CRUSH map, which together report the state of the
                Ceph cluster. See :ref:`the "Cluster Map" section of the
                Architecture document<architecture_cluster_map>` for details.

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

	:ref:`MDS<cephfs_add_remote_mds>`
                The Ceph metadata server daemon. Also referred to as
                "ceph-mds". The Ceph metadata server daemon is required to run
                the CephFS file system. The MDS stores all filesystem metadata. 

	MGR
                The Ceph manager software, which collects all the state from
                the whole cluster in one place.

	MON
		The Ceph monitor software.

	Node
	Object Storage Device
                See :term:`OSD`.

	OSD
                Probably :term:`Ceph Object Storage Daemon<Ceph OSD>`, but not
                necessarily. Sometimes (especially in older correspondence, and
                especially in documentation that is not specifically written
                for Ceph), "OSD" means "**O**\bject **S**\torage **D**\evice",
                which refers to a physical or logical storage unit (for
                example: LUN). The Ceph community has always used the term
                "OSD" to refer to :term:`Ceph OSD Daemon` despite an industry
                push in the mid-2010s to insist that "OSD" should refer to
                "Object Storage Device", so it is important to know which
                meaning is intended. 

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

	:ref:`Pool<rados_pools>`
		A pool is a logical partition used to store objects.

	Pools
                See :term:`pool`.

	RADOS
                **R**\eliable **A**\utonomic **D**\istributed **O**\bject
                **S**\tore. RADOS is the object store that provides a scalable
                service for variably-sized objects. The RADOS object store is
                the core component of a Ceph cluster.  `This blog post from
                2009
                <https://ceph.io/en/news/blog/2009/the-rados-distributed-object-store/>`_
                provides a beginner's introduction to RADOS. Readers interested
                in a deeper understanding of RADOS are directed to `RADOS: A
                Scalable, Reliable Storage Service for Petabyte-scale Storage
                Clusters <https://ceph.io/assets/pdfs/weil-rados-pdsw07.pdf>`_.

	RADOS Cluster
	RADOS Gateway
                See :term:`RGW`.

	RBD
		The block storage component of Ceph.

	Reliable Autonomic Distributed Object Store
                The core set of storage software which stores the user's data
                (MON+OSD). See also :term:`RADOS`.

	:ref:`RGW<object-gateway>`
                **R**\ADOS **G**\ate **W**\ay.

                The component of Ceph that provides a gateway to both the
                Amazon S3 RESTful API and the OpenStack Swift API. Also called
                "RADOS Gateway" and "Ceph Object Gateway".

	systemd oneshot
                A systemd ``type`` where a command is defined in ``ExecStart``
                which will exit upon completion (it is not intended to
                daemonize)

	Teuthology
		The collection of software that performs scripted tests on Ceph.

.. _https://github.com/ceph: https://github.com/ceph
.. _Cluster Map: ../architecture#cluster-map   
