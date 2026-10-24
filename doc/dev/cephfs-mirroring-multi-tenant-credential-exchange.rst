======================================================================================
CephFS Snapshot Mirroring: Multi-Tenant Credential Management & Isolation Architecture
======================================================================================

Overview & Problem Statement
============================

CephFS snapshot mirroring currently relies on a cluster-wide CephX credential that grants broad read/write
capabilities across the filesystem root (``/``). When replication scales across thousands of multi-tenant
subvolumes, this model presents critical trade-offs:

1. **Unbounded Blast Radius:** A compromised mirror daemon or leaked peer credential grants access across
   all shares and metadata on the secondary cluster.
2. **Key-Management Anti-Patterns:** Having ``ceph-mgr`` act as a dynamic key-minting proxy to issue
   path-scoped keys reintroduces privilege escalation risks (e.g., service keys with MGR execution
   permissions requesting arbitrary path access).
3. **The Target Path Creation Dilemma:** If a credential is strictly path-jailed to a destination
   subvolume (e.g., ``mds 'allow rwps path=/volumes/.../subvol_B'``), the mirror daemon lacks permissions
   to execute ``mkdir`` in parent directories, preventing automated target directory creation.
4. **Metadata & UUID Divergence:** Manually creating secondary subvolumes leads to layout mismatches
   (UUIDs, pool namespaces, subvolume versions), breaking predictable pathing across clusters.

This document outlines a two-track architecture that enforces least-privilege, path-jailed isolation for
orchestrated environments while maintaining an operational :math:`O(1)` setup for standalone
CLI/``cephadm`` deployments without permitting key-minting in ``ceph-mgr``.

Threat Model & Guiding Principles
=================================

* **Least-Privilege Data Path:** Mirror daemons must only possess access to the specific paths they are
  actively synchronizing.
* **Control Plane vs. Data Plane Separation:** Key minting and directory lifecycle management belong
  strictly to the control plane (MON and MGR).
* **No Dynamic Key-Minting in MGR:** ``ceph-mgr`` modules must never act as un-jailed proxies into the MON
  auth subsystem. MGR modules track replication, balance workloads, and package tokens, but do not mint credentials.

Two-Tier Dynamic Credential Resolution
======================================

To prevent storing thousands of per-directory credentials in the primary monitor's ``config-key`` store
(which causes monitor store bloat), the primary cluster does not store per-subvolume keys
locally. Instead, it uses a two-tier dynamic exchange via a cluster-level **Super-Key** :

1. **Tier 1: Cluster-Level Peer Control Key ("Super-Key")**

   * A single, pre-shared CephX identity per cluster pairing (e.g., ``client.mirror_peer.<peer_uuid>``).
   * Authorized strictly for MON discovery/read access (``mon 'allow r'``) and the specific credential
     resolution endpoint on the secondary MGR.
   * Uses ``MgrCap`` argument constraints to lock down invocation by filesystem and pairing identity:

     .. code-block:: text

        [client.mirror_peer.<peer_uuid>]
        caps mon = "allow r" caps mgr = "allow command 'fs mirror peer_resolve_credentials' fs_name=<fs> peer_uuid=<peer_uuid>"

   * Contains **zero MDS and OSD capabilities**.

2. **Tier 2: Pre-Created Path-Jailed Subvolume Keys**
   * Dedicated path-scoped credentials created directly on the secondary MON during subvolume replication
     onboarding.
   * **Entity Naming Stability:** Entities avoid volatile subvolume internal UUIDs (which cause orphaned
     keys on subvolume teardown or recreate with ``--retain-snapshots``). They follow a deterministic
     hierarchical convention:

     .. code-block:: text

        client.mirror.<peer_uuid>.<group_name>.<subvol_name>

   * Scoped strictly to the target subvolume path (``mds 'allow rwps path=/volumes/<group>/<subvol>'``)
     and matching data pools/namespaces.

Track 1: Orchestrated Environments (CSI, Rook, Dashboard)
=========================================================

For multi-tenant orchestrated environments, structural directory provisioning and key minting are
delegated to the control plane before data replication begins.

.. code-block:: text

   [ Primary Cluster ]                                      [ Secondary Cluster ]
   ┌─────────────────────────┐                             ┌─────────────────────────┐
   │ Control Plane / CSI     │                             │ Control Plane / CSI     │
   └────────────┬────────────┘                             └────────────┬────────────┘
                │ 1. Export Template                                    │
                │    (UUID, version, pools)                             │
                ├──────────────────────( Base64 Token )────────────────►│
                │                                                       │ 2. Create Target Subvol
                │                                                       │    via Template
                │                                                       │ 3. Mint Path-Jailed Key
                │                                                       │    on Secondary MON
                │                                                       │
   ┌────────────┴────────────┐                             ┌────────────┴────────────┐
   │ cephfs-mirror Daemon    │                             │ ceph-mgr (Handshake)    │
   └────────────┬────────────┘                             └────────────┬────────────┘
                │                                                       │
                │ 4. Handshake via Super-Key (No MDS/OSD Caps)          │
                │    "peer_resolve_credentials fs_name=...              │
                │     peer_uuid=... group=... subvolume=..."            │
                ├──────────────────────────────────────────────────────►│
                │                                                       │ 5. Fetch pre-created
                │                                                       │    path-jailed key
                │ 6. Return Scoped Credential                           │
                │◄──────────────────────────────────────────────────────┤
                │                                                       │
                ▼                                                       ▼
   ┌─────────────────────────┐      Data Synchronization   ┌────────────────────────────┐
   │ Borrow/Init Libcephfs   ├────────────────────────────►│ Pre-created Target Dir     │
   │ Context with Scoped Key │      (allow rwps path=...)  │ (/volumes/<group>/<subvol>)│
   └─────────────────────────┘                             └────────────────────────────┘

Peer Registration & Super-Key Setup
-----------------------------------

When setting up cross-cluster replication, the orchestrator establishes the peer pairing identity:

1. A unique ``peer_uuid`` is assigned to the relationship and recorded in the primary cluster's peer
   table.
2. The orchestrator creates the Super-Key directly on the secondary MON:

   .. code-block:: bash

      ceph auth get-or-create client.mirror_peer.<peer_uuid> \
          mon 'allow r' \
          mgr 'allow command "fs mirror peer_resolve_credentials" fs_name=<sec_fs> peer_uuid=<peer_uuid>'

3. The primary imports this key and associates it with the secondary peer record.

Subvolume Template Generation
-----------------------------

To guarantee that the secondary subvolume matches the primary's internal subvolume version, UUID, pool
layout, and pool namespaces, the primary cluster exports a serialized, opaque template:

.. code-block:: bash

   ceph fs subvolume template export <vol> <subvol> [--group_name <group>]

* The output is encoded as an opaque base64 string (containing JSON metadata: ``version``, ``uuid``,
  ``data_pool``, ``pool_namespace``, ``quota_bytes``, and modes).
* Base64 encoding avoids escaping issues across shell wrappers, Kubernetes CRDs (``VolumeReplication``),
  and automation APIs.

Matched Subvolume Creation on Secondary
---------------------------------------

The orchestrator (Rook/CSI tooling, or Ceph Dashboard) ingests the base64 token directly on the secondary
cluster before synchronization begins:

.. code-block:: bash

   ceph fs subvolume create <vol> <subvol> --template <base64_token> [--group_name <group>]

The secondary cluster instantiates the directory and metadata using the exact UUID and parameters
extracted from the template.

Direct MON Key Provisioning
---------------------------

When a subvolume is scheduled for replication, the orchestrator generates the secondary subvolume using
the template metadata and registers the stable CephX identity on the secondary MON:

.. code-block:: bash

   ceph auth get-or-create client.mirror.<peer_uuid>.<group>.<subvol_name> \
       mon 'allow r' \
       mds 'allow rwps path=/volumes/<group>/<subvol_name>' \
       osd 'allow rw pool=<sec_data_pool> namespace=<ns> tag cephfs data=/volumes/<group>/<subvol_name>'

Because the entity name is tied to ``<peer_uuid>.<group>.<subvol_name>`` rather than an ephemeral
subvolume UUID, deleting and recreating the underlying volume cleanly updates or reuses the credential
entry without accumulating orphaned keys in the secondary MON database.

Dynamic Credential Resolution
-----------------------------

When a sync job is scheduled on the primary:

1. The ``cephfs-mirror`` worker presents the Super-Key (``client.mirror_peer.<peer_uuid>``) to the
   secondary MGR endpoint:

   .. code-block:: bash

      ceph --id mirror_peer.<peer_uuid> fs mirror peer_resolve_credentials \
          --fs_name <sec_fs> \
          --peer_uuid <peer_uuid> \
          --group_name <group> \
          --subvolume <subvol_name>

2. The secondary MGR verifies that the caller's ``peer_uuid`` matches its ``MgrCap`` constraints.
3. The MGR performs a local read of ``client.mirror.<peer_uuid>.<group>.<subvol_name>`` and returns the
   keyring and capability specification to the daemon.
4. The mirror daemon initializes or borrows an active ``libcephfs`` mount context using the path-scoped
   key to replicate the snapshot into the target subvolume.

Track 2: Standalone CLI & cephadm Environments
==============================================

Standalone deployments lack an external orchestrator to automate per-subvolume rituals. To retain an
:math:`O(1)` administrative experience without delegating auth-minting authority to ``ceph-mgr``, the
bootstrap token mechanism is revised:

.. code-block:: text

   [ Secondary Operator ]                                   [ Primary Operator ]
             │                                                       │
             ├── 1. Mint Key via MON:                                │
             │      ceph fs authorize <fs> client.mirror_peer \      │
             │          /volumes rwps                                │
             │                                                       │
             ├── 2. Package via MGR (Read-Only Fetch):               │
             │      ceph fs mirror peer_bootstrap_create ...         │
             │                                                       │
             └───────────────────( Bootstrap Token )────────────────►│
                                                                     │
                                                                     └── 3. Import Token:
                                                                            ceph fs mirror peer_bootstrap_import ...

1. **Pre-Creation on MON:** The administrator explicitly provisions the key on the secondary cluster using
   direct MON authentication:

   .. code-block:: bash

      ceph fs authorize <sec_fs> client.mirror_peer /volumes rwps

   Caps can be scoped to the ``/volumes`` prefix to prevent access to underlying filesystem metadata.

2. **Fetch-Only MGR Packaging:** The MGR interface ``peer_bootstrap_create`` is stripped of ``auth
   get-or-create`` write privileges. It only executes a read-only fetch for the pre-existing entity name
   and encodes it into the bootstrap token.

3. **Primary Import:** The operator imports the token on the primary cluster via ``peer_bootstrap_import``
   as before.

Mirror Daemon Runtime: Libcephfs Connection Pooling
===================================================

Holding persistent, concurrent ``libcephfs`` connections across thousands of mirrored subvolumes is not
viable. The ``cephfs-mirror`` daemon will manage scoped mounts using an active handle pool:

* **Worker-Bound Concurrency:** Active mounts are bounded by the existing worker concurrency configuration
  (``cephfs_mirror_max_concurrent_directory_syncs``).
* **Handle Lifecycle (Borrow / Cooldown / Evict):**

  * When a sync job is scheduled, the worker thread requests a handle matching the target path/identity.
  * If a handle exists, it is reused (avoiding auth and session negotiation). If not, a new ``libcephfs``
    context is initialized using the scoped CephX key.
  * Upon sync completion, the handle enters an idle pool.
  * An idle-timeout and LRU eviction policy tears down idle contexts to reclaim MDS capabilities and
    memory.

Future Direction: Daemon-to-Daemon Push Replication
===================================================

The long-term architectural alternative to direct ``libcephfs`` cross-cluster mounting is a native
daemon-to-daemon receiver protocol:

* **Architecture:** Primary and secondary mirror daemons communicate directly over a dedicated,
  authenticated RPC channel.
* **Security Decoupling:** The primary daemon never authenticates against the secondary CephX layer. Local
  daemons run with local filesystem permissions, eliminating the transmission of CephX storage credentials
  across clusters.
* **Bi-directional Mirroring:** Provides a clean peer-to-peer foundation for bidirectional snapshot
  replication without symmetric credential exchange.
