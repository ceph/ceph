===============
 RBD Mirroring
===============

.. index:: Ceph Block Device; mirroring

RBD images can be asynchronously mirrored between two Ceph clusters. This
capability is available in two modes:

* **Journal-based**: This mode uses the RBD journaling image feature to ensure
  point-in-time, crash-consistent replication between clusters. Every write to
  the RBD image is first recorded to the associated journal before modifying the
  actual image. The remote cluster will read from this associated journal and
  replay the updates to its local copy of the image. Since each write to the
  RBD image will result in two writes to the Ceph cluster, expect write
  latencies to nearly double while using the RBD journaling image feature.

* **Snapshot-based**: This mode uses periodically scheduled or manually
  created RBD image mirror-snapshots to replicate crash-consistent RBD images
  between clusters. The remote cluster will determine any data or metadata
  updates between two mirror-snapshots and copy the deltas to its local copy of
  the image. With the help of the RBD ``fast-diff`` image feature, updated data
  blocks can be quickly determined without the need to scan the full RBD image.
  Since this mode is not as fine-grained as journaling, the complete delta 
  between two snapshots will need to be synced prior to use during a failover
  scenario. Any partially applied set of deltas will be rolled back at moment
  of failover.

.. note:: journal-based mirroring requires the Ceph Jewel release or later;
   snapshot-based mirroring requires the Ceph Octopus release or later.

Mirroring is configured on a per-pool basis within peer clusters and can be
configured on a specific subset of images within the pool.  You can also mirror
all images within a given pool when using journal-based
mirroring. Mirroring is configured using the ``rbd`` command. The
``rbd-mirror`` daemon is responsible for pulling image updates from the remote
peer cluster and applying them to the image within the local cluster.

Depending on the desired needs for replication, RBD mirroring can be configured
for either one- or two-way replication:

* **One-way Replication**: When data is only mirrored from a primary cluster to
  a secondary cluster, the ``rbd-mirror`` daemon runs only on the secondary
  cluster.

* **Two-way Replication**: When data is mirrored from primary images on one
  cluster to non-primary images on another cluster (and vice-versa), the
  ``rbd-mirror`` daemon runs on both clusters.

.. important:: Each instance of the ``rbd-mirror`` daemon must be able to
   connect to both the local and remote Ceph clusters simultaneously (i.e.
   all monitor and OSD hosts). Additionally, the network must have sufficient
   bandwidth between the two data centers to handle mirroring workload.

Pool Configuration
==================

The following procedures demonstrate how to perform the basic administrative
tasks to configure mirroring using the ``rbd`` command. Mirroring is
configured on a per-pool basis.

These pool configuration steps should be performed on both peer clusters. These
procedures assume that both clusters, named "site-a" and "site-b", are accessible
from a single host for clarity.

See the `rbd`_ manpage for additional details of how to connect to different
Ceph clusters.

.. note:: The cluster name in the following examples corresponds to a Ceph
   configuration file of the same name (e.g. /etc/ceph/site-b.conf).  See the
   `ceph-conf`_ documentation for how to configure multiple clusters.  Note
   that ``rbd-mirror`` does **not** require the source and destination clusters
   to have unique internal names; both can and should call themselves ``ceph``.
   The config `files` that ``rbd-mirror`` needs for local and remote clusters
   can be named arbitrarily, and containerizing the daemon is one strategy
   for maintaining them outside of ``/etc/ceph`` to avoid confusion.

Enable Mirroring
----------------

To enable mirroring on a pool with ``rbd``, issue the ``mirror pool enable``
subcommand with the pool name, the mirroring mode, and an optional friendly
site name to describe the local cluster::

        rbd mirror pool enable [--site-name {local-site-name}] {pool-name} {mode}

The mirroring mode can either be ``image`` or ``pool``:

* **image**: When configured in ``image`` mode, mirroring must
  `explicitly enabled`_ on each image.
* **pool** (default):  When configured in ``pool`` mode, all images in the pool
  with the journaling feature enabled are mirrored.

For example::

        $ rbd --cluster site-a mirror pool enable --site-name site-a image-pool image
        $ rbd --cluster site-b mirror pool enable --site-name site-b image-pool image

The site name can also be specified when creating or importing a new
`bootstrap token`_.

The site name can be changed later using the same ``mirror pool enable``
subcommand but note that the local site name and the corresponding site name
used by the remote cluster generally must match.

Disable Mirroring
-----------------

To disable mirroring on a pool with ``rbd``, specify the ``mirror pool disable``
command and the pool name::

        rbd mirror pool disable {pool-name}

When mirroring is disabled on a pool in this way, mirroring will also be
disabled on any images (within the pool) for which mirroring was enabled
explicitly.

For example::

        $ rbd --cluster site-a mirror pool disable image-pool
        $ rbd --cluster site-b mirror pool disable image-pool

Bootstrap Peers
---------------

In order for the ``rbd-mirror`` daemon to discover its peer cluster, the peer
must be registered and a user account must be created.
This process can be automated with ``rbd`` and the
``mirror pool peer bootstrap create`` and ``mirror pool peer bootstrap import``
commands.

To manually create a new bootstrap token with ``rbd``, issue the
``mirror pool peer bootstrap create`` subcommand, a pool name, and an
optional friendly site name to describe the local cluster::

        rbd mirror pool peer bootstrap create [--site-name {local-site-name}] {pool-name}

The output of ``mirror pool peer bootstrap create`` will be a token that should
be provided to the ``mirror pool peer bootstrap import`` command. For example,
on site-a::

        $ rbd --cluster site-a mirror pool peer bootstrap create --site-name site-a image-pool
        eyJmc2lkIjoiOWY1MjgyZGItYjg5OS00NTk2LTgwOTgtMzIwYzFmYzM5NmYzIiwiY2xpZW50X2lkIjoicmJkLW1pcnJvci1wZWVyIiwia2V5IjoiQVFBUnczOWQwdkhvQmhBQVlMM1I4RmR5dHNJQU50bkFTZ0lOTVE9PSIsIm1vbl9ob3N0IjoiW3YyOjE5Mi4xNjguMS4zOjY4MjAsdjE6MTkyLjE2OC4xLjM6NjgyMV0ifQ==

To manually import the bootstrap token created by another cluster with ``rbd``,
specify the ``mirror pool peer bootstrap import`` command, the pool name, a file
path to the created token (or '-' to read from standard input), along with an
optional friendly site name to describe the local cluster and a mirroring
direction (defaults to rx-tx for bidirectional mirroring, but can also be set
to rx-only for unidirectional mirroring)::

        rbd mirror pool peer bootstrap import [--site-name {local-site-name}] [--direction {rx-only or rx-tx}] {pool-name} {token-path}

For example, on site-b::

        $ cat <<EOF > token
        eyJmc2lkIjoiOWY1MjgyZGItYjg5OS00NTk2LTgwOTgtMzIwYzFmYzM5NmYzIiwiY2xpZW50X2lkIjoicmJkLW1pcnJvci1wZWVyIiwia2V5IjoiQVFBUnczOWQwdkhvQmhBQVlMM1I4RmR5dHNJQU50bkFTZ0lOTVE9PSIsIm1vbl9ob3N0IjoiW3YyOjE5Mi4xNjguMS4zOjY4MjAsdjE6MTkyLjE2OC4xLjM6NjgyMV0ifQ==
        EOF
        $ rbd --cluster site-b mirror pool peer bootstrap import --site-name site-b image-pool token

Add Cluster Peer Manually
-------------------------

Cluster peers can be specified manually if desired or if the above bootstrap
commands are not available with the currently installed Ceph release.

The remote ``rbd-mirror`` daemon will need access to the local cluster to
perform mirroring. A new local Ceph user should be created for the remote
daemon to use. To `create a Ceph user`_, with ``ceph`` specify the
``auth get-or-create`` command, user name, monitor caps, and OSD caps::

        $ ceph auth get-or-create client.rbd-mirror-peer mon 'profile rbd-mirror-peer' osd 'profile rbd'

The resulting keyring should be copied to the other cluster's ``rbd-mirror``
daemon hosts if not using the Ceph monitor ``config-key`` store described below.

To manually add a mirroring peer Ceph cluster with ``rbd``, specify the
``mirror pool peer add`` command, the pool name, and a cluster specification::

        rbd mirror pool peer add {pool-name} {client-name}@{cluster-name}

For example::

        $ rbd --cluster site-a mirror pool peer add image-pool client.rbd-mirror-peer@site-b
        $ rbd --cluster site-b mirror pool peer add image-pool client.rbd-mirror-peer@site-a

By default, the ``rbd-mirror`` daemon needs to have access to a Ceph
configuration file located at ``/etc/ceph/{cluster-name}.conf`` that provides
the addresses of the peer cluster's monitors, in addition to a keyring for
``{client-name}`` located in the default or configured keyring search paths
(e.g. ``/etc/ceph/{cluster-name}.{client-name}.keyring``).

Alternatively, the peer cluster's monitor and/or client key can be securely
stored within the local Ceph monitor ``config-key`` store. To specify the
peer cluster connection attributes when adding a mirroring peer, use the
``--remote-mon-host`` and ``--remote-key-file`` optionals. For example::

        $ cat <<EOF > remote-key-file
        AQAeuZdbMMoBChAAcj++/XUxNOLFaWdtTREEsw==
        EOF
        $ rbd --cluster site-a mirror pool peer add image-pool client.rbd-mirror-peer@site-b --remote-mon-host 192.168.1.1,192.168.1.2 --remote-key-file remote-key-file
        $ rbd --cluster site-a mirror pool info image-pool --all
        Mode: pool
        Peers: 
          UUID                                 NAME   CLIENT                 MON_HOST                KEY                                      
          587b08db-3d33-4f32-8af8-421e77abb081 site-b client.rbd-mirror-peer 192.168.1.1,192.168.1.2 AQAeuZdbMMoBChAAcj++/XUxNOLFaWdtTREEsw== 

Remove Cluster Peer
-------------------

To remove a mirroring peer Ceph cluster with ``rbd``, specify the
``mirror pool peer remove`` command, the pool name, and the peer UUID
(available from the ``rbd mirror pool info`` command)::

        rbd mirror pool peer remove {pool-name} {peer-uuid}

For example::

        $ rbd --cluster site-a mirror pool peer remove image-pool 55672766-c02b-4729-8567-f13a66893445
        $ rbd --cluster site-b mirror pool peer remove image-pool 60c0e299-b38f-4234-91f6-eed0a367be08

Data Pools
----------

When creating images in the destination cluster, ``rbd-mirror`` selects a data
pool as follows:

#. If the destination cluster has a default data pool configured (with the
   ``rbd_default_data_pool`` configuration option), it will be used.
#. Otherwise, if the source image uses a separate data pool, and a pool with the
   same name exists on the destination cluster, that pool will be used.
#. If neither of the above is true, no data pool will be set.

Image Configuration
===================

Unlike pool configuration, image configuration only needs to be performed
against a single mirroring peer Ceph cluster.

Mirrored RBD images are designated as either primary or non-primary. This is a
property of the image and not the pool. Images that are designated as
non-primary cannot be modified.

Images are automatically promoted to primary when mirroring is first enabled on
an image (either implicitly if the pool mirror mode was ``pool`` and the image
has the journaling image feature enabled, or `explicitly enabled`_ by the
``rbd`` command if the pool mirror mode was ``image``).

Enable Image Mirroring
----------------------

If mirroring is configured in ``image`` mode for the image's pool, then it
is necessary to explicitly enable mirroring for each image within the pool.
To enable mirroring for a specific image with ``rbd``, specify the
``mirror image enable`` command along with the pool, image name, and mode::

        rbd mirror image enable {pool-name}/{image-name} {mode}

The mirror image mode can either be ``journal`` or ``snapshot``:

* **journal** (default): When configured in ``journal`` mode, mirroring will
  utilize the RBD journaling image feature to replicate the image contents. If
  the RBD journaling image feature is not yet enabled on the image, it will be
  automatically enabled.

* **snapshot**:  When configured in ``snapshot`` mode, mirroring will utilize
  RBD image mirror-snapshots to replicate the image contents. Once enabled, an
  initial mirror-snapshot will automatically be created. Additional RBD image
  `mirror-snapshots`_ can be created by the ``rbd`` command.

For example::

        $ rbd --cluster site-a mirror image enable image-pool/image-1 snapshot
        $ rbd --cluster site-a mirror image enable image-pool/image-2 journal

Enable Image Journaling Feature
-------------------------------

RBD journal-based mirroring uses the RBD image journaling feature to ensure that
the replicated image always remains crash-consistent. When using the ``image``
mirroring mode, the journaling feature will be automatically enabled when
mirroring is enabled on the image. When using the ``pool`` mirroring mode,
before an image can be mirrored to a peer cluster, the RBD image journaling
feature must be enabled. The feature can be enabled at image creation time by
providing the ``--image-feature exclusive-lock,journaling`` option to the
``rbd`` command.

Alternatively, the journaling feature can be dynamically enabled on
pre-existing RBD images. To enable journaling with ``rbd``, specify
the ``feature enable`` command, the pool and image name, and the feature name::

        rbd feature enable {pool-name}/{image-name} {feature-name}

For example::

        $ rbd --cluster site-a feature enable image-pool/image-1 journaling

.. note:: The journaling feature is dependent on the exclusive-lock feature. If
   the exclusive-lock feature is not already enabled, it should be enabled prior
   to enabling the journaling feature.

.. tip:: You can enable journaling on all new images by default by adding
   ``rbd default features = 125`` to your Ceph configuration file.

.. tip:: ``rbd-mirror`` tunables are set by default to values suitable for
   mirroring an entire pool.  When using ``rbd-mirror`` to migrate single
   volumes between clusters you may achieve substantial performance gains
   by setting ``rbd_journal_max_payload_bytes=8388608`` within the ``[client]``
   config section of the local or centralized configuration.  Note that this
   setting may allow ``rbd-mirror`` to present a substantial write workload
   to the destination cluster:  monitor cluster performance closely during
   migrations and test carefully before running multiple migrations in parallel.

Create Image Mirror-Snapshots
-----------------------------

When using snapshot-based mirroring, mirror-snapshots will need to be created
whenever it is desired to mirror the changed contents of the RBD image. To
create a mirror-snapshot manually with ``rbd``, specify the
``mirror image snapshot`` command along with the pool and image name::

        rbd mirror image snapshot {pool-name}/{image-name}

For example::

        $ rbd --cluster site-a mirror image snapshot image-pool/image-1

By default up to ``5`` mirror-snapshots will be created per-image. The most
recent mirror-snapshot is automatically pruned if the limit is reached.
The limit can be overridden via the ``rbd_mirroring_max_mirroring_snapshots``
configuration option if required. Additionally, mirror-snapshots are
automatically deleted when the image is removed or when mirroring is disabled.

Mirror-snapshots can also be automatically created on a periodic basis if
mirror-snapshot schedules are defined. The mirror-snapshot can be scheduled
globally, per-pool, or per-image levels. Multiple mirror-snapshot schedules can
be defined at any level, but only the most-specific snapshot schedules that
match an individual mirrored image will run.

To create a mirror-snapshot schedule with ``rbd``, specify the
``mirror snapshot schedule add`` command along with an optional pool or
image name; interval; and optional start time::

        rbd mirror snapshot schedule add [--pool {pool-name}] [--image {image-name}] {interval} [{start-time}]

The ``interval`` can be specified in days, hours, or minutes using ``d``, ``h``,
``m`` suffix respectively. The optional ``start-time`` can be specified using
the ISO 8601 time format. For example::

        $ rbd --cluster site-a mirror snapshot schedule add --pool image-pool 24h 14:00:00-05:00
        $ rbd --cluster site-a mirror snapshot schedule add --pool image-pool --image image1 6h

To remove a mirror-snapshot schedules with ``rbd``, specify the
``mirror snapshot schedule remove`` command with options that match the
corresponding ``add`` schedule command.

To list all snapshot schedules for a specific level (global, pool, or image)
with ``rbd``, specify the ``mirror snapshot schedule ls`` command along with
an optional pool or image name. Additionally, the ``--recursive`` option can
be specified to list all schedules at the specified level and below. For
example::

        $ rbd --cluster site-a mirror snapshot schedule ls --pool image-pool --recursive
        POOL        NAMESPACE IMAGE  SCHEDULE                            
        image-pool  -         -      every 1d starting at 14:00:00-05:00 
        image-pool            image1 every 6h                            

To view the status for when the next snapshots will be created for
snapshot-based mirroring RBD images with ``rbd``, specify the
``mirror snapshot schedule status`` command along with an optional pool or
image name::

        rbd mirror snapshot schedule status [--pool {pool-name}] [--image {image-name}]

For example::

        $ rbd --cluster site-a mirror snapshot schedule status
        SCHEDULE TIME       IMAGE             
        2020-02-26 18:00:00 image-pool/image1 

Disable Image Mirroring
-----------------------

To disable mirroring for a specific image with ``rbd``, specify the
``mirror image disable`` command along with the pool and image name::

        rbd mirror image disable {pool-name}/{image-name}

For example::

        $ rbd --cluster site-a mirror image disable image-pool/image-1

Image Promotion and Demotion
----------------------------

In a failover scenario where the primary designation needs to be moved to the
image in the peer Ceph cluster, access to the primary image should be stopped
(e.g. power down the VM or remove the associated drive from a VM), demote the
current primary image, promote the new primary image, and resume access to the
image on the alternate cluster.

.. note:: RBD only provides the necessary tools to facilitate an orderly
   failover of an image. An external mechanism is required to coordinate the
   full failover process (e.g. closing the image before demotion).

To demote a specific image to non-primary with ``rbd``, specify the
``mirror image demote`` command along with the pool and image name::

        rbd mirror image demote {pool-name}/{image-name}

For example::

        $ rbd --cluster site-a mirror image demote image-pool/image-1

To demote all primary images within a pool to non-primary with ``rbd``, specify
the ``mirror pool demote`` command along with the pool name::

        rbd mirror pool demote {pool-name}

For example::

        $ rbd --cluster site-a mirror pool demote image-pool

To promote a specific image to primary with ``rbd``, specify the
``mirror image promote`` command along with the pool and image name::

        rbd mirror image promote [--force] {pool-name}/{image-name}

For example::

        $ rbd --cluster site-b mirror image promote image-pool/image-1

To promote all non-primary images within a pool to primary with ``rbd``, specify
the ``mirror pool promote`` command along with the pool name::

        rbd mirror pool promote [--force] {pool-name}

For example::

        $ rbd --cluster site-a mirror pool promote image-pool

.. tip:: Since the primary / non-primary status is per-image, it is possible to
   have two clusters split the IO load and stage failover / failback.

.. note:: Promotion can be forced using the ``--force`` option. Forced
   promotion is needed when the demotion cannot be propagated to the peer
   Ceph cluster (e.g. Ceph cluster failure, communication outage). This will
   result in a split-brain scenario between the two peers and the image will no
   longer be in-sync until a `force resync command`_ is issued.

Force Image Resync
------------------

If a split-brain event is detected by the ``rbd-mirror`` daemon, it will not
attempt to mirror the affected image until corrected. To resume mirroring for an
image, first `demote the image`_ determined to be out-of-date and then request a
resync to the primary image. To request an image resync with ``rbd``, specify
the ``mirror image resync`` command along with the pool and image name::

        rbd mirror image resync {pool-name}/{image-name}

For example::

        $ rbd mirror image resync image-pool/image-1

.. note:: The ``rbd`` command only flags the image as requiring a resync. The
   local cluster's ``rbd-mirror`` daemon process is responsible for performing
   the resync asynchronously.

Mirror Status
=============

The peer cluster replication status is stored for every primary mirrored image.
This status can be retrieved using the ``mirror image status`` and
``mirror pool status`` commands.

To request the mirror image status with ``rbd``, specify the
``mirror image status`` command along with the pool and image name::

        rbd mirror image status {pool-name}/{image-name}

For example::

        $ rbd mirror image status image-pool/image-1

To request the mirror pool summary status with ``rbd``, specify the
``mirror pool status`` command along with the pool name::

        rbd mirror pool status {pool-name}

For example::

        $ rbd mirror pool status image-pool

.. note:: Adding ``--verbose`` option to the ``mirror pool status`` command will
   additionally output status details for every mirroring image in the pool.

rbd-mirror Daemon
=================

The two ``rbd-mirror`` daemons are responsible for watching image journals on
the remote, peer cluster and replaying the journal events against the local
cluster. The RBD image journaling feature records all modifications to the
image in the order they occur. This ensures that a crash-consistent mirror of
the remote image is available locally.

The ``rbd-mirror`` daemon is available within the optional ``rbd-mirror``
distribution package.

.. important:: Each ``rbd-mirror`` daemon requires the ability to connect
   to both clusters simultaneously.
.. warning:: Pre-Luminous releases: only run a single ``rbd-mirror`` daemon per
   Ceph cluster.

Each ``rbd-mirror`` daemon should use a unique Ceph user ID. To
`create a Ceph user`_, with ``ceph`` specify the ``auth get-or-create``
command, user name, monitor caps, and OSD caps::

  ceph auth get-or-create client.rbd-mirror.{unique id} mon 'profile rbd-mirror' osd 'profile rbd'

The ``rbd-mirror`` daemon can be managed by ``systemd`` by specifying the user
ID as the daemon instance::

  systemctl enable ceph-rbd-mirror@rbd-mirror.{unique id}

The ``rbd-mirror`` can also be run in foreground by ``rbd-mirror`` command::

  rbd-mirror -f --log-file={log_path}

.. _rbd: ../../man/8/rbd
.. _ceph-conf: ../../rados/configuration/ceph-conf/#running-multiple-clusters
.. _explicitly enabled: #enable-image-mirroring
.. _bootstrap token: #bootstrap-peers
.. _force resync command: #force-image-resync
.. _demote the image: #image-promotion-and-demotion
.. _create a Ceph user: ../../rados/operations/user-management#add-a-user
.. _mirror-snapshots: #create-image-mirror-snapshots
