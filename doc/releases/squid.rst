=====
Squid
=====

Squid is the 19th stable release of Ceph.

v19.2.0 Squid
=============

Ceph
~~~~

* ceph: a new `--daemon-output-file` switch is available for `ceph tell`
  commands to dump output to a file local to the daemon. For commands which
  produce large amounts of output, this avoids a potential spike in memory
  usage on the daemon, allows for faster streaming writes to a file local to
  the daemon, and reduces time holding any locks required to execute the
  command. For analysis, it is necessary to retrieve the file from the host
  running the daemon manually. Currently, only ``--format=json|json-pretty``
  are supported.
* ``cls_cxx_gather`` is marked as deprecated.
* Tracing: The blkin tracing feature (see
  https://docs.ceph.com/en/reef/dev/blkin/) is now deprecated in favor of
  Opentracing
  (https://docs.ceph.com/en/reef/dev/developer_guide/jaegertracing/) and will
  be removed in a later release.
* PG dump: The default output of ``ceph pg dump --format json`` has changed.
  The default JSON format produces a rather massive output in large clusters
  and isn't scalable. So we have removed the 'network_ping_times' section from
  the output. Details in the tracker: https://tracker.ceph.com/issues/57460

CephFS
~~~~~~

* CephFS: MDS evicts clients which are not advancing their request tids which
  causes a large buildup of session metadata resulting in the MDS going
  read-only due to the RADOS operation exceeding the size threshold.
  `mds_session_metadata_threshold` config controls the maximum size that a
  (encoded) session metadata can grow.
* CephFS: A new "mds last-seen" command is available for querying the last time
  an MDS was in the FSMap, subject to a pruning threshold.
* CephFS: For clusters with multiple CephFS file systems, all the snap-schedule
  commands now expect the '--fs' argument.
* CephFS: The period specifier ``m`` now implies minutes and the period
  specifier ``M`` now implies months. This has been made consistent with the
  rest of the system.
* CephFS: Running the command "ceph fs authorize" for an existing entity now
  upgrades the entity's capabilities instead of printing an error. It can now
  also change read/write permissions in a capability that the entity already
  holds. If the capability passed by user is same as one of the capabilities
  that the entity already holds, idempotency is maintained.
* CephFS: Two FS names can now be swapped, optionally along with their IDs,
  using "ceph fs swap" command. The function of this API is to facilitate
  file system swaps for disaster recovery. In particular, it avoids situations
  where a named file system is temporarily missing which would prompt a higher
  level storage operator (like Rook) to recreate the missing file system.
  See https://docs.ceph.com/en/latest/cephfs/administration/#file-systems
  docs for more information.
* CephFS: Before running the command "ceph fs rename", the filesystem to be
  renamed must be offline and the config "refuse_client_session" must be set
  for it. The config "refuse_client_session" can be removed/unset and
  filesystem can be online after the rename operation is complete.
* CephFS: Disallow delegating preallocated inode ranges to clients. Config
  `mds_client_delegate_inos_pct` defaults to 0 which disables async dirops
  in the kclient.
* CephFS: MDS log trimming is now driven by a separate thread which tries to
  trim the log every second (`mds_log_trim_upkeep_interval` config). Also, a
  couple of configs govern how much time the MDS spends in trimming its logs.
  These configs are `mds_log_trim_threshold` and `mds_log_trim_decay_rate`.
* CephFS: Full support for subvolumes and subvolume groups is now available
* CephFS: The `subvolume snapshot clone` command now depends on the config
  option `snapshot_clone_no_wait` which is used to reject the clone operation
  when all the cloner threads are busy. This config option is enabled by
  default which means that if no cloner threads are free, the clone request
  errors out with EAGAIN.  The value of the config option can be fetched by
  using: `ceph config get mgr mgr/volumes/snapshot_clone_no_wait` and it can be
  disabled by using: `ceph config set mgr mgr/volumes/snapshot_clone_no_wait
  false`
  for snap_schedule Manager module.
* CephFS: Commands ``ceph mds fail`` and ``ceph fs fail`` now require a
  confirmation flag when some MDSs exhibit health warning MDS_TRIM or
  MDS_CACHE_OVERSIZED. This is to prevent accidental MDS failover causing
  further delays in recovery.
* CephFS: fixes to the implementation of the ``root_squash`` mechanism enabled
  via cephx ``mds`` caps on a client credential require a new client feature
  bit, ``client_mds_auth_caps``. Clients using credentials with ``root_squash``
  without this feature will trigger the MDS to raise a HEALTH_ERR on the
  cluster, MDS_CLIENTS_BROKEN_ROOTSQUASH. See the documentation on this warning
  and the new feature bit for more information.
* CephFS: Expanded removexattr support for cephfs virtual extended attributes.
  Previously one had to use setxattr to restore the default in order to
  "remove".  You may now properly use removexattr to remove. You can also now
  remove layout on root inode, which then will restore layout to default
  layout.
* CephFS: cephfs-journal-tool is guarded against running on an online file
  system.  The 'cephfs-journal-tool --rank <fs_name>:<mds_rank> journal reset'
  and 'cephfs-journal-tool --rank <fs_name>:<mds_rank> journal reset --force'
  commands require '--yes-i-really-really-mean-it'.
* CephFS: "ceph fs clone status" command will now print statistics about clone
  progress in terms of how much data has been cloned (in both percentage as
  well as bytes) and how many files have been cloned.
* CephFS: "ceph status" command will now print a progress bar when cloning is
  ongoing. If clone jobs are more than the cloner threads, it will print one
  more progress bar that shows total amount of progress made by both ongoing
  as well as pending clones. Both progress are accompanied by messages that
  show number of clone jobs in the respective categories and the amount of
  progress made by each of them.
* cephfs-shell: The cephfs-shell utility is now packaged for RHEL 9 / CentOS 9
  as required python dependencies are now available in EPEL9.

CephX
~~~~~

* cephx: key rotation is now possible using `ceph auth rotate`. Previously,
  this was only possible by deleting and then recreating the key.

Dashboard
~~~~~~~~~

* Dashboard: Rearranged Navigation Layout: The navigation layout has been
  reorganized for improved usability and easier access to key features.
* Dashboard: CephFS Improvments
  * Support for managing CephFS snapshots and clones, as well as snapshot
    schedule management
  * Manage authorization capabilities for CephFS resources
  * Helpers on mounting a CephFS volume
* Dashboard: RGW Improvements
  * Support for managing bucket policies
  * Add/Remove bucket tags
  * ACL Management
  * Several UI/UX Improvements to the bucket form

MGR
~~~

* MGR/REST: The REST manager module will trim requests based on the
  'max_requests' option.  Without this feature, and in the absence of manual
  deletion of old requests, the accumulation of requests in the array can lead
  to Out Of Memory (OOM) issues, resulting in the Manager crashing.
* MGR: An OpTracker to help debug mgr module issues is now available.

Monitoring
~~~~~~~~~~

* Monitoring: Grafana dashboards are now loaded into the container at runtime
  rather than building a grafana image with the grafana dashboards. Official
  Ceph grafana images can be found in quay.io/ceph/grafana
* Monitoring: RGW S3 Analytics: A new Grafana dashboard is now available,
  enabling you to visualize per bucket and user analytics data, including total
  GETs, PUTs, Deletes, Copies, and list metrics.
* The ``mon_cluster_log_file_level`` and ``mon_cluster_log_to_syslog_level``
  options have been removed. Henceforth, users should use the new generic
  option ``mon_cluster_log_level`` to control the cluster log level verbosity
  for the cluster log file as well as for all external entities.

RADOS
~~~~~

* RADOS: ``A POOL_APP_NOT_ENABLED`` health warning will now be reported if the
  application is not enabled for the pool irrespective of whether the pool is
  in use or not. Always tag a pool with an application using ``ceph osd pool
  application enable`` command to avoid reporting of POOL_APP_NOT_ENABLED
  health warning for that pool. The user might temporarily mute this warning
  using ``ceph health mute POOL_APP_NOT_ENABLED``.
* RADOS: `get_pool_is_selfmanaged_snaps_mode` C++ API has been deprecated due
  to being prone to false negative results.  It's safer replacement is
  `pool_is_in_selfmanaged_snaps_mode`.
* RADOS: For bug 62338 (https://tracker.ceph.com/issues/62338), we did not
  choose to condition the fix on a server flag in order to simplify
  backporting.  As a result, in rare cases it may be possible for a PG to flip
  between two acting sets while an upgrade to a version with the fix is in
  progress.  If you observe this behavior, you should be able to work around it
  by completing the upgrade or by disabling async recovery by setting
  osd_async_recovery_min_cost to a very large value on all OSDs until the
  upgrade is complete: ``ceph config set osd osd_async_recovery_min_cost
  1099511627776``
* RADOS: A detailed version of the `balancer status` CLI command in the
  balancer module is now available. Users may run `ceph balancer status detail`
  to see more details about which PGs were updated in the balancer's last
  optimization.  See https://docs.ceph.com/en/latest/rados/operations/balancer/
  for more information.
* RADOS: Read balancing may now be managed automatically via the balancer
  manager module. Users may choose between two new modes: ``upmap-read``, which
  offers upmap and read optimization simultaneously, or ``read``, which may be
  used to only optimize reads. For more detailed information see
  https://docs.ceph.com/en/latest/rados/operations/read-balancer/#online-optimization.
* RADOS: BlueStore has been optimized for better performance in snapshot-intensive workloads.
* RADOS: BlueStore RocksDB LZ4 compression is now enabled by default to improve average
  performance and "fast device" space usage.
* RADOS: A new CRUSH rule type, MSR (Multi-Step Retry), allows for more flexible EC
  configurations.
* RADOS: Scrub scheduling behavior has been improved.

Crimson/Seastore
~~~~~~~~~~~~~~~~

* Crimson's first tech preview release!
  Supporting RBD workloads on Replicated pools.
  For more information please visit: https://ceph.io/en/news/crimson

RBD
~~~

* RBD: When diffing against the beginning of time (`fromsnapname == NULL`) in
  fast-diff mode (`whole_object == true` with `fast-diff` image feature enabled
  and valid), diff-iterate is now guaranteed to execute locally if exclusive
  lock is available.  This brings a dramatic performance improvement for QEMU
  live disk synchronization and backup use cases.
* RBD: The ``try-netlink`` mapping option for rbd-nbd has become the default
  and is now deprecated. If the NBD netlink interface is not supported by the
  kernel, then the mapping is retried using the legacy ioctl interface.
* RBD: The option ``--image-id`` has been added to `rbd children` CLI command,
  so it can be run for images in the trash.
* RBD: `Image::access_timestamp` and `Image::modify_timestamp` Python APIs now
  return timestamps in UTC.
* RBD: Support for cloning from non-user type snapshots is added.  This is
  intended primarily as a building block for cloning new groups from group
  snapshots created with `rbd group snap create` command, but has also been
  exposed via the new `--snap-id` option for `rbd clone` command.
* RBD: The output of `rbd snap ls --all` command now includes the original
  type for trashed snapshots.
* RBD: `RBD_IMAGE_OPTION_CLONE_FORMAT` option has been exposed in Python
  bindings via `clone_format` optional parameter to `clone`, `deep_copy` and
  `migration_prepare` methods.
* RBD: `RBD_IMAGE_OPTION_FLATTEN` option has been exposed in Python bindings
  via `flatten` optional parameter to `deep_copy` and `migration_prepare`
  methods.
* RBD: `rbd-wnbd` driver has gained the ability to multiplex image mappings.
  Previously, each image mapping spawned its own `rbd-wnbd` daemon, which lead
  to an excessive amount of TCP sessions and other resources being consumed,
  eventually exceeding Windows limits.  With this change, a single `rbd-wnbd`
  daemon is spawned per host and most OS resources are shared between image
  mappings.  Additionally, `ceph-rbd` service starts much faster.

RGW
~~~

* RGW: GetObject and HeadObject requests now return a x-rgw-replicated-at
  header for replicated objects. This timestamp can be compared against the
  Last-Modified header to determine how long the object took to replicate.
* RGW: S3 multipart uploads using Server-Side Encryption now replicate
  correctly in multi-site. Previously, the replicas of such objects were
  corrupted on decryption.  A new tool, ``radosgw-admin bucket resync encrypted
  multipart``, can be used to identify these original multipart uploads. The
  ``LastModified`` timestamp of any identified object is incremented by 1ns to
  cause peer zones to replicate it again.  For multi-site deployments that make
  any use of Server-Side Encryption, we recommended running this command
  against every bucket in every zone after all zones have upgraded.
* RGW: Introducing a new data layout for the Topic metadata associated with S3
  Bucket Notifications, where each Topic is stored as a separate RADOS object
  and the bucket notification configuration is stored in a bucket attribute.
  This new representation supports multisite replication via metadata sync and
  can scale to many topics. This is on by default for new deployments, but is
  is not enabled by default on upgrade. Once all radosgws have upgraded (on all
  zones in a multisite configuration), the ``notification_v2`` zone feature can
  be enabled to migrate to the new format. See
  https://docs.ceph.com/en/squid/radosgw/zone-features for details. The "v1"
  format is now considered deprecated and may be removed after 2 major releases.
* RGW: New tools have been added to radosgw-admin for identifying and
  correcting issues with versioned bucket indexes. Historical bugs with the
  versioned bucket index transaction workflow made it possible for the index
  to accumulate extraneous "book-keeping" olh entries and plain placeholder
  entries. In some specific scenarios where clients made concurrent requests
  referencing the same object key, it was likely that a lot of extra index
  entries would accumulate. When a significant number of these entries are
  present in a single bucket index shard, they can cause high bucket listing
  latencies and lifecycle processing failures. To check whether a versioned
  bucket has unnecessary olh entries, users can now run ``radosgw-admin
  bucket check olh``. If the ``--fix`` flag is used, the extra entries will
  be safely removed. A distinct issue from the one described thus far, it is
  also possible that some versioned buckets are maintaining extra unlinked
  objects that are not listable from the S3/ Swift APIs. These extra objects
  are typically a result of PUT requests that exited abnormally, in the middle
  of a bucket index transaction - so the client would not have received a
  successful response. Bugs in prior releases made these unlinked objects easy
  to reproduce with any PUT request that was made on a bucket that was actively
  resharding. Besides the extra space that these hidden, unlinked objects
  consume, there can be another side effect in certain scenarios, caused by
  the nature of the failure mode that produced them, where a client of a bucket
  that was a victim of this bug may find the object associated with the key to
  be in an inconsistent state. To check whether a versioned bucket has unlinked
  entries, users can now run ``radosgw-admin bucket check unlinked``. If the
  ``--fix`` flag is used, the unlinked objects will be safely removed. Finally,
  a third issue made it possible for versioned bucket index stats to be
  accounted inaccurately. The tooling for recalculating versioned bucket stats
  also had a bug, and was not previously capable of fixing these inaccuracies.
  This release resolves those issues and users can now expect that the existing
  ``radosgw-admin bucket check`` command will produce correct results. We
  recommend that users with versioned buckets, especially those that existed
  on prior releases, use these new tools to check whether their buckets are
  affected and to clean them up accordingly.
* RGW: The User Accounts feature unlocks several new AWS-compatible IAM APIs
  for the self-service management of users, keys, groups, roles, policy and
  more. Existing users can be adopted into new accounts. This process is
  optional but irreversible. See https://docs.ceph.com/en/squid/radosgw/account
  and https://docs.ceph.com/en/squid/radosgw/iam for details.
* RGW: On startup, radosgw and radosgw-admin now validate the ``rgw_realm``
  config option. Previously, they would ignore invalid or missing realms and go
  on to load a zone/zonegroup in a different realm. If startup fails with a
  "failed to load realm" error, fix or remove the ``rgw_realm`` option.
* RGW: The radosgw-admin commands ``realm create`` and ``realm pull`` no longer
  set the default realm without ``--default``.
* RGW: Fixed an S3 Object Lock bug with PutObjectRetention requests that
  specify a RetainUntilDate after the year 2106. This date was truncated to 32
  bits when stored, so a much earlier date was used for object lock
  enforcement.  This does not effect PutBucketObjectLockConfiguration where a
  duration is given in Days.  The RetainUntilDate encoding is fixed for new
  PutObjectRetention requests, but cannot repair the dates of existing object
  locks. Such objects can be identified with a HeadObject request based on the
  x-amz-object-lock-retain-until-date response header.
* S3 ``Get/HeadObject`` now supports the query parameter ``partNumber`` to read
  a specific part of a completed multipart upload.
* RGW: The SNS CreateTopic API now enforces the same topic naming requirements
  as AWS: Topic names must be made up of only uppercase and lowercase ASCII
  letters, numbers, underscores, and hyphens, and must be between 1 and 256
  characters long.
* RGW: Notification topics are now owned by the user that created them.  By
  default, only the owner can read/write their topics. Topic policy documents
  are now supported to grant these permissions to other users. Preexisting
  topics are treated as if they have no owner, and any user can read/write them
  using the SNS API.  If such a topic is recreated with CreateTopic, the
  issuing user becomes the new owner.  For backward compatibility, all users
  still have permission to publish bucket notifications to topics owned by
  other users. A new configuration parameter,
  ``rgw_topic_require_publish_policy``, can be enabled to deny ``sns:Publish``
  permissions unless explicitly granted by topic policy.
* RGW: Fix issue with persistent notifications where the changes to topic param
  that were modified while persistent notifications were in the queue will be
  reflected in notifications.  So if user sets up topic with incorrect config
  (password/ssl) causing failure while delivering the notifications to broker,
  can now modify the incorrect topic attribute and on retry attempt to delivery
  the notifications, new configs will be used.
* RGW: in bucket notifications, the ``principalId`` inside ``ownerIdentity``
  now contains the complete user ID, prefixed with the tenant ID.

Telemetry
~~~~~~~~~

* The ``basic`` channel in telemetry now captures pool flags that allows us to
  better understand feature adoption, such as Crimson. 
  To opt in to telemetry, run ``ceph telemetry on``.
