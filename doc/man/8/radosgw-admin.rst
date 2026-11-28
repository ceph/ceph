:orphan:

.. _man-radosgw-admin:

=================================================================
 radosgw-admin -- rados REST gateway user administration utility
=================================================================

.. program:: radosgw-admin

Synopsis
========

| **radosgw-admin** *command* [ *options* *...* ]


Description
===========

:program:`radosgw-admin` is a Ceph Object Gateway user administration utility. It
is used to create and modify users.


Commands
========

:program:`radosgw-admin` utility provides commands for administration purposes
as follows:

:command:`user create`
  Create a new user.

:command:`user modify`
  Modify a user.

:command:`user info`
  Display information for a user including any subusers and keys.

:command:`user rename`
  Renames a user.

:command:`user rm`
  Remove a user.

:command:`user suspend`
  Suspend a user.

:command:`user enable`
  Re-enable user after suspension.

:command:`user check`
  Check user info.

:command:`user stats`
  Show user stats as accounted by the quota subsystem.

:command:`user list`
  List all users.

:command:`user policy attach`
  Attach the managed policy given with --policy-arn to a user that belongs to
  an account.

:command:`user policy detach`
  Detach a managed policy from a user.

:command:`user policy list attached`
  List the managed policies that are attached to a user.

:command:`caps add`
  Add user capabilities.

:command:`caps rm`
  Remove user capabilities.

:command:`subuser create`
  Create a new subuser (primarily useful for clients using the Swift API).

:command:`subuser modify`
  Modify a subuser.

:command:`subuser rm`
  Remove a subuser.

:command:`key create`
  Create access key.

:command:`key rm`
  Remove access key.

:command:`account create`
  Create an account. See https://docs.ceph.com/en/latest/radosgw/account/.

:command:`account modify`
  Modify an account.

:command:`account get`
  Show an account's information.

:command:`account stats`
  Show an account's storage stats. With --sync-stats, first update them from
  the account's buckets.

:command:`account rm`
  Remove an account. With --purge-data, also remove the users, buckets, and
  other resources that the account owns, which otherwise must be removed first.

:command:`account list`
  List the IDs of all accounts.

:command:`bucket list`
  List buckets, or, if a bucket is specified with --bucket=<bucket>,
  list its objects. Adding --allow-unordered
  removes the ordering requirement, possibly generating results more
  quickly for buckets with large number of objects.
  Use --marker to paginate through object listings (requires ordered listing;
  do not use with --allow-unordered). For versioned buckets, also specify
  --object-version with the instance/version ID to resume from a specific version
  (e.g., ``--marker=obj1 --object-version=abc123``).

:command:`bucket limit check`
  Show bucket sharding stats. With no filters, checks all buckets for all users.
  Pass ``--uid`` to limit to one user's buckets, or ``--bucket`` (and optional
  ``--tenant``) to check a single bucket. ``--warnings-only`` lists only buckets
  nearing or over the per-shard object limit. Without filters, a full scan on a
  large deployment, or when ``.meta`` and ``.index`` pools sit on slow media, can
  take several minutes.

:command:`bucket link`
  Link bucket to specified user.

:command:`bucket unlink`
  Unlink bucket from specified user.

:command:`bucket chown`
  Change bucket ownership to the specified user and update object ACLs. 
  Invoke with --marker to resume if the command is interrupted.

:command:`bucket stats`
  List bucket statistics plus other internal information about a bucket.

:command:`bucket suspend`
  Suspend an individual bucket. S3 requests against the bucket fail with
  ``403 BucketSuspended``.

:command:`bucket unsuspend`
  Unsuspend a previously suspended bucket.

:command:`bucket rm`
  Remove a bucket.

:command:`bucket check`
  Check bucket index.

:command:`bucket rewrite`
  Rewrite all objects in the specified bucket.

:command:`bucket radoslist`
  List the RADOS objects that contain the data for all objects in
  the designated bucket, if --bucket=<bucket> is specified. 
  Otherwise, list the RADOS objects that contain data for all 
  buckets.

:command:`bucket reshard`
  Reshard a bucket's index.

:command:`bucket sync checkpoint`
  Wait until a bucket's sync catches up with its source zones, checking every
  --retry-delay-ms until --timeout-sec runs out.

:command:`bucket sync info`
  Show the zones and buckets from which a bucket syncs.

:command:`bucket sync status`
  Show a bucket's sync status with each of its source zones, or with the one
  given with --source-zone.

:command:`bucket sync markers`
  Show a bucket's sync markers for the source zone given with --source-zone.

:command:`bucket sync init`
  Initialize a bucket's sync status for the source zone given with
  --source-zone, so that its sync from that zone starts over.

:command:`bucket sync run`
  Run a bucket's sync from the source zone given with --source-zone.

:command:`bucket sync disable`
  Disable bucket sync.

:command:`bucket sync enable`
  Enable bucket sync.

:command:`bucket logging list`
  List pending log objects for a source bucket.

:command:`bucket logging info`
  Show logging configuration for a source bucket, or list source buckets
  logging to a specific log bucket.

:command:`bucket logging flush`
  Flush pending log objects to the log bucket immediately.

:command:`bi get`
  Retrieve bucket index object entries.

:command:`bi put`
  Store bucket index object entries.

:command:`bi list`
  List raw bucket index entries.

:command:`bi purge`
  Purge bucket index entries.

:command:`object rm`
  Remove an S3/Swift object. Include "--yes-i-really-mean-it" to remove object's
  entry from bucket index, for example if it is damaged.

:command:`object stat`
  Stat an S3/Swift object for its metadata.

:command:`object manifest`
  Display the manifest of an S3/Swift object, producing a list of RADOS objects containing the data.

:command:`object unlink`
  Unlink S3/Swift object from bucket index.

:command:`object rewrite`
  Rewrite the specified S3/Swift object.

:command:`object reindex`
  Add an S3/Swift object to its bucket's index. Used rarely for emergency repairs.

:command:`objects expire`
  Run expired objects cleanup.

:command:`period delete`
  Remove a period.

:command:`period get`
  Get the period info.

:command:`period get-current`
  Get the current period info.

:command:`period pull`
  Pull a period.

:command:`period push`
  Push a period.

:command:`period list`
  List all periods.

:command:`period update`
  Update the staging period.

:command:`period commit`
  Commit the staging period.

:command:`quota set`
  Set quota params.

:command:`quota enable`
  Enable quota.

:command:`quota disable`
  Disable quota.

:command:`global quota get`
  View global quota parameters.

:command:`global quota set`
  Set global quota parameters.

:command:`global quota enable`
  Enable a global quota.

:command:`global quota disable`
  Disable a global quota.

:command:`realm create`
  Create a new realm.

:command:`realm rm`
  Remove a realm.

:command:`realm get`
  Show the realm info.

:command:`realm get-default`
  Get the default realm name.

:command:`realm list`
  List all realms.

:command:`realm list-periods`
  List all realm periods.

:command:`realm rename`
  Rename a realm.

:command:`realm set`
  Set the realm info (requires infile).

:command:`realm default`
  Set the realm as default.

:command:`realm pull`
  Pull a realm and its current period.

:command:`zonegroup add`
  Add a zone to a zonegroup.

:command:`zonegroup create`
  Create a new zone group info.

:command:`zonegroup default`
  Set the default zone group.

:command:`zonegroup delete`
  Remove a zone group info.

:command:`zonegroup get`
  Show the zone group info.

:command:`zonegroup modify`
  Modify an existing zonegroup.

:command:`zonegroup set`
  Set the zone group info (requires infile).

:command:`zonegroup remove`
  Remove a zone from a zonegroup.

:command:`zonegroup rename`
  Rename a zone group.

:command:`zonegroup list`
  List all zone groups set on this cluster.

:command:`zonegroup placement list`
  List zonegroup's placement targets.

:command:`zonegroup placement add`
  Add a placement target id to a zonegroup.

:command:`zonegroup placement modify`
  Modify a placement target of a specific zonegroup.

:command:`zonegroup placement rm`
  Remove a placement target from a zonegroup.

:command:`zonegroup placement default`
  Set a zonegroup's default placement target.

:command:`zone create`
  Create a new zone.

:command:`zone delete`
  Remove a zone.

:command:`zone get`
  Show zone cluster params.

:command:`zone set`
  Set zone cluster params (requires infile).

:command:`zone modify`
  Modify an existing zone.

:command:`zone list`
  List all zones set on this cluster.

:command:`metadata sync status`
  Get metadata sync status.

:command:`metadata sync init`
  Init metadata sync.

:command:`metadata sync run`
  Run metadata sync.

:command:`data sync status`
  Get data sync status of the specified source zone.
  
:command:`data sync init`
  Init data sync for the specified source zone.

:command:`data sync run`
  Run data sync for the specified source zone.

:command:`sync status`
  Show a summary of the multisite sync of this zone: its metadata sync, and its
  data sync from each source zone.

:command:`sync error list`
  List sync errors.

:command:`sync error trim`
  Trim sync errors.

:command:`sync policy get`
  Show the zonegroup's sync policy or, with --bucket, a bucket's sync policy.
  See https://docs.ceph.com/en/latest/radosgw/multisite-sync-policy/.

:command:`sync group create`
  Create a sync policy group.

:command:`sync group modify`
  Modify the status of a sync policy group.

:command:`sync group get`
  Show the sync policy groups, or the one given with --group-id.

:command:`sync group remove`
  Remove a sync policy group.

:command:`sync group flow create`
  Create or update a data flow in a sync policy group.

:command:`sync group flow remove`
  Remove a data flow, or some of its zones, from a sync policy group.

:command:`sync group pipe create`
  Create a sync pipe in a sync policy group, or update its parameters.

:command:`sync group pipe modify`
  Modify the parameters of an existing sync pipe.

:command:`sync group pipe remove`
  Remove a sync pipe, or some of its parameters.

:command:`sync info`
  Show the sync sources and destinations that the sync policy gives this zone
  or, with --bucket, a bucket.

:command:`zone rename`
  Rename a zone.

:command:`zone placement list`
  List a zone's placement targets.

:command:`zone placement add`
  Add a zone placement target.

:command:`zone placement modify`
  Modify a zone placement target.

:command:`zone placement rm`
  Remove a zone placement target.

:command:`policy`
  Display bucket/object policies (e.g. permissions/ACLs etc.).

:command:`log list`
  List log objects.

:command:`log show`
  Dump a log from specific object or (bucket + date + bucket-id).
  (NOTE: required to specify formatting of the date as "YYYY-MM-DD-hh")

:command:`log rm`
  Remove log object.

:command:`usage show`
  Show the usage information (with optional user and date range).

:command:`usage trim`
  Trim usage information (with optional user and date range).

:command:`gc list`
  Dump expired garbage collection objects (specify --include-all to list all
  entries, including unexpired).

:command:`gc process`
  Manually process garbage.

:command:`lc get`
  Get lifecycle config for a bucket.

:command:`lc list`
  List all bucket lifecycle progress.

:command:`lc process`
  Manually process lifecycle transitions.  If a bucket is specified (e.g., via
  --bucket_id or via --bucket and optional --tenant), only that bucket
  is processed.

:command:`metadata get`
  Get metadata info.

:command:`metadata put`
  Put metadata info.

:command:`metadata rm`
  Remove metadata info.

:command:`metadata list`
  List metadata info.

:command:`mdlog list`
  List metadata log which is needed for multi-site deployments.

:command:`mdlog autotrim`
  Trim the metadata log once, the way the gateway's integrated log trimming
  does.

:command:`mdlog trim`
  Trim metadata log manually instead of relying on the gateway's integrated log sync.
  Before trimming, compare the listings and make sure the last sync was
  complete, otherwise it can reinitiate a sync.

:command:`mdlog status`
  Read metadata log status.

:command:`bilog list`
  List bucket index log which is needed for multi-site deployments.

:command:`bilog trim`
  Trim bucket index log (use end-marker) manually instead
  of relying on the gateway's integrated log sync.
  Before trimming, compare the listings and make sure the last sync was
  complete, otherwise it can reinitiate a sync. Without an end-marker the
  trim has no upper bound, so it requires --yes-i-really-mean-it.

:command:`bilog status`
  Show the bucket index log markers of each shard of a bucket.

:command:`bilog autotrim`
  Trim the bucket index logs once, the way the gateway's integrated log
  trimming does. In a zone that does not export data, such as an archive zone,
  it requires --yes-i-really-mean-it.

:command:`datalog list`
  List data log which is needed for multi-site deployments.

:command:`datalog autotrim`
  Trim the data log once, the way the gateway's integrated log trimming does.

:command:`datalog trim`
  Trim data log manually instead of relying on the gateway's integrated log sync.
  Before trimming, compare the listings and make sure the last sync was
  complete, otherwise it can reinitiate a sync.

:command:`datalog type`
  Change the data log's backing store to FIFO (--log-type=fifo). Changing it
  to omap is not supported.

:command:`datalog prune`
  Remove the data log's older generations that are empty. datalog type starts
  a new generation, and the old one can be pruned after it has been trimmed.

:command:`datalog semaphore list`
  List the data log's recovery semaphores, which mark bucket shards whose
  changes might not have reached the data log yet.

:command:`datalog semaphore reset`
  Reset the recovery semaphore whose key is given with --marker to --count,
  or to 0.

:command:`datalog status`
  Read data log status.

:command:`orphans find`
  Init and run search for leaked RADOS objects.
  DEPRECATED. See the "rgw-orphan-list" tool.

:command:`orphans finish`
  Clean up search for leaked RADOS objects.
  DEPRECATED. See the "rgw-orphan-list" tool.

:command:`orphans list-jobs`
  List the current orphans search job IDs.
  DEPRECATED. See the "rgw-orphan-list" tool.

:command:`role create`
  Create a new role for use with STS (Security Token Service).

:command:`role delete`
  Remove a role.

:command:`role get`
  Get a role.

:command:`role list`
  List the roles with specified path prefix.

:command:`role-trust-policy modify`
  Modify the assume role policy of an existing role.

:command:`role-policy put`
  Add/update permission policy to role.

:command:`role-policy list`
  List the policies attached to a role.

:command:`role-policy get`
  Get the specified inline policy document embedded with the given role.

:command:`role-policy delete`
  Remove the policy attached to a role

:command:`role policy attach`
  Attach the managed policy given with --policy-arn to a role that belongs to
  an account.

:command:`role policy detach`
  Detach a managed policy from a role.

:command:`role policy list attached`
  List the managed policies that are attached to a role.

:command:`role update`
  Update the maximum session duration of a role.

:command:`oidc-provider create`
  Create an OIDC provider. If ``account-id`` is not specified,
  the provider is created in the global scope.

:command:`oidc-provider modify`
  Update thumbprints and/or client-ids of an OIDC provider. The provided list
  fully replaces the existing list for that field; unspecified fields are left
  unchanged.

:command:`oidc-provider get`
  Get information about an OIDC provider.

:command:`oidc-provider delete`
  Delete an OIDC provider.

:command:`oidc-provider list`
  List OIDC providers.

:command:`reshard add`
  Schedule a resharding of a bucket

:command:`reshard list`
  List all bucket resharding or scheduled to be resharded

:command:`reshard process`
  Process of scheduled reshard jobs

:command:`reshard status`
  Resharding status of a bucket

:command:`reshard cancel`
  Cancel resharding a bucket

:command:`topic list`
  List bucket notifications topics

:command:`topic get`
  Get a bucket notification topic 

:command:`topic rm`
  Remove a bucket notifications topic 

:command:`topic stats`
  Get a bucket notifications persistent topic stats (i.e. reservations, entries & size)

:command:`topic dump`
  Dump (in JSON format) all pending bucket notifications of a persistent topic

:command:`restore list`
  List restore status of each object in a bucket

:command:`restore status`
  Show restoration status of an object in the bucket

Options
=======

.. option:: -c ceph.conf, --conf=ceph.conf

   Use ``ceph.conf`` configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine Monitor addresses during
   startup.

.. option:: -m monaddress[:port]

   Connect to specified Monitor (instead of selecting one
   from ceph.conf).

.. option:: --tenant=<tenant>

   Name of the tenant.

.. option:: --user_ns=<namespace>

   The namespace of the user. Users who authenticate through an OIDC provider
   are in the ``oidc`` namespace.

.. option:: --uid=uid

   The user on which to operate.

.. option:: --new-uid=uid

   The new ID of the user. Used with 'user rename' command.

.. option:: --subuser=<name>

    Name of the subuser.

.. option:: --account-name=<name>

   The name of the account.

.. option:: --account-id=<id>

   The ID of the account.

.. option:: --max-users

   The maximum number of users in an account. The default is 1000.

.. option:: --max-roles

   The maximum number of roles in an account. The default is 1000.

.. option:: --max-groups

   The maximum number of groups in an account. The default is 1000.

.. option:: --max-access-keys

   The maximum number of access keys for each user in an account. The default
   is 4.

.. option:: --access-key=<key>

   S3 access key.

.. option:: --email=email

   The e-mail address of the user.

.. option:: --secret/--secret-key=<key>

   The secret key.

.. option:: --gen-access-key

    Generate random access key (for S3).


.. option:: --gen-secret

    Generate random secret key.

.. option:: --generate-key

    Create user with or without credentials.
    If this option set to false, then user cannot set --gen-access-key/--gen-secret/--secret-key/--access-key.
    If this option set to true, then user cannot set  --secret-key/--access-key and bypass options for --gen-secret/--gen-access-key.
    Default is true.

.. option:: --key-type=<type>

    Key type, options are: swift, s3.

.. option:: --temp-url-key[-2]=<key>

    Temporary URL key.

.. option:: --max-buckets

    Maximum number of buckets for a user (0 for no limit, negative value to disable bucket creation).
    Default is 1000.

.. option:: --access=<access>

   Set the access permissions for the subuser.
   Available access permissions are read, write, readwrite and full.

.. option:: --display-name=<name>

   The display name of the user.

.. option:: --admin

   Set the admin flag on the user.

.. option:: --system

   Set the system flag on the user.

.. option:: --account-root

   Set the account root flag on a user that belongs to an account. The account
   root user has default permissions on all of the account's resources.

.. option:: --bucket=[tenant-id/]bucket

   Specify the bucket name.  If tenant-id is not specified, the tenant-id
   of the user (--uid) is used.

.. option:: --pool=<pool>

   Specify the pool name.
   Also used with `orphans find` as data pool to scan for leaked RADOS objects.

.. option:: --object=object

   Specify the object name.

.. option:: --marker=<marker>

   The marker at which a listing begins, up to which mdlog trim and datalog
   trim remove entries, or at which bucket chown resumes. For bucket list, it
   requires an ordered listing. For datalog semaphore reset, it is the key of
   the semaphore.

.. option:: --date=yyyy-mm-dd

   The date in the format yyyy-mm-dd.

.. option:: --start-date=yyyy-mm-dd

   The start date in the format yyyy-mm-dd.

.. option:: --end-date=yyyy-mm-dd

   The end date in the format yyyy-mm-dd.

.. option:: --bucket-id=<bucket-id>

   Specify the bucket id.

.. option:: --bucket-new-name=[tenant-id/]<bucket>

   Optional for `bucket link`; use to rename a bucket.
   While the tenant-id can be specified, this is not
   necessary in normal operation.

.. option:: --count=<count>

   The value to which datalog semaphore reset sets a semaphore. The default is
   0.

.. option:: --shard-id=<shard-id>

   Optional for mdlog list, mdlog status, bilog list, bilog trim, bilog status,
   datalog list, datalog status, datalog semaphore list, data sync status, sync
   error list, sync error trim, reshardlog list, bi list, gc list, and gc
   process. Required for ``mdlog trim`` and ``datalog trim``.

.. option:: --gen=<gen-id>

   The generation of the bucket index log, for bilog list, bilog trim, and
   bilog status. bilog list and bilog status default to the latest generation,
   and bilog trim to generation 0.

.. option:: --log-type=<type>

   The backing store to which datalog type changes the data log: ``fifo``.

.. option:: --max-entries=<entries>

   Optional for listing operations to specify the max entries.

.. option:: --purge-data

   When specified, user removal will also purge the user's data.

.. option:: --purge-keys

   When specified, subuser removal will also purge the subuser' keys.

.. option:: --purge-objects

   When specified, the bucket removal will also purge all objects in it.

.. option:: --metadata-key=<key>

   Key from which to retrieve metadata, used with ``metadata get``.

.. option:: --remote=<remote>

   Zone or zonegroup id of remote gateway.

.. option:: --period=<id>

   Period ID.

.. option:: --url=<url>

   URL for pushing/pulling period or realm.

.. option:: --epoch=<number>

   Period epoch.

.. option:: --commit

   Commit the period during 'period update'.

.. option:: --staging

   Get the staging period info.

.. option:: --master

   Set as master.

.. option:: --master-zone=<id>

   Master zone ID.

.. option:: --rgw-realm=<name>

   The realm name.

.. option:: --realm-id=<id>

   The realm ID.

.. option:: --realm-new-name=<name>

   New name for the realm.

.. option:: --rgw-zonegroup=<name>

   The zonegroup name.

.. option:: --zonegroup-id=<id>

   The zonegroup ID.

.. option:: --zonegroup-new-name=<name>

   The new name of the zonegroup.

.. option:: --rgw-zone=<zone>

   Zone in which the gateway is running.

.. option:: --zone-id=<id>

   The zone ID.

.. option:: --zone-new-name=<name>

   The new name of the zone.

.. option:: --source-zone

   The source zone for data sync.

.. option:: --default

   Set the entity (realm, zonegroup, zone) as default.

.. option:: --read-only

   Set the zone as read-only when adding to the zonegroup.

.. option:: --placement-id

   Placement ID for the zonegroup placement commands.

.. option:: --tags=<list>

   The list of tags for zonegroup placement add and modify commands.

.. option:: --tags-add=<list>

   The list of tags to add for zonegroup placement modify command.

.. option:: --tags-rm=<list>

   The list of tags to remove for zonegroup placement modify command.

.. option:: --endpoints=<list>

   The zone endpoints.

.. option:: --index-pool=<pool>

   The placement target index pool.

.. option:: --data-pool=<pool>

   The placement target data pool.

.. option:: --data-extra-pool=<pool>

   The placement target data extra (non-EC) pool.

.. option:: --placement-index-type=<type>

   The placement target index type (normal, indexless, or #id).

.. option:: --placement-inline-data=<true>

   Whether the placement target is configured to store a data chunk inline in head objects.

.. option:: --tier-type=<type>

   The zone tier type.

.. option:: --tier-config=<k>=<v>[,...]

   Set zone tier config keys, values.

.. option:: --tier-config-rm=<k>[,...]

   Unset zone tier config keys.

.. option:: --sync-from-all[=false]

   Set/reset whether zone syncs from all zonegroup peers.

.. option:: --sync-from=[zone-name][,...]

   Set the list of zones from which to sync.

.. option:: --sync-from-rm=[zone-name][,...]

   Remove zone(s) from list of zones from which to sync.

.. option:: --bucket-index-max-shards

   Override a zone's or zonegroup's default number of bucket index shards. This
   option is accepted by the 'zone create', 'zone modify', 'zonegroup add',
   and 'zonegroup modify' commands, and applies to buckets that are created
   after the zone/zonegroup changes take effect.

.. option:: --fix

    Fix the bucket index in addition to checking it.

.. option:: --check-objects

    Bucket check: Rebuilds the bucket index according to actual object state.

.. option:: --format=<format>

    Specify output format for certain operations. Supported formats: xml, json.

.. option:: --pretty-format

    Enable pretty formatting for JSON/XML output with indentation and newlines.
    If no format is specified, default is true. Use ``--format <json/xml>`` to disable
    for more compact output.

.. option:: --sync-stats

    Option for the 'user stats' command. When specified, it will update user stats with
    the current stats reported by the user's buckets indexes.

.. option:: --show-config

    Show configuration.

.. option:: --show-log-entries=<flag>

    Enable/disable dumping of log entries on log show.

.. option:: --show-log-sum=<flag>

    Enable/disable dump of log summation on log show.

.. option:: --skip-zero-entries

    Log show only dumps entries that don't have zero value in one of the numeric
    field.

.. option:: --infile

    Specify a file to read when setting data.

.. option:: --categories=<list>

    Comma-separated list of categories, used in usage show.

.. option:: --caps=<caps>

    List of capabilities (e.g., "usage=read, write; user=read").

.. option:: --compression=<compression-algorithm>

    Placement target compression algorithm (lz4|snappy|zlib|zstd).

.. option:: --yes-i-really-mean-it

    Required as a guardrail for certain destructive operations.

.. option:: --min-rewrite-size

    Specify the minimum object size for bucket rewrite (default 4M).

.. option:: --max-rewrite-size

    Specify the maximum object size for bucket rewrite (default ULLONG_MAX).

.. option:: --min-rewrite-stripe-size

    Specify the minimum stripe size for object rewrite (default 0). If the value
    is set to 0, then the specified object will always be
    rewritten when restriping.

.. option:: --trim-delay-ms

   For sync error trim, the time in milliseconds to sleep after every 1000
   entries that are trimmed, which limits the rate of trimming. The default is
   0.

.. option:: --warnings-only

   When specified with bucket limit check,
   list only buckets nearing or over the current max objects per shard value.

.. option:: --bypass-gc

   When specified with bucket deletion,
   triggers object deletion without involving GC.

.. option:: --inconsistent-index

   When specified with bucket deletion and bypass-gc set to true,
   ignores bucket index consistency.

.. option:: --max-concurrent-ios

   Maximum concurrent bucket operations. Affects operations that
   scan the bucket index, e.g., listing, deletion, and all scan/search
   operations such as finding orphans or checking the bucket index.
   The default is 32.

.. option:: --restore-status

   Filter objects returned by the 'restore list' command by status.

.. option:: --show-restore-stats

   Shows restore stats in a bucket stat command. The bucket name needs to be provided.

Quota Options
=============

.. option:: --max-objects

   Specify the maximum number of objects (negative value to disable).

.. option:: --max-size

    Specify the maximum object size (in B/K/M/G/T, negative value to disable).

.. option:: --quota-scope

    The scope of quota (bucket, user).


Orphans Search Options
======================

.. option:: --num-shards

    Number of shards to use for temporary scan info

.. option:: --orphan-stale-secs

   Number of seconds to wait before declaring an object to be an orphan.
   The default is 86400 (24 hours).

.. option:: --job-id

   Set the job id (for orphans find)


Orphans list-jobs options
=========================

.. option:: --extra-info

   Provide extra info in the job list.


Role Options
============

.. option:: --role-name

   The name of the role to create.

.. option:: --path

   The path to the role.

.. option:: --assume-role-policy-doc

   The trust relationship policy document that grants an entity permission to
   assume the role.

.. option:: --policy-name

   The name of the policy document.

.. option:: --policy-doc

   The permission policy document.

.. option:: --path-prefix

   The path prefix for filtering the roles.

.. option:: --description

   The description of the role, for role create.

.. option:: --max-session-duration

   The maximum session duration of the role, in seconds, from 3600 to 43200,
   for role create and role update.

.. option:: --policy-arn

   The ARN of a managed policy, for user policy attach and detach and for role
   policy attach and detach. See
   https://docs.ceph.com/en/latest/radosgw/iam/#managed-policies.


OIDC Provider Options
=====================

.. option:: --provider-url

   URL of the OIDC provider.

.. option:: --client-ids

   Comma-separated list of client IDs.

.. option:: --thumbprints

   Comma-separated list of thumbprints.


Bucket Notifications/PubSub Options
===================================
.. option:: --topic

   The bucket notifications/pubsub topic name.


Sync Options
============

These options are for the sync policy commands, sync info, and the bucket sync
commands. See https://docs.ceph.com/en/latest/radosgw/multisite-sync-policy/.

.. option:: --group-id=<id>

   The ID of a sync policy group.

.. option:: --status=<status>

   The status of a sync policy group: ``enabled``, ``allowed``, or
   ``forbidden``.

.. option:: --flow-id=<id>

   The ID of a data flow in a sync policy group.

.. option:: --flow-type=<type>

   The type of a data flow: ``symmetrical`` or ``directional``.

.. option:: --zones=<list>, --zone-ids=<list>

   The zones of a symmetrical data flow, by name or by ID.

.. option:: --source-zone-id=<id>

   The source zone of a directional data flow, by ID. --source-zone gives it by
   name.

.. option:: --dest-zone=<zone>, --dest-zone-id=<id>

   The destination zone of a directional data flow, by name or by ID.

.. option:: --pipe-id=<id>

   The ID of a sync pipe.

.. option:: --source-zones=<list>, --source-zone-ids=<list>

   The source zones of a sync pipe, by name or by ID, or ``*`` for any zone.

.. option:: --dest-zones=<list>, --dest-zone-ids=<list>

   The destination zones of a sync pipe, by name or by ID, or ``*`` for any
   zone.

.. option:: --source-bucket=<bucket>, --source-bucket-id=<id>

   The source bucket of a sync pipe, by name or by ID, or ``*`` for the bucket
   whose policy holds the pipe. The bucket sync commands take them to select
   one source bucket.

.. option:: --source-tenant=<tenant>

   The tenant of the source bucket.

.. option:: --dest-bucket=<bucket>, --dest-bucket-id=<id>

   The destination bucket of a sync pipe, by name or by ID, or ``*`` for the
   bucket whose policy holds the pipe.

.. option:: --dest-tenant=<tenant>

   The tenant of the destination bucket.

.. option:: --prefix=<prefix>

   Sync only the source objects whose names begin with this prefix.

.. option:: --prefix-rm=<value>

   Remove the prefix filter from a sync pipe. The option needs a value, which
   is ignored, such as ``--prefix-rm=1``.

.. option:: --dest-owner=<user>

   The user who owns the objects that a sync pipe writes.

.. option:: --mode=<mode>

   The mode of a sync pipe: ``system`` or ``user``. In ``user`` mode, the pipe
   syncs with the permissions of the user given with --uid.

.. option:: --priority=<number>

   The priority of a sync pipe.

.. option:: --effective-zone-name=<zone>, --effective-zone-id=<id>

   The zone for which sync info shows the sync sources and destinations. The
   default is this zone.

.. option:: --retry-delay-ms=<ms>

   For bucket sync checkpoint, the time in milliseconds between checks. The
   default is 2000.

.. option:: --timeout-sec=<seconds>

   For bucket sync checkpoint, the time in seconds to wait before giving up.
   The default is 60.


Examples
========

Generate a new user::

        $ radosgw-admin user create --display-name="johnny rotten" --uid=johnny
        { "user_id": "johnny",
          "rados_uid": 0,
          "display_name": "johnny rotten",
          "email": "",
          "suspended": 0,
          "subusers": [],
          "keys": [
                { "user": "johnny",
                  "access_key": "TCICW53D9BQ2VGC46I44",
                  "secret_key": "tfm9aHMI8X76L3UdgE+ZQaJag1vJQmE6HDb5Lbrz"}],
          "swift_keys": []}

Remove a user::

        $ radosgw-admin user rm --uid=johnny

Rename a user::

        $ radosgw-admin user rename --uid=johnny --new-uid=joe
        
Remove a user and all associated buckets with their contents::

        $ radosgw-admin user rm --uid=johnny --purge-data

Remove a bucket::

	$ radosgw-admin bucket rm --bucket=foo

Link bucket to specified user::
	
	$ radosgw-admin bucket link --bucket=foo --bucket_id=<bucket id> --uid=johnny

Unlink bucket from specified user::

        $ radosgw-admin bucket unlink --bucket=foo --uid=johnny

Rename a bucket::

        $ radosgw-admin bucket link --bucket=foo --bucket-new-name=bar --uid=johnny

Move a bucket from the old global tenant space to a specified tenant::

        $ radosgw-admin bucket link --bucket=foo --uid='12345678$12345678'

Link bucket to specified user and change object ACLs::

        $ radosgw-admin bucket chown --bucket=foo --uid='12345678$12345678'

Show the logs of a bucket from April 1st, 2012::

        $ radosgw-admin log show --bucket=foo --date=2012-04-01-01 --bucket-id=default.14193.1

Show usage information for user from March 1st to (but not including) April 1st, 2012::

        $ radosgw-admin usage show --uid=johnny \
                        --start-date=2012-03-01 --end-date=2012-04-01

Show only summary of usage information for all users::

        $ radosgw-admin usage show --show-log-entries=false

Trim usage information for user until March 1st, 2012::

        $ radosgw-admin usage trim --uid=johnny --end-date=2012-04-01


Availability
============

:program:`radosgw-admin` is part of Ceph, a massively scalable, open-source,
distributed storage system.  Please refer to the Ceph documentation at
https://docs.ceph.com for more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`radosgw <radosgw>`\(8)
