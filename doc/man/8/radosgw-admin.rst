:orphan:

=================================================================
 radosgw-admin -- rados REST gateway user administration utility
=================================================================

.. program:: radosgw-admin

Synopsis
========

| **radosgw-admin** *command* [ *options* *...* ]


Description
===========

:program:`radosgw-admin` is a RADOS gateway user administration utility. It
allows creating and modifying users.


Commands
========

:program:`radosgw-admin` utility uses many commands for administration purpose
which are as follows:

:command:`user create`
  Create a new user.

:command:`user modify`
  Modify a user.

:command:`user info`
  Display information of a user, and any potentially available
  subusers and keys.

:command:`user rm`
  Remove a user.

:command:`user suspend`
  Suspend a user.

:command:`user enable`
  Re-enable user after suspension.

:command:`user check`
  Check user info.

:command:`user stats`
  Show user stats as accounted by quota subsystem.

:command:`user list`
  List all users.

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

:command:`bucket list`
  List all buckets.

:command:`bucket limit check`
  Show bucket sharding stats.

:command:`bucket link`
  Link bucket to specified user.

:command:`bucket unlink`
  Unlink bucket from specified user.

:command:`bucket stats`
  Returns bucket statistics.

:command:`bucket rm`
  Remove a bucket.

:command:`bucket check`
  Check bucket index.

:command:`bucket rewrite`
  Rewrite all objects in the specified bucket.

:command:`bucket reshard`
  Reshard a bucket.

:command:`bucket sync disable`
  Disable bucket sync.

:command:`bucket sync enable`
  Enable bucket sync.

:command:`bi get`
  Retrieve bucket index object entries.

:command:`bi put`
  Store bucket index object entries.

:command:`bi list`
  List raw bucket index entries.

:command:`bi purge`
  Purge bucket index entries.

:command:`object rm`
  Remove an object.

:command:`object stat`
  Stat an object for its metadata.

:command:`object unlink`
  Unlink object from bucket index.

:command:`object rewrite`
  Rewrite the specified object.

:command:`objects expire`
  Run expired objects cleanup.

:command:`period rm`
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

:command:`zonegroup rm`
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

:command:`zone rm`
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

:command:`sync error list`
  list sync error.

:command:`sync error trim`
  trim sync error.

:command:`zone rename`
  Rename a zone.

:command:`zone placement list`
  List zone's placement targets.

:command:`zone placement add`
  Add a zone placement target.

:command:`zone placement modify`
  Modify a zone placement target.

:command:`zone placement rm`
  Remove a zone placement target.

:command:`pool add`
  Add an existing pool for data placement.

:command:`pool rm`
  Remove an existing pool from data placement set.

:command:`pools list`
  List placement active set.

:command:`policy`
  Display bucket/object policy.

:command:`log list`
  List log objects.

:command:`log show`
  Dump a log from specific object or (bucket + date + bucket-id).
  (NOTE: required to specify formatting of date to "YYYY-MM-DD-hh")

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

:command:`lc list`
  List all bucket lifecycle progress.

:command:`lc process`
  Manually process lifecycle.

:command:`metadata get`
  Get metadata info.

:command:`metadata put`
  Put metadata info.

:command:`metadata rm`
  Remove metadata info.

:command:`metadata list`
  List metadata info.

:command:`mdlog list`
  List metadata log.

:command:`mdlog trim`
  Trim metadata log.

:command:`mdlog status`
  Read metadata log status.

:command:`bilog list`
  List bucket index log.

:command:`bilog trim`
  Trim bucket index log (use start-marker, end-marker).

:command:`datalog list`
  List data log.

:command:`datalog trim`
  Trim data log.

:command:`datalog status`
  Read data log status.

:command:`orphans find`
  Init and run search for leaked rados objects

:command:`orphans finish`
  Clean up search for leaked rados objects

:command:`orphans list-jobs`
  List the current job-ids for the orphans search.

:command:`role create`
  create a new AWS role for use with STS.

:command:`role rm`
  Remove a role.

:command:`role get`
  Get a role.

:command:`role list`
  List the roles with specified path prefix.

:command:`role modify`
  Modify the assume role policy of an existing role.

:command:`role-policy put`
  Add/update permission policy to role.

:command:`role-policy list`
  List the policies attached to a role.

:command:`role-policy get`
  Get the specified inline policy document embedded with the given role.

:command:`role-policy rm`
  Remove the policy attached to a role

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

Options
=======

.. option:: -c ceph.conf, --conf=ceph.conf

   Use ``ceph.conf`` configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during
   startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ceph.conf).

.. option:: --tenant=<tenant>

   Name of the tenant.

.. option:: --uid=uid

   The radosgw user ID.

.. option:: --subuser=<name>

	Name of the subuser.

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

.. option:: --key-type=<type>

	key type, options are: swift, s3.

.. option:: --temp-url-key[-2]=<key>

	Temporary url key.

.. option:: --max-buckets

	max number of buckets for a user (0 for no limit, negative value to disable bucket creation).
	Default is 1000.

.. option:: --access=<access>

   Set the access permissions for the sub-user.
   Available access permissions are read, write, readwrite and full.

.. option:: --display-name=<name>

   The display name of the user.

.. option:: --admin

   Set the admin flag on the user.

.. option:: --system

   Set the system flag on the user.

.. option:: --bucket=bucket

   Specify the bucket name.

.. option:: --pool=<pool>

   Specify the pool name.
   Also used with `orphans find` as data pool to scan for leaked rados objects.

.. option:: --object=object

   Specify the object name.

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
        When using implicit tenants, prefix the name
        with the desired tenant-id.

.. option:: --shard-id=<shard-id>

	Optional for mdlog list, data sync status. Required for ``mdlog trim``.

.. option:: --max-entries=<entries>

	Optional for listing operations to specify the max entires

.. option:: --purge-data

   When specified, user removal will also purge all the user data.

.. option:: --purge-keys

	When specified, subuser removal will also purge all the subuser keys.
   
.. option:: --purge-objects

   When specified, the bucket removal will also purge all objects in it.

.. option:: --metadata-key=<key>

	Key to retrieve metadata from with ``metadata get``.

.. option:: --remote=<remote>

   Zone or zonegroup id of remote gateway.

.. option:: --period=<id>

   Period id.

.. option:: --url=<url>

   url for pushing/pulling period or realm.

.. option:: --epoch=<number>

   Period epoch.

.. option:: --commit

   Commit the period during 'period update'.

.. option:: --staging

   Get the staging period info.

.. option:: --master

   Set as master.

.. option:: --master-zone=<id>

   Master zone id.

.. option:: --rgw-realm=<name>

   The realm name.

.. option:: --realm-id=<id>

   The realm id.

.. option:: --realm-new-name=<name>

   New name of realm.

.. option:: --rgw-zonegroup=<name>

   The zonegroup name.

.. option:: --zonegroup-id=<id>

   The zonegroup id.

.. option:: --zonegroup-new-name=<name>

   The new name of the zonegroup.

.. option:: --rgw-zone=<zone>

	Zone in which radosgw is running.

.. option:: --zone-id=<id>

   The zone id.

.. option:: --zone-new-name=<name>

   The new name of the zone.

.. option:: --source-zone

   The source zone for data sync.

.. option:: --default

   Set the entity (realm, zonegroup, zone) as default.

.. option:: --read-only

   Set the zone as read-only when adding to the zonegroup.

.. option:: --placement-id

   Placement id for the zonegroup placement commands.

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

   The placement target data extra (non-ec) pool.

.. option:: --placement-index-type=<type>

   The placement target index type (normal, indexless, or #id).

.. option:: --tier-type=<type>

   The zone tier type.

.. option:: --tier-config=<k>=<v>[,...]

   Set zone tier config keys, values.

.. option:: --tier-config-rm=<k>[,...]

   Unset zone tier config keys.

.. option:: --sync-from-all[=false]

   Set/reset whether zone syncs from all zonegroup peers.

.. option:: --sync-from=[zone-name][,...]

   Set the list of zones to sync from.

.. option:: --sync-from-rm=[zone-name][,...]

   Remove the zones from list of zones to sync from.

.. option:: --fix

	Besides checking bucket index, will also fix it.

.. option:: --check-objects

	bucket check: Rebuilds bucket index according to actual objects state.

.. option:: --format=<format>

	Specify output format for certain operations. Supported formats: xml, json.

.. option:: --sync-stats

	Option for 'user stats' command. When specified, it will update user stats with
	the current stats reported by user's buckets indexes.

.. option:: --show-log-entries=<flag>

	Enable/disable dump of log entries on log show.

.. option:: --show-log-sum=<flag>

	Enable/disable dump of log summation on log show.

.. option:: --skip-zero-entries

	Log show only dumps entries that don't have zero value in one of the numeric
	field.

.. option:: --infile

	Specify a file to read in when setting data.

.. option:: --categories=<list>

	Comma separated list of categories, used in usage show.

.. option:: --caps=<caps>

	List of caps (e.g., "usage=read, write; user=read".

.. option:: --compression=<compression-algorithm>

    Placement target compression algorithm (lz4|snappy|zlib|zstd)

.. option:: --yes-i-really-mean-it

	Required for certain operations.

.. option:: --min-rewrite-size

    Specify the min object size for bucket rewrite (default 4M).

.. option:: --max-rewrite-size

    Specify the max object size for bucket rewrite (default ULLONG_MAX).

.. option:: --min-rewrite-stripe-size

    Specify the min stripe size for object rewrite (default 0). If the value
    is set to 0, then the specified object will always be
    rewritten for restriping.

.. option:: --warnings-only

   When specified with bucket limit check,
   list only buckets nearing or over the current max objects per shard value.

.. option:: --bypass-gc

   When specified with bucket deletion,
   triggers object deletions by not involving GC.

.. option:: --inconsistent-index

   When specified with bucket deletion and bypass-gc set to true,
   ignores bucket index consistency.

Quota Options
=============

.. option:: --max-objects

	Specify max objects (negative value to disable).

.. option:: --max-size

	Specify max size (in B/K/M/G/T, negative value to disable).

.. option:: --quota-scope

	The scope of quota (bucket, user).


Orphans Search Options
======================

.. option:: --num-shards

	Number of shards to use for keeping the temporary scan info

.. option:: --orphan-stale-secs

        Number of seconds to wait before declaring an object to be an orphan.
        Default is 86400 (24 hours).

.. option:: --job-id

        Set the job id (for orphans find)

.. option:: --max-concurrent-ios

        Maximum concurrent ios for orphans find.
        Default is 32.


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
        
Remove a user and all associated buckets with their contents::

        $ radosgw-admin user rm --uid=johnny --purge-data

Remove a bucket::

	$ radosgw-admin bucket rm --bucket=foo

Link bucket to specified user::
	
	$ radosgw-admin bucket link --bucket=foo --bucket_id=<bucket id> --uid=johnny

Unlink bucket from specified user::

        $ radosgw-admin bucket unlink --bucket=foo --uid=johnny

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
http://ceph.com/docs for more information.


See also
========

:doc:`ceph <ceph>`\(8)
:doc:`radosgw <radosgw>`\(8)
