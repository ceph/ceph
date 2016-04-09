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

:command:`object rm`
  Remove an object.

:command:`object unlink`
  Unlink object from bucket index.

:command:`quota set`
  Set quota params.

:command:`quota enable`
  Enable quota.

:command:`quota disable`
  Disable quota.

:command:`region get`
  Show region info.

:command:`region list`
  List all regions set on this cluster.

:command:`region set`
  Set region info (requires infile).

:command:`region default`
  Set default region.

:command:`region-map get`
  Show region-map.

:command:`region-map set`
  Set region-map (requires infile).

:command:`zone get`
  Show zone cluster params.

:command:`zone set`
  Set zone cluster params (requires infile).

:command:`zone list`
  List all zones set on this cluster.

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

:command:`temp remove`
  Remove temporary objects that were created up to specified date
  (and optional time).

:command:`gc list`
  Dump expired garbage collection objects (specify --include-all to list all
  entries, including unexpired).

:command:`gc process`
  Manually process garbage.

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

:command:`bilog list`
  List bucket index log.

:command:`bilog trim`
  Trim bucket index log (use start-marker, end-marker).

:command:`datalog list`
  List data log.

:command:`datalog trim`
  Trim data log.

:command:`opstate list`
  List stateful operations entries (use client_id, op_id, object).

:command:`opstate set`
  Set state on an entry (use client_id, op_id, object, state).

:command:`opstate renew`
  Renew state on an entry (use client_id, op_id, object).

:command:`opstate rm`
  Remove entry (use client_id, op_id, object).

:command:`replicalog get`
  Get replica metadata log entry.

:command:`replicalog delete`
  Delete replica metadata log entry.

:command:`orphans find`
  Init and run search for leaked rados objects

:command:`orphans finish`
  Clean up search for leaked rados objects


Options
=======

.. option:: -c ceph.conf, --conf=ceph.conf

   Use ``ceph.conf`` configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during
   startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ceph.conf).

.. option:: --uid=uid

   The radosgw user ID.

.. option:: --subuser=<name>

	Name of the subuser.

.. option:: --email=email

   The e-mail address of the user.

.. option:: --display-name=name

   Configure the display name of the user.

.. option:: --access-key=<key>

	S3 access key.

.. option:: --gen-access-key

	Generate random access key (for S3).

.. option:: --secret=secret

   The secret associated with a given key.

.. option:: --gen-secret

	Generate random secret key.

.. option:: --key-type=<type>

	key type, options are: swift, S3.

.. option:: --temp-url-key[-2]=<key>

	Temporary url key.

.. option:: --system

	Set the system flag on the user.

.. option:: --bucket=bucket

   Specify the bucket name.

.. option:: --object=object

   Specify the object name.

.. option:: --date=yyyy-mm-dd

   The date needed for some commands.

.. option:: --start-date=yyyy-mm-dd

   The start date needed for some commands.

.. option:: --end-date=yyyy-mm-dd

   The end date needed for some commands.

.. option:: --shard-id=<shard-id>

	Optional for mdlog list. Required for ``mdlog trim``,
	``replica mdlog get/delete``, ``replica datalog get/delete``.

.. option:: --auth-uid=auid

   The librados auid.

.. option:: --purge-data

   Remove user data before user removal.

.. option:: --purge-keys

	When specified, subuser removal will also purge all the subuser keys.
   
.. option:: --purge-objects

   Remove all objects before bucket removal.

.. option:: --metadata-key=<key>

	Key to retrieve metadata from with ``metadata get``.

.. option:: --rgw-region=<region>

	Region in which radosgw is running.

.. option:: --rgw-zone=<zone>

	Zone in which radosgw is running.

.. option:: --fix

	Besides checking bucket index, will also fix it.

.. option:: --check-objects

	bucket check: Rebuilds bucket index according to actual objects state.

.. option:: --format=<format>

	Specify output format for certain operations: xml, json.

.. option:: --sync-stats

	Option to 'user stats', update user stats with current stats reported by
	user's buckets indexes.

.. option:: --show-log-entries=<flag>

	Enable/disable dump of log entries on log show.

.. option:: --show-log-sum=<flag>

	Enable/disable dump of log summation on log show.

.. option:: --skip-zero-entries

	Log show only dumps entries that don't have zero value in one of the numeric
	field.

.. option:: --infile

	Specify a file to read in when setting data.

.. option:: --state=<state string>

	Specify a state for the opstate set command.

.. option:: --replica-log-type

	Replica log type (metadata, data, bucket), required for replica log
	operations.

.. option:: --categories=<list>

	Comma separated list of categories, used in usage show.

.. option:: --caps=<caps>

	List of caps (e.g., "usage=read, write; user=read".

.. option:: --yes-i-really-mean-it

	Required for certain operations.


Quota Options
=============

.. option:: --max-objects

	Specify max objects (negative value to disable).

.. option:: --max-size

	Specify max size (in bytes, negative value to disable).

.. option:: --quota-scope

	Scope of quota (bucket, user).


Orphans Search Options
======================

.. option:: --pool

	Data pool to scan for leaked rados objects

.. option:: --num-shards

	Number of shards to use for keeping the temporary scan info


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

        $ radosgw-admin bucket unlink --bucket=foo

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
