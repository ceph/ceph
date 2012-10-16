=================================================================
 radosgw-admin -- rados REST gateway user administration utility
=================================================================

.. program:: radosgw-admin

Synopsis
========

| **radosgw-admin** *command* [ *options* *...* ]


Description
===========

**radosgw-admin** is a RADOS gateway user administration utility. It
allows creating and modifying users.


Commands
========

*command* can be one of the following options:

:command:`user create`
  Create a new user

:command:`user modify`
  Modify a user

:command:`user info`
  Display information of a user, and any potentially available
  subusers and keys

:command:`user rm`
  Remove a user

:command:`subuser create`
  Create a new subuser (primarily useful for clients using the Swift API)

:command:`subuser modify`
  Modify a subuser

:command:`subuser rm`
  Remove a subuser

:command:`bucket list`
  List all buckets

:command:`bucket unlink`
  Remove a bucket

:command:`bucket rm`
  Remove a bucket

:command:`object rm`
  Remove an object

:command:`key create`
  Create an access key

:command:`key rm`
  Remove an access key

:command:`pool add`
  Add an existing pool for data placement

:command:`pool rm`
  Remove an existing pool from data placement set

:command:`pools list`
  List placement active set

:command:`policy`
  Display bucket/object policy

:command:`log show`
  Show the log of a bucket (with a specified date)

:command:`usage show`
  Show the usage information (with optional user and date range)

:command:`usage trim`
  Trim usage information (with optional user and date range)


Options
=======

.. option:: -c ceph.conf, --conf=ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during
   startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ceph.conf).

.. option:: --uid=uid

   The radosgw user ID.

.. option:: --secret=secret

   The secret associated with a given key.

.. option:: --display-name=name

   Configure the display name of the user.

.. option:: --email=email

   The e-mail address of the user

.. option:: --bucket=bucket

   Specify the bucket name.

.. option:: --object=object

   Specify the object name.

.. option:: --date=yyyy-mm-dd

   The date needed for some commands

.. option:: --start-date=yyyy-mm-dd

   The start date needed for some commands

.. option:: --end-date=yyyy-mm-dd

   The end date needed for some commands

.. option:: --auth-uid=auid

   The librados auid

.. option:: --purge-data

   Remove user data before user removal
   
.. option:: --purge-objects

   Remove all objects before bucket removal

.. option:: --lazy-remove

   Defer removal of object tail
   

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

        $ radosgw-admin log show --bucket=foo --date=2012=04-01

Show usage information for user from March 1st to (but not including) April 1st, 2012::

        $ radosgw-admin usage show --uid=johnny \
                        --start-date=2012-03-01 --end-date=2012-04-01

Show only summary of usage information for all users::

        $ radosgw-admin usage show --show-log-entries=false

Trim usage information for user until March 1st, 2012::

        $ radosgw-admin usage trim --uid=johnny --end-date=2012-04-01

Availability
============

**radosgw-admin** is part of the Ceph distributed file system.  Please
refer to the Ceph documentation at http://ceph.com/docs for more
information.

See also
========

:doc:`ceph <ceph>`\(8)
