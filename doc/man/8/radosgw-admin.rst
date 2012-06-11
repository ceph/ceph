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
  Display information of a user

:command:`user rm`
  Remove a user

:command:`bucket list`
  List all buckets

:command:`bucket unlink`
  Remove a bucket

:command:`policy`
  Display bucket/object policy

:command:`log show`
  Show the log of a bucket (with a specified date)


Options
=======

.. option:: -c ceph.conf, --conf=ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during
   startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ceph.conf).

.. option:: --uid=uid

   The S3 user/access key.

.. option:: --secret=secret

   The S3 secret.

.. option:: --display-name=name

   Configure the display name of the user.

.. option:: --email=email

   The e-mail address of the user

.. option:: --bucket=bucket

   Specify the bucket name.

.. option:: --object=object

   Specify the object name.

.. option:: --date=yyyy-mm-dd

   The date need for some commands

.. option:: --os-user=group:name

   The OpenStack user (only needed for use with OpenStack)

.. option:: --os-secret=key

   The OpenStack key

.. option:: --auth-uid=auid

   The librados auid


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

Remove a bucket::

        $ radosgw-admin bucket unlink --bucket=foo

Show the logs of a bucket from April 1st 2011::

        $ radosgw-admin log show --bucket=foo --date=2011=04-01

Availability
============

**radosgw-admin** is part of the Ceph distributed file system.  Please
refer to the Ceph wiki at http://ceph.newdream.net/wiki for more
information.

See also
========

:doc:`ceph <ceph>`\(8)
