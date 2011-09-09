=================================================================
 radosgw_admin -- rados REST gateway user administration utility
=================================================================

.. program:: radosgw_admin

Synopsis
========

| **radosgw_admin** *command* [ *options* *...* ]


Description
===========

**radosgw_admin** is a RADOS gateway user administration utility. It
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

        $ radosgw_admin user gen --display-name="johnny rotten" --email=johnny@rotten.com
        User ID: CHBQFRTG26I8DGJDGQLW
        Secret Key: QR6cI/31N+J0VKVgHSpEGVSfEEsmf6PyXG040KCB
        Display Name: johnny rotten

Remove a user::

        $ radosgw_admin user rm --uid=CHBQFRTG26I8DGJDGQLW

Remove a bucket::

        $ radosgw_admin bucket unlink --bucket=foo

Show the logs of a bucket from April 1st 2011::

        $ radosgw_admin log show --bucket=foo --date=2011=04-01

Availability
============

**radosgw_admin** is part of the Ceph distributed file system.  Please
refer to the Ceph wiki at http://ceph.newdream.net/wiki for more
information.

See also
========

:doc:`ceph <ceph>`\(8)
