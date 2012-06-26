========================================
 obsync -- The object synchronizer tool
========================================

.. program:: obsync

Synopsis
========

| **obsync** [ *options* ] *source-url* *destination-url*


Description
===========

**obsync** is an object syncrhonizer tool designed to transfer objects
between different object storage systems. Similar to rsync, you
specify a source and a destination, and it will transfer objects
between them until the destination has all the objects in the
source. Obsync will never modify the source -- only the destination.

By default, obsync does not delete anything. However, by specifying
``--delete-after`` or ``--delete-before``, you can ask it to delete
objects from the destination that are not in the source.


Target types
============

Obsync supports S3 via ``libboto``. To use the s3 target, your URL
should look like this: ``s3://host-name/bucket-name``

Obsync supports storing files locally via the ``file://`` target. To
use the file target, your URL should look like this:
``file://directory-name``

Alternately, give no prefix, like this: ``./directory-name``

Obsync supports storing files in a RADOS Gateway backend via the
``librados`` Python bindings. To use the ``rgw` target, your URL
should look like this: ``rgw:ceph-configuration-path:rgw-bucket-name``


Options
=======

.. option:: -h, --help

   Display a help message

.. option:: -n, --dry-run

   Show what would be done, but do not modify the destination.

.. option:: -c, --create-dest

   Create the destination if it does not exist.

.. option:: --delete-before

   Before copying any files, delete objects in the destination that
   are not in the source.

.. option:: -L, --follow-symlinks

   Follow symlinks when dealing with ``file://`` targets.

.. option:: --no-preserve-acls

   Don't preserve ACLs when copying objects.

.. option:: -v, --verbose

   Be verbose.

.. option:: -V, --more-verbose

   Be really, really verbose (developer mode)

.. option:: -x SRC=DST, --xuser SRC=DST

   Set up a user translation. You can specify multiple user
   translations with multiple ``--xuser`` arguments.

.. option:: --force

   Overwrite all destination objects, even if they appear to be the
   same as the source objects.


Environment variables
=====================

.. envvar:: SRC_AKEY

   Access key for the source URL

.. envvar:: SRC_SKEY

   Secret access key for the source URL

.. envvar:: DST_AKEY

   Access key for the destination URL

.. envvar:: DST_SKEY

   Secret access key for the destination URL

.. envvar:: AKEY

   Access key for both source and dest

.. envvar:: SKEY

   Secret access key for both source and dest

.. envvar:: DST_CONSISTENCY

   Set to 'eventual' if the destination is eventually consistent. If the destination
   is eventually consistent, we may have to retry certain operations multiple times.


Examples
========

::

        AKEY=... SKEY=... obsync -c -d -v ./backup-directory s3://myhost1/mybucket1

Copy objects from backup-directory to mybucket1 on myhost1::

        SRC_AKEY=... SRC_SKEY=... DST_AKEY=... DST_SKEY=... obsync -c -d -v s3://myhost1/mybucket1 s3://myhost1/mybucket2

Copy objects from mybucket1 to mybucket2


Availability
============

**obsync** is part of the Ceph distributed file system. Please refer
to the Ceph documentation at http://ceph.com/docs for more
information.
