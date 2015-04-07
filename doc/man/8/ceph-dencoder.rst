:orphan:

==============================================
 ceph-dencoder -- ceph encoder/decoder utility
==============================================

.. program:: ceph-dencoder

Synopsis
========

| **ceph-dencoder** [commands...]


Description
===========

**ceph-dencoder** is a utility to encode, decode, and dump ceph data
structures.  It is used for debugging and for testing inter-version
compatibility.

**ceph-dencoder** takes a simple list of commands and performs them
in order.

Commands
========

.. option:: version

   Print the version string for the **ceph-dencoder** binary.

.. option:: import <file>

   Read a binary blob of encoded data from the given file.  It will be
   placed in an in-memory buffer.

.. option:: export <file>

   Write the contents of the current in-memory buffer to the given
   file.

.. option:: list_types

   List the data types known to this build of **ceph-dencoder**.

.. option:: type <name>

   Select the given type for future ``encode`` or ``decode`` operations.

.. option:: skip <bytes>

   Seek <bytes> into the imported file before reading data structure, use
   this with objects that have a preamble/header before the object of interest.

.. option:: decode

   Decode the contents of the in-memory buffer into an instance of the
   previously selected type.  If there is an error, report it.

.. option:: encode

   Encode the contents of the in-memory instance of the previously
   selected type to the in-memory buffer.

.. option:: dump_json

   Print a JSON-formatted description of the in-memory object.

.. option:: count_tests

   Print the number of built-in test instances of the previosly
   selected type that **ceph-dencoder** is able to generate.

.. option:: select_test <n>

   Select the given build-in test instance as a the in-memory instance
   of the type.

.. option:: get_features

   Print the decimal value of the feature set supported by this version
   of **ceph-dencoder**.  Each bit represents a feature.  These correspond to
   CEPH_FEATURE_* defines in src/include/ceph_features.h.

.. option:: set_features <f>

   Set the feature bits provided to ``encode`` to *f*.  This allows
   you to encode objects such that they can be understood by old
   versions of the software (for those types that support it).

Example
=======

Say you want to examine an attribute on an object stored by ``ceph-osd``.  You can do this:

::

    $ cd /mnt/osd.12/current/2.b_head
    $ attr -l foo_bar_head_EFE6384B
    Attribute "ceph.snapset" has a 31 byte value for foo_bar_head_EFE6384B
    Attribute "ceph._" has a 195 byte value for foo_bar_head_EFE6384B
    $ attr foo_bar_head_EFE6384B -g ceph._ -q > /tmp/a
    $ ceph-dencoder type object_info_t import /tmp/a decode dump_json
    { "oid": { "oid": "foo",
          "key": "bar",
          "snapid": -2,
          "hash": 4024842315,
          "max": 0},
      "locator": { "pool": 2,
          "preferred": -1,
          "key": "bar"},
      "category": "",
      "version": "9'1",
      "prior_version": "0'0",
      "last_reqid": "client.4116.0:1",
      "size": 1681,
      "mtime": "2012-02-21 08:58:23.666639",
      "lost": 0,
      "wrlock_by": "unknown.0.0:0",
      "snaps": [],
      "truncate_seq": 0,
      "truncate_size": 0,
      "watchers": {}}

Alternatively, perhaps you wish to dump an internal CephFS metadata object, you might
do that like this:

::

   $ rados -p metadata get mds_snaptable mds_snaptable.bin
   $ ceph-dencoder type SnapServer skip 8 import mds_snaptable.bin decode dump_json
   { "snapserver": { "last_snap": 1,
      "pending_noop": [],
      "snaps": [],
      "need_to_purge": {},
      "pending_create": [],
      "pending_destroy": []}} 


Availability
============

**ceph-dencoder** is part of Ceph, a massively scalable, open-source, distributed storage system. Please
refer to the Ceph documentation at http://ceph.com/docs for more
information.


See also
========

:doc:`ceph <ceph>`\(8)
