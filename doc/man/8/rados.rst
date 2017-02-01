:orphan:

=======================================
 rados -- rados object storage utility
=======================================

.. program:: rados

Synopsis
========

| **rados** [ -m *monaddr* ] [ mkpool | rmpool *foo* ] [ -p | --pool
  *pool* ] [ -s | --snap *snap* ] [ -i *infile* ] [ -o *outfile* ]
  *command* ...


Description
===========

**rados** is a utility for interacting with a Ceph object storage
cluster (RADOS), part of the Ceph distributed storage system.


Options
=======

.. option:: -p pool, --pool pool

   Interact with the given pool. Required by most commands.

.. option:: -s snap, --snap snap

   Read from the given pool snapshot. Valid for all pool-specific read operations.

.. option:: -i infile

   will specify an input file to be passed along as a payload with the
   command to the monitor cluster. This is only used for specific
   monitor commands.

.. option:: -o outfile

   will write any payload returned by the monitor cluster with its
   reply to outfile. Only specific monitor commands (e.g. osd getmap)
   return a payload.

.. option:: -c ceph.conf, --conf=ceph.conf

   Use ceph.conf configuration file instead of the default
   /etc/ceph/ceph.conf to determine monitor addresses during startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ceph.conf).

.. option:: -b block_size

  Set the block size for put/get ops and for write benchmarking.

.. option:: --striper

   Uses the striping API of rados rather than the default one.
   Available for stat, get, put, truncate, rm, ls and all xattr related operation


Global commands
===============

:command:`lspools`
  List object pools

:command:`df`
  Show utilization statistics, including disk usage (bytes) and object
  counts, over the entire system and broken down by pool.

:command:`mkpool` *foo*
  Create a pool with name foo.

:command:`rmpool` *foo* [ *foo* --yes-i-really-really-mean-it ]
  Delete the pool foo (and all its data)


Pool specific commands
======================

:command:`get` *name* *outfile*
  Read object name from the cluster and write it to outfile.

:command:`put` *name* *infile*
  Write object name to the cluster with contents from infile.

:command:`rm` *name*
  Remove object name.

:command:`listwatchers` *name*
  List the watchers of object name.

:command:`ls` *outfile*
  List objects in given pool and write to outfile.

:command:`lssnap`
  List snapshots for given pool.

:command:`clonedata` *srcname* *dstname* --object-locator *key*
  Clone object byte data from *srcname* to *dstname*.  Both objects must be stored with the locator key *key* (usually either *srcname* or *dstname*).  Object attributes and omap keys are not copied or cloned.

:command:`mksnap` *foo*
  Create pool snapshot named *foo*.

:command:`rmsnap` *foo*
  Remove pool snapshot named *foo*.

:command:`bench` *seconds* *mode* [ -b *objsize* ] [ -t *threads* ]
  Benchmark for *seconds*. The mode can be *write*, *seq*, or
  *rand*. *seq* and *rand* are read benchmarks, either
  sequential or random. Before running one of the reading benchmarks,
  run a write benchmark with the *--no-cleanup* option. The default
  object size is 4 MB, and the default number of simulated threads
  (parallel writes) is 16. The *--run-name <label>* option is useful
  for benchmarking a workload test from multiple clients. The *<label>*
  is an arbitrary object name. It is "benchmark_last_metadata" by
  default, and is used as the underlying object name for "read" and
  "write" ops.
  Note: -b *objsize* option is valid only in *write* mode.
  Note: *write* and *seq* must be run on the same host otherwise the
  objects created by *write* will have names that will fail *seq*.

:command:`cleanup`

:command:`listomapkeys` *name*
  List all the keys stored in the object map of object name.

:command:`listomapvals` *name*
  List all key/value pairs stored in the object map of object name.
  The values are dumped in hexadecimal.

:command:`getomapval` [ --omap-key-file *file* ] *name* *key* [ *out-file* ]
  Dump the hexadecimal value of key in the object map of object name.
  If the optional *out-file* argument isn't provided, the value will be
  written to standard output.

:command:`setomapval` [ --omap-key-file *file* ] *name* *key* [ *value* ]
  Set the value of key in the object map of object name. If the optional
  *value* argument isn't provided, the value will be read from standard
  input.

:command:`rmomapkey` [ --omap-key-file *file* ] *name* *key*
  Remove key from the object map of object name.

:command:`getomapheader` *name*
  Dump the hexadecimal value of the object map header of object name.

:command:`setomapheader` *name* *value*
  Set the value of the object map header of object name.

Examples
========

To view cluster utilization::

       rados df

To get a list object in pool foo sent to stdout::

       rados -p foo ls -

To write an object::

       rados -p foo put myobject blah.txt

To create a snapshot::

       rados -p foo mksnap mysnap

To delete the object::

       rados -p foo rm myobject

To read a previously snapshotted version of an object::

       rados -p foo -s mysnap get myobject blah.txt.old


Availability
============

**rados** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

:doc:`ceph <ceph>`\(8)
