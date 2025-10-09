:orphan:

=======================================
 rados -- rados object storage utility
=======================================

.. program:: rados

Synopsis
========

| **rados** [ *options* ] [ *command* ]


Description
===========

**rados** is a utility for interacting with a Ceph object storage
cluster (RADOS), part of the Ceph distributed storage system.


Global Options
==============

.. option:: --object-locator object_locator

   Set object_locator for operation.

.. option:: -p pool, --pool pool

   Interact with the given pool. Required by most commands.

.. option:: --target-pool pool

   Select target pool by name.

.. option:: --pgid

   As an alternative to ``--pool``, ``--pgid`` also allow users to specify the
   PG id to which the command will be directed. With this option, certain
   commands like ``ls`` allow users to limit the scope of the command to the given PG.

.. option:: -N namespace, --namespace namespace

   Specify the rados namespace to use for the object.

.. option:: --all

   Use with ls to list objects in all namespaces.
   Put in CEPH_ARGS environment variable to make this the default.

.. option:: --default

   Use with ls to list objects in default namespace.
   Takes precedence over --all in case --all is in environment.

.. option:: -s snap, --snap snap

   Read from the given pool snapshot. Valid for all pool-specific read operations.

.. option:: --create

   Create the pool or directory that was specified.

.. option:: -i infile

   will specify an input file to be passed along as a payload with the
   command to the monitor cluster. This is only used for specific
   monitor commands.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ceph.conf).

.. option:: -b block_size

  Set the block size for put/get/append ops and for write benchmarking.

.. option:: --striper

   Uses the striping API of rados rather than the default one.
   Available for stat, stat2, get, put, append, truncate, rm, ls
   and all xattr related operation.

.. option:: -O object_size, --object-size object_size

   Set the object size for put/get ops and for write benchmarking.

.. option:: --max-objects

   Set the max number of objects for write benchmarking.

.. option:: --lock-cookie locker-cookie

   Will set the lock cookie for acquiring advisory lock (lock get command).
   If the cookie is not empty, this option must be passed to lock break command
   to find the correct lock when releasing lock.

.. option:: --target-locator

   Use with cp to specify the locator of the new object.

.. option:: --target-nspace

   Use with cp to specify the namespace of the new object.


Bench options
=============

.. option:: -t N, --concurrent-ios=N

   Set number of concurrent I/O operations.

.. option:: --show-time

   Prefix output with date/time.

.. option:: --no-verify

   Do not verify contents of read objects.

.. option:: --write-object

   Write contents to the objects.

.. option:: --write-omap

   Write contents to the omap.

.. option:: --write-xattr

   Write contents to the extended attributes.


Load gen options
================

.. option:: --num-objects

   Total number of objects.

.. option:: --min-object-size

  Min object size.

.. option:: --max-object-size

   Max object size.

.. option:: --min-op-len

   Min io size of operations.

.. option:: --max-op-len

   Max io size of operations.

.. option:: --max-ops

   Max number of operations.

.. option:: --max-backlog

   Max backlog size.

.. option:: --read-percent

   Percent of operations that are read.

.. option:: --target-throughput

   Target throughput (in bytes).

.. option:: --run-length

   Total time (in seconds).

.. option:: --offset-align

   At what boundary to align random op offsets.


Cache pools options
===================

.. option:: --with-clones

   Include clones when doing flush or evict.


OMAP options
============

.. option:: --omap-key-file file

   Read the omap key from a file.


Generic options
===============

.. option:: -c FILE, --conf FILE

   Read configuration from the given configuration file.

.. option:: --id ID

   Set ID portion of my name.

.. option:: -n TYPE.ID, --name TYPE.ID

   Set cephx user name.

.. option:: --cluster NAME

   Set cluster name (default: ceph).

.. option:: --setuser USER

   Set uid to user or uid (and gid to user's gid).

.. option:: --setgroup GROUP

   Set gid to group or gid.

.. option:: --version

   Show version and quit.


Global commands
===============

:command:`lspools`
  List object pools

:command:`df`
  Show utilization statistics, including disk usage (bytes) and object
  counts, over the entire system and broken down by pool.

:command:`list-inconsistent-pg` *pool*
  List inconsistent PGs in given pool.

:command:`list-inconsistent-obj` *pgid*
  List inconsistent objects in given PG.

:command:`list-inconsistent-snapset` *pgid*
  List inconsistent snapsets in given PG.


Pool specific commands
======================

:command:`get` *name* *outfile*
  Read object name from the cluster and write it to outfile.

:command:`put` *name* *infile* [--offset offset]
  Write object name with start offset (default:0) to the cluster with contents from infile.
  **Warning:** The put command creates a single RADOS object, sized just as
  large as your input file. Unless your objects are of reasonable and consistent sizes, that
  is probably not what you want -- consider using RGW/S3, CephFS, or RBD instead.

:command:`append` *name* *infile*
  Append object name to the cluster with contents from infile.

:command:`rm` [--force-full] *name* ...
  Remove object(s) with name(s). With ``--force-full`` will remove when cluster is marked full.

:command:`listwatchers` *name*
  List the watchers of object name.

:command:`ls` *outfile*
  List objects in the given pool and write to outfile. Instead of ``--pool`` if ``--pgid`` will be specified, ``ls`` will only list the objects in the given PG.

:command:`lssnap`
  List snapshots for given pool.

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

:command:`cleanup` [ --run-name *run_name* ] [ --prefix *prefix* ]
  Clean up a previous benchmark operation.
  Note: the default run-name is "benchmark_last_metadata"

:command:`listxattr` *name*
  List all extended attributes of an object.

:command:`getxattr` *name* *attr*
  Dump the extended attribute value of *attr* of an object.

:command:`setxattr` *name* *attr* *value*
  Set the value of *attr* in the extended attributes of an object.

:command:`rmxattr` *name* *attr*
  Remove *attr* from the extended attributes of an object.

:command:`stat` *name*
   Get stat (ie. mtime, size) of given object

:command:`stat2` *name*
   Get stat (similar to stat, but with high precision time) of given object

:command:`listomapkeys` *name*
  List all the keys stored in the object map of object name.

:command:`listomapvals` *name*
  List all key/value pairs stored in the object map of object name.
  The values are dumped in hexadecimal.

:command:`getomapval` [ --omap-key-file *file* ] *name* *key* [ *out-file* ]
  Dump the hexadecimal value of key in the object map of object name.
  If the optional *out-file* argument is not provided, the value will be
  written to standard output.

:command:`setomapval` [ --omap-key-file *file* ] *name* *key* [ *value* ]
  Set the value of key in the object map of object name. If the optional
  *value* argument is not provided, the value will be read from standard
  input.

:command:`rmomapkey` [ --omap-key-file *file* ] *name* *key*
  Remove key from the object map of object name.

:command:`getomapheader` *name*
  Dump the hexadecimal value of the object map header of object name.

:command:`setomapheader` *name* *value*
  Set the value of the object map header of object name.

:command:`export` *filename*
  Serialize pool contents to a file or standard output.\n"

:command:`import` [--dry-run] [--no-overwrite] < filename | - >
  Load pool contents from a file or standard input


Examples
========

To view cluster utilization::

       rados df

To get a list object in pool foo sent to stdout::

       rados -p foo ls -

To get a list of objects in PG 0.6::

       rados --pgid 0.6 ls

To write an object::

       rados -p foo put myobject blah.txt

To create a snapshot::

       rados -p foo mksnap mysnap

To delete the object::

       rados -p foo rm myobject

To read a previously snapshotted version of an object::

       rados -p foo -s mysnap get myobject blah.txt.old

To list inconsistent objects in PG 0.6::

       rados list-inconsistent-obj 0.6 --format=json-pretty


Availability
============

**rados** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at https://docs.ceph.com for more information.


See also
========

:doc:`ceph <ceph>`\(8)
