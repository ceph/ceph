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

   As an alternative to ``--pool``, ``--pgid`` also allows users to specify the
   PG id to which the command will be directed. With this option, certain
   commands like ``ls`` allow users to limit the scope of the command to the given PG.

.. option:: -N namespace, --namespace namespace

   Specify the RADOS namespace to use for the object.

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

.. option:: -m monaddress[:port]

   Connect to specified Monitor (instead of looking through ceph.conf).

.. option:: -b block_size

  Set the block size for put/get/append ops and for write benchmarking.

.. option:: --striper

   Uses the striping API of RADOS rather than the default one.
   Available for stat, stat2, get, put, append, truncate, rm, ls
   and all xattr-related operations.

.. option:: -O object_size, --object-size object_size

   Set the object size for put/get ops and for write benchmarking.

.. option:: --max-objects

   Set the max number of objects for write benchmarking.

.. option:: --obj-name-file file

   Use the content of *file* as the object name, and leave out the object
   name argument. The whole content of the file is used, including any
   trailing newline. This allows object names that are hard to type, such
   as names that contain binary data. The ``cp``, ``listwatchers``,
   ``lock``, ``notify``, ``rollback``, ``set-chunk``, ``set-redirect``,
   ``setomapval``, and ``watch`` commands ignore this option.

.. option:: --input-file file

   Use the content of *file* as the value for ``setomapval`` and
   ``setomapheader``, in place of the value argument.

.. option:: --lock-cookie locker-cookie

   Will set the lock cookie for acquiring advisory lock (lock get command).
   If the cookie is not empty, this option must be passed to lock break command
   to find the correct lock when releasing lock.

.. option:: --lock-tag tag

   Set the tag of the lock to take (``lock get``). All the lockers of a
   lock must use the same tag.

.. option:: --lock-description description

   Set a description of the lock to take (``lock get``), which
   ``lock info`` shows.

.. option:: --lock-duration seconds

   Set how long the lock to take (``lock get``) lasts, in seconds. By
   default, the lock does not expire.

.. option:: --lock-type type

   Take a *shared* or an *exclusive* lock (``lock get``). The default is
   *exclusive*.

.. option:: --target-locator

   Use with cp to specify the locator of the new object.

.. option:: --target-nspace

   Use with cp to specify the namespace of the new object.

.. option:: -f format, --format format

    Specify the structured format for output data.
    Format can be *plain*, *json* or *json-pretty*.

.. option:: -o filename, --output filename

    Specify the file path to which structured output should be written.

Bench options
=============

.. option:: -t N, --concurrent-ios=N

   Set number of concurrent I/O operations.

.. option:: --show-time

   Prefix output with date/time.

.. option:: --no-verify

   Do not verify contents of read objects.

.. option:: --no-hints

   Do not send allocation hints with the object writes of a write
   benchmark. By default, each write carries a hint with the object size
   and the write size.

.. option:: --reuse-bench

   Make a write benchmark overwrite the objects of an earlier write
   benchmark, with that benchmark's object size and write size, instead of
   writing new objects. The earlier benchmark must have been run with
   ``--no-cleanup``, and ``--run-name`` selects it.

.. option:: --object, --write-object

   Read or write contents to the objects. This is the default.
   ``--write-object`` is a deprecated name for this option.

.. option:: --omap, --write-omap

   Read or write contents to the omap. ``--write-omap`` is a deprecated
   name for this option.

.. option:: --xattr, --write-xattr

   Write contents to the extended attributes. Read benchmarks do not
   support this option. ``--write-xattr`` is a deprecated name for this
   option.

.. option:: --omap-read-start-after key

   With ``--omap``, make each read of a read benchmark list the omap entries
   that come after *key*.

.. option:: --omap-read-filter-prefix prefix

   With ``--omap``, make each read of a read benchmark list only the omap
   entries whose keys begin with *prefix*.

.. option:: --omap-read-max-return count

   With ``--omap``, make each read of a read benchmark list at most *count*
   omap entries. The default is 1024.


Load gen options
================

.. option:: --num-objects

   Total number of objects.

.. option:: --min-object-size

   Min object size.

.. option:: --max-object-size

   Max object size.

.. option:: --min-op-len

   Min I/O size of operations.

.. option:: --max-op-len

   Max I/O size of operations.

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

   Set ID portion of a client entity. e.g. admin for client.admin

.. option:: -n TYPE.ID, --name TYPE.ID

   Set the full entity name. e.g. client.admin

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

:command:`cppool` *source-pool* *dest-pool* [ --yes-i-really-mean-it ]
  Copy every object in every namespace of *source-pool* to *dest-pool*,
  which must already exist. Each copy gets the data, extended attributes,
  object map, name, and namespace of its source object, but not its locator
  key. Snapshots are not copied, and object versions are not preserved. If
  *source-pool* has self-managed snapshots, such as RBD and CephFS
  snapshots, the command refuses to run unless ``--yes-i-really-mean-it``
  is given.

:command:`purge` *pool* --yes-i-really-really-mean-it
  Remove every object, in every namespace, from *pool* without removing
  the pool itself. This cannot be undone. The command works even when the
  pool is full.

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

:command:`get` *name* *outfile* [--offset offset]
  Read object name from the cluster, starting at offset (default: 0), and
  write it to outfile.

:command:`put` *name* *infile* [--offset offset]
  Write object name with start offset (default:0) to the cluster with contents from infile.
  **Warning:** The put command creates a single RADOS object, sized just as
  large as your input file. Unless your objects are of reasonable and consistent sizes, that
  is probably not what you want -- consider using RGW/S3, CephFS, or RBD instead.

:command:`append` *name* *infile*
  Append object name to the cluster with contents from infile.

:command:`truncate` *name* *length*
  Set the size of object *name* to *length* bytes. Data past *length* is
  discarded. If *length* is larger than the object, the object is extended,
  and the added range reads as zeros.

:command:`create` *name*
  Create an empty object named *name*. The command fails if the object
  already exists.

:command:`rm` [--force-full] *name* ...
  Remove object(s) with name(s). With ``--force-full`` will remove when cluster is marked full.

:command:`cp` *name* [ *target-name* ]
  Copy object *name*, with its data, extended attributes, and object map, to
  *target-name*. The copy is made in the same pool unless ``--target-pool``
  names another pool. ``--target-locator`` and ``--target-nspace`` set the
  locator key and the namespace of the copy. If *target-name* is omitted,
  the copy gets the same name, which requires ``--target-pool`` to name a
  different pool.

:command:`watch` *name*
  Register a watch on object *name*, which must exist, and print each
  notification that arrives, with a hexadecimal dump of its payload, until
  Enter is pressed. Each notification is acknowledged with its own payload
  as the reply.

:command:`notify` *name* *message*
  Send a notification that carries *message* to the watchers of object
  *name*, and wait up to 10 seconds for them to acknowledge it. Print each
  reply, with a hexadecimal dump of its payload, and list the watchers that
  did not reply in time.

:command:`listwatchers` *name*
  List the watchers of object name.

:command:`ls`
  List the objects in the given pool and write them to standard output, or
  to the file given by ``--output``. If ``--pgid`` is given instead of
  ``--pool``, ``ls`` lists only the objects in the given PG.

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

:command:`load-gen` [ *options* ]
  Generate a random load of reads and writes on the pool for
  ``--run-length`` seconds (default: 60), and then remove the objects that
  it created. It first creates ``--num-objects`` objects (default: 200) of
  random sizes between ``--min-object-size`` (default: 1 KiB) and
  ``--max-object-size`` (default: 5 GiB). It then reads and writes random
  ranges of between ``--min-op-len`` (default: 1 KiB) and ``--max-op-len``
  (default: 2 MiB) bytes, of which ``--read-percent`` (default: 80) percent
  are reads, at up to ``--target-throughput`` bytes per second (default:
  5 MiB), with no more than ``--max-ops`` operations (default: 16) and
  ``--max-backlog`` bytes (default: 10 MiB) in flight. ``--offset-align``
  aligns the offsets of the operations.
  Note: the default ``--max-object-size`` is larger than the default
  ``osd_max_object_size`` of 128 MiB, and the OSDs refuse to create larger
  objects, so pass a ``--max-object-size`` that the cluster allows.

:command:`listxattr` *name*
  List all extended attributes of an object.

:command:`getxattr` *name* *attr*
  Dump the extended attribute value of *attr* of an object.

:command:`setxattr` *name* *attr* *value*
  Set the value of *attr* in the extended attributes of an object.

:command:`rmxattr` *name* *attr*
  Remove *attr* from the extended attributes of an object.

:command:`stat` *name*
  Get stat (i.e. mtime, size) of given object

:command:`stat2` *name*
  Get stat (similar to stat, but with high precision time) of given object

:command:`touch` *name* [ *timestamp* ]
  Set the modification time of object *name* to *timestamp*, given in
  seconds since the Unix epoch, or to the current time if *timestamp* is
  omitted. The object is created if it does not exist.

:command:`mapext` *name*
  List the allocated extents of object *name*, one per line, as a
  hexadecimal offset and length. Erasure-coded pools do not support this
  command.

:command:`rollback` *name* *snap*
  Roll back object *name* to its state in pool snapshot *snap*.

:command:`listsnaps` *name*
  List the clones of object *name*, the IDs of the snapshots that each clone
  belongs to, the size of each clone, and the ranges that each clone shares
  with the next newer clone. The current version of the object is listed as
  ``head``. With ``--format``, the output also gives the names of pool
  snapshots.

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
  input. With ``--input-file``, the value is read from *file* instead.

:command:`rmomapkey` [ --omap-key-file *file* ] *name* *key*
  Remove key from the object map of object name.

:command:`clearomap` *name* [ *name* ... ]
  Remove all the keys from the object maps of the given objects.

:command:`getomapheader` *name* [ *out-file* ]
  Dump the hexadecimal value of the object map header of object name.
  If *out-file* is given, write the raw header to *out-file* instead.

:command:`setomapheader` *name* < *value* | --input-file *file* >
  Set the value of the object map header of object name, from *value* or
  from the content of *file*.

:command:`set-alloc-hint` *name* *expected-object-size* *expected-write-size*
  Set an allocation hint on object *name*: the size, in bytes, that the
  object is expected to reach and the expected size of writes to it. The
  object is created if it does not exist.

:command:`set-redirect` *name* [ *target-name* ] [ --with-reference ]
  Make object *name*, which must exist, a redirect to object *target-name*
  in the pool given by ``--target-pool`` (default: the same pool). This
  discards the data, object map, and extended attributes of object *name*.
  If *target-name* is omitted, the target has the same name, which requires
  ``--target-pool`` to name a different pool. With ``--with-reference``,
  the redirect takes a reference on the target object. ``set-redirect``,
  ``set-chunk``, and ``unset-manifest`` act on manifest objects, which
  deduplication uses.

:command:`set-chunk` *name* *offset* *length* *target-name* *target-offset*
  Map the range of object *name* that starts at *offset* and is *length*
  bytes long to the range of object *target-name* that starts at
  *target-offset*, in the pool given by ``--target-pool`` (default: the
  same pool). The range must lie within the object and must not overlap a
  range that is already mapped. The mapping always takes a reference on
  the target object, so ``--with-reference`` has no effect. Object *name*
  cannot be in an erasure-coded pool.

:command:`unset-manifest` *name*
  Remove the redirect or the chunk mappings from object *name*, and drop
  the references that they hold on target objects. Data that is held only
  in target objects is not copied back.

:command:`export` *filename*
  Serialize pool contents to a file or standard output.

:command:`import` [--dry-run] [--no-overwrite] < filename | - >
  Load pool contents from a file or standard input

:command:`lock list` *name*
  List the names of the advisory locks on object *name*.

:command:`lock get` *name* *lock-name* [ *lock options* ]
  Try to take the advisory lock *lock-name* on object *name*. The command
  does not wait: it fails if the lock is held in a way that conflicts. The
  lock is exclusive unless ``--lock-type shared`` is given, and
  ``--lock-cookie``, ``--lock-tag``, ``--lock-description``, and
  ``--lock-duration`` set its other properties. The locker is the client
  entity of this run of **rados**, and the lock stays in place after
  **rados** exits, until it expires or is broken with ``lock break``.

:command:`lock break` *name* *lock-name* *locker* [ --lock-cookie *cookie* ]
  Break the lock *lock-name* on object *name* that is held by *locker*, an
  entity name such as ``client.4123`` as ``lock info`` shows it. If the
  locker took the lock with a cookie, pass the same cookie with
  ``--lock-cookie``.

:command:`lock info` *name* *lock-name*
  Show the type and tag of the lock *lock-name* on object *name*, and the
  name, cookie, description, expiration, and address of each locker.
  ``lock list`` and ``lock info`` print JSON unless ``--format`` selects
  another format.


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
