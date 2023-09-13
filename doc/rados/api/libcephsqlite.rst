.. _libcephsqlite:

================
 Ceph SQLite VFS
================

This `SQLite VFS`_ may be used for storing and accessing a `SQLite`_ database
backed by RADOS. This allows you to fully decentralize your database using
Ceph's object store for improved availability, accessibility, and use of
storage.

Note what this is not: a distributed SQL engine. SQLite on RADOS can be thought
of like RBD as compared to CephFS: RBD puts a disk image on RADOS for the
purposes of exclusive access by a machine and generally does not allow parallel
access by other machines; on the other hand, CephFS allows fully distributed
access to a file system from many client mounts. SQLite on RADOS is meant to be
accessed by a single SQLite client database connection at a given time.  The
database may be manipulated safely by multiple clients only in a serial fashion
controlled by RADOS locks managed by the Ceph SQLite VFS.


Usage
^^^^^

Normal unmodified applications (including the sqlite command-line toolset
binary) may load the *ceph* VFS using the `SQLite Extension Loading API`_.

.. code:: sql

    .LOAD libcephsqlite.so

or during the invocation of ``sqlite3``

.. code:: sh

   sqlite3 -cmd '.load libcephsqlite.so'

A database file is formatted as a SQLite URI::

    file:///<"*"poolid|poolname>:[namespace]/<dbname>?vfs=ceph

The RADOS ``namespace`` is optional. Note the triple ``///`` in the path. The URI
authority must be empty or localhost in SQLite. Only the path part of the URI
is parsed. For this reason, the URI will not parse properly if you only use two
``//``.

A complete example of (optionally) creating a database and opening:

.. code:: sh

   sqlite3 -cmd '.load libcephsqlite.so' -cmd '.open file:///foo:bar/baz.db?vfs=ceph'

Note you cannot specify the database file as the normal positional argument to
``sqlite3``. This is because the ``.load libcephsqlite.so`` command is applied
after opening the database, but opening the database depends on the extension
being loaded first.

An example passing the pool integer id and no RADOS namespace:

.. code:: sh

   sqlite3 -cmd '.load libcephsqlite.so' -cmd '.open file:///*2:/baz.db?vfs=ceph'

Like other Ceph tools, the *ceph* VFS looks at some environment variables that
help with configuring which Ceph cluster to communicate with and which
credential to use. Here would be a typical configuration:

.. code:: sh

   export CEPH_CONF=/path/to/ceph.conf
   export CEPH_KEYRING=/path/to/ceph.keyring
   export CEPH_ARGS='--id myclientid'
   ./runmyapp
   # or
   sqlite3 -cmd '.load libcephsqlite.so' -cmd '.open file:///foo:bar/baz.db?vfs=ceph'

The default operation would look at the standard Ceph configuration file path
using the ``client.admin`` user.


User
^^^^

The *ceph* VFS requires a user credential with read access to the monitors, the
ability to blocklist dead clients of the database, and access to the OSDs
hosting the database. This can be done with authorizations as simply as:

.. code:: sh

   ceph auth get-or-create client.X mon 'allow r, allow command "osd blocklist" with blocklistop=add' osd 'allow rwx'

.. note:: The terminology change from ``blacklist`` to ``blocklist``; older clusters may require using the old terms.

You may also simplify using the ``simple-rados-client-with-blocklist`` profile:

.. code:: sh

   ceph auth get-or-create client.X mon 'profile simple-rados-client-with-blocklist' osd 'allow rwx'

To learn why blocklisting is necessary, see :ref:`libcephsqlite-corrupt`.


Page Size
^^^^^^^^^

SQLite allows configuring the page size prior to creating a new database. It is
advisable to increase this config to 65536 (64K) when using RADOS backed
databases to reduce the number of OSD reads/writes and thereby improve
throughput and latency.

.. code:: sql

   PRAGMA page_size = 65536

You may also try other values according to your application needs but note that
64K is the max imposed by SQLite.


Cache
^^^^^

The ceph VFS does not do any caching of reads or buffering of writes. Instead,
and more appropriately, the SQLite page cache is used. You may find it is too small
for most workloads and should therefore increase it significantly:


.. code:: sql

   PRAGMA cache_size = 4096

Which will cache 4096 pages or 256MB (with 64K ``page_cache``).


Journal Persistence
^^^^^^^^^^^^^^^^^^^

By default, SQLite deletes the journal for every transaction. This can be
expensive as the *ceph* VFS must delete every object backing the journal for each
transaction. For this reason, it is much faster and simpler to ask SQLite to
**persist** the journal. In this mode, SQLite will invalidate the journal via a
write to its header. This is done as:

.. code:: sql

   PRAGMA journal_mode = PERSIST

The cost of this may be increased unused space according to the high-water size
of the rollback journal (based on transaction type and size).


Exclusive Lock Mode
^^^^^^^^^^^^^^^^^^^

SQLite operates in a ``NORMAL`` locking mode where each transaction requires
locking the backing database file. This can add unnecessary overhead to
transactions when you know there's only ever one user of the database at a
given time. You can have SQLite lock the database once for the duration of the
connection using:

.. code:: sql

   PRAGMA locking_mode = EXCLUSIVE

This can more than **halve** the time taken to perform a transaction. Keep in
mind this prevents other clients from accessing the database.

In this locking mode, each write transaction to the database requires 3
synchronization events: once to write to the journal, another to write to the
database file, and a final write to invalidate the journal header (in
``PERSIST`` journaling mode).


WAL Journal
^^^^^^^^^^^

The `WAL Journal Mode`_ is only available when SQLite is operating in exclusive
lock mode. This is because it requires shared memory communication with other
readers and writers when in the ``NORMAL`` locking mode.

As with local disk databases, WAL mode may significantly reduce small
transaction latency. Testing has shown it can provide more than 50% speedup
over persisted rollback journals in exclusive locking mode. You can expect
around 150-250 transactions per second depending on size.


Performance Notes
^^^^^^^^^^^^^^^^^

The filing backend for the database on RADOS is asynchronous as much as
possible.  Still, performance can be anywhere from 3x-10x slower than a local
database on SSD. Latency can be a major factor. It is advisable to be familiar
with SQL transactions and other strategies for efficient database updates.
Depending on the performance of the underlying pool, you can expect small
transactions to take up to 30 milliseconds to complete. If you use the
``EXCLUSIVE`` locking mode, it can be reduced further to 15 milliseconds per
transaction. A WAL journal in ``EXCLUSIVE`` locking mode can further reduce
this as low as ~2-5 milliseconds (or the time to complete a RADOS write; you
won't get better than that!).

There is no limit to the size of a SQLite database on RADOS imposed by the Ceph
VFS. There are standard `SQLite Limits`_ to be aware of, notably the maximum
database size of 281 TB. Large databases may or may not be performant on Ceph.
Experimentation for your own use-case is advised.

Be aware that read-heavy queries could take significant amounts of time as
reads are necessarily synchronous (due to the VFS API). No readahead is yet
performed by the VFS.


Recommended Use-Cases
^^^^^^^^^^^^^^^^^^^^^

The original purpose of this module was to support saving relational or large
data in RADOS which needs to span multiple objects. Many current applications
with trivial state try to use RADOS omap storage on a single object but this
cannot scale without striping data across multiple objects. Unfortunately, it
is non-trivial to design a store spanning multiple objects which is consistent
and also simple to use. SQLite can be used to bridge that gap.


Parallel Access
^^^^^^^^^^^^^^^

The VFS does not yet support concurrent readers. All database access is protected
by a single exclusive lock.


Export or Extract Database out of RADOS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The database is striped on RADOS and can be extracted using the RADOS cli toolset.

.. code:: sh

    rados --pool=foo --striper get bar.db local-bar.db
    rados --pool=foo --striper get bar.db-journal local-bar.db-journal
    sqlite3 local-bar.db ...

Keep in mind the rollback journal is also striped and will need to be extracted
as well if the database was in the middle of a transaction. If you're using
WAL, that journal will need to be extracted as well.

Keep in mind that extracting the database using the striper uses the same RADOS
locks as those used by the *ceph* VFS. However, the journal file locks are not
used by the *ceph* VFS (SQLite only locks the main database file) so there is a
potential race with other SQLite clients when extracting both files. That could
result in fetching a corrupt journal.

Instead of manually extracting the files, it would be more advisable to use the
`SQLite Backup`_ mechanism instead.


Temporary Tables
^^^^^^^^^^^^^^^^

Temporary tables backed by the ceph VFS are not supported. The main reason for
this is that the VFS lacks context about where it should put the database, i.e.
which RADOS pool. The persistent database associated with the temporary
database is not communicated via the SQLite VFS API.

Instead, it's suggested to attach a secondary local or `In-Memory Database`_
and put the temporary tables there. Alternatively, you may set a connection
pragma:

.. code:: sql

   PRAGMA temp_store=memory


.. _libcephsqlite-breaking-locks:

Breaking Locks
^^^^^^^^^^^^^^

Access to the database file is protected by an exclusive lock on the first
object stripe of the database. If the application fails without unlocking the
database (e.g. a segmentation fault), the lock is not automatically unlocked,
even if the client connection is blocklisted afterward. Eventually, the lock
will timeout subject to the configurations::

    cephsqlite_lock_renewal_timeout = 30000

The timeout is in milliseconds. Once the timeout is reached, the OSD will
expire the lock and allow clients to relock. When this occurs, the database
will be recovered by SQLite and the in-progress transaction rolled back. The
new client recovering the database will also blocklist the old client to
prevent potential database corruption from rogue writes.

The holder of the exclusive lock on the database will periodically renew the
lock so it does not lose the lock. This is necessary for large transactions or
database connections operating in ``EXCLUSIVE`` locking mode. The lock renewal
interval is adjustable via::

    cephsqlite_lock_renewal_interval = 2000

This configuration is also in units of milliseconds.

It is possible to break the lock early if you know the client is gone for good
(e.g. blocklisted). This allows restoring database access to clients
immediately. For example:

.. code:: sh

    $ rados --pool=foo --namespace bar lock info baz.db.0000000000000000 striper.lock
    {"name":"striper.lock","type":"exclusive","tag":"","lockers":[{"name":"client.4463","cookie":"555c7208-db39-48e8-a4d7-3ba92433a41a","description":"SimpleRADOSStriper","expiration":"0.000000","addr":"127.0.0.1:0/1831418345"}]}

    $ rados --pool=foo --namespace bar lock break baz.db.0000000000000000 striper.lock client.4463 --lock-cookie 555c7208-db39-48e8-a4d7-3ba92433a41a

.. _libcephsqlite-corrupt:

How to Corrupt Your Database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is the usual reading on `How to Corrupt Your SQLite Database`_ that you
should review before using this tool. To add to that, the most likely way you
may corrupt your database is by a rogue process transiently losing network
connectivity and then resuming its work. The exclusive RADOS lock it held will
be lost but it cannot know that immediately. Any work it might do after
regaining network connectivity could corrupt the database.

The *ceph* VFS library defaults do not allow for this scenario to occur. The Ceph
VFS will blocklist the last owner of the exclusive lock on the database if it
detects incomplete cleanup.

By blocklisting the old client, it's no longer possible for the old client to
resume its work on the database when it returns (subject to blocklist
expiration, 3600 seconds by default). To turn off blocklisting the prior client, change::

    cephsqlite_blocklist_dead_locker = false

Do NOT do this unless you know database corruption cannot result due to other
guarantees. If this config is true (the default), the *ceph* VFS will cowardly
fail if it cannot blocklist the prior instance (due to lack of authorization,
for example).

One example where out-of-band mechanisms exist to blocklist the last dead
holder of the exclusive lock on the database is in the ``ceph-mgr``. The
monitors are made aware of the RADOS connection used for the *ceph* VFS and will
blocklist the instance during ``ceph-mgr`` failover. This prevents a zombie
``ceph-mgr`` from continuing work and potentially corrupting the database. For
this reason, it is not necessary for the *ceph* VFS to do the blocklist command
in the new instance of the ``ceph-mgr`` (but it still does so, harmlessly).

To blocklist the *ceph* VFS manually, you may see the instance address of the
*ceph* VFS using the ``ceph_status`` SQL function:

.. code:: sql

    SELECT ceph_status();

.. code::

    {"id":788461300,"addr":"172.21.10.4:0/1472139388"}

You may easily manipulate that information using the `JSON1 extension`_:

.. code:: sql

    SELECT json_extract(ceph_status(), '$.addr');

.. code::

   172.21.10.4:0/3563721180

This is the address you would pass to the ceph blocklist command:

.. code:: sh

   ceph osd blocklist add 172.21.10.4:0/3082314560


Performance Statistics
^^^^^^^^^^^^^^^^^^^^^^

The *ceph* VFS provides a SQLite function, ``ceph_perf``, for querying the
performance statistics of the VFS. The data is from "performance counters" as
in other Ceph services normally queried via an admin socket.

.. code:: sql

    SELECT ceph_perf();

.. code::

    {"libcephsqlite_vfs":{"op_open":{"avgcount":2,"sum":0.150001291,"avgtime":0.075000645},"op_delete":{"avgcount":0,"sum":0.000000000,"avgtime":0.000000000},"op_access":{"avgcount":1,"sum":0.003000026,"avgtime":0.003000026},"op_fullpathname":{"avgcount":1,"sum":0.064000551,"avgtime":0.064000551},"op_currenttime":{"avgcount":0,"sum":0.000000000,"avgtime":0.000000000},"opf_close":{"avgcount":1,"sum":0.000000000,"avgtime":0.000000000},"opf_read":{"avgcount":3,"sum":0.036000310,"avgtime":0.012000103},"opf_write":{"avgcount":0,"sum":0.000000000,"avgtime":0.000000000},"opf_truncate":{"avgcount":0,"sum":0.000000000,"avgtime":0.000000000},"opf_sync":{"avgcount":0,"sum":0.000000000,"avgtime":0.000000000},"opf_filesize":{"avgcount":2,"sum":0.000000000,"avgtime":0.000000000},"opf_lock":{"avgcount":1,"sum":0.158001360,"avgtime":0.158001360},"opf_unlock":{"avgcount":1,"sum":0.101000871,"avgtime":0.101000871},"opf_checkreservedlock":{"avgcount":1,"sum":0.002000017,"avgtime":0.002000017},"opf_filecontrol":{"avgcount":4,"sum":0.000000000,"avgtime":0.000000000},"opf_sectorsize":{"avgcount":0,"sum":0.000000000,"avgtime":0.000000000},"opf_devicecharacteristics":{"avgcount":4,"sum":0.000000000,"avgtime":0.000000000}},"libcephsqlite_striper":{"update_metadata":0,"update_allocated":0,"update_size":0,"update_version":0,"shrink":0,"shrink_bytes":0,"lock":1,"unlock":1}}

You may easily manipulate that information using the `JSON1 extension`_:

.. code:: sql

    SELECT json_extract(ceph_perf(), '$.libcephsqlite_vfs.opf_sync.avgcount');

.. code::

    776

That tells you the number of times SQLite has called the xSync method of the
`SQLite IO Methods`_ of the VFS (for **all** open database connections in the
process). You could analyze the performance stats before and after a number of
queries to see the number of file system syncs required (this would just be
proportional to the number of transactions). Alternatively, you may be more
interested in the average latency to complete a write:

.. code:: sql

    SELECT json_extract(ceph_perf(), '$.libcephsqlite_vfs.opf_write');

.. code::

    {"avgcount":7873,"sum":0.675005797,"avgtime":0.000085736}

Which would tell you there have been 7873 writes with an average
time-to-complete of 85 microseconds. That clearly shows the calls are executed
asynchronously. Returning to sync:

.. code:: sql

    SELECT json_extract(ceph_perf(), '$.libcephsqlite_vfs.opf_sync');

.. code::

    {"avgcount":776,"sum":4.802041199,"avgtime":0.006188197}

6 milliseconds were spent on average executing a sync call. This gathers all of
the asynchronous writes as well as an asynchronous update to the size of the
striped file.


Debugging
^^^^^^^^^

Debugging libcephsqlite can be turned on via::

    debug_cephsqlite

If running the ``sqlite3`` command-line tool, use:

.. code:: sh

   env CEPH_ARGS='--log_to_file true --log-file sqlite3.log --debug_cephsqlite 20 --debug_ms 1' sqlite3 ...

This will save all the usual Ceph debugging to a file ``sqlite3.log`` for inspection.


.. _SQLite: https://sqlite.org/index.html
.. _SQLite VFS: https://www.sqlite.org/vfs.html
.. _SQLite Backup: https://www.sqlite.org/backup.html
.. _SQLite Limits: https://www.sqlite.org/limits.html
.. _SQLite Extension Loading API: https://sqlite.org/c3ref/load_extension.html
.. _In-Memory Database: https://www.sqlite.org/inmemorydb.html
.. _WAL Journal Mode: https://sqlite.org/wal.html
.. _How to Corrupt Your SQLite Database: https://www.sqlite.org/howtocorrupt.html
.. _JSON1 Extension: https://www.sqlite.org/json1.html
.. _SQLite IO Methods: https://www.sqlite.org/c3ref/io_methods.html
