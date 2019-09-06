:orphan:

=====================================================
 ceph-kvstore-tool -- ceph kvstore manipulation tool
=====================================================

.. program:: ceph-kvstore-tool

Synopsis
========

| **ceph-kvstore-tool** <leveldb|rocksdb|bluestore-kv> <store path> *command* [args...]


Description
===========

:program:`ceph-kvstore-tool` is a tool to interact with key-value database used internally by ceph.
Key-value databases are part of object store instances.
Modes *leveldb* and *rocksdb* are for FileStore and *bluestore-kv* is for BlueStore.
FileStore uses DB only for OMAP, while BlueStore also preserves internal metadata in DB.

OSD must be offline to manipulate on its key-value DB.
Operations are not logically validated. Modifications of BlueStore's metadata are unadvised.

Column Families
===============
In some BlueStore deployments rocksdb key-value database can be internally split into
multiple separate column families. This division is transparent and *column* part is unnecessary. 
To select a specific column family for debug purposes *column* part should be provided.

Commands
========

:program:`ceph-kvstore-tool` utility uses many commands for debugging purpose
which are as follows:

:command:`list [column/][prefix]`
    Print key of all KV pairs stored with the URL encoded prefix.

:command:`list-crc [column/][prefix]`
    Print CRC of all KV pairs stored with the URL encoded prefix.

:command:`dump [column/][prefix]`
    Print key and value of all KV pairs stored with the URL encoded prefix.

:command:`exists [column/]<prefix> [key]`
    Check if there is any KV pair stored with the URL encoded prefix. If key
    is also specified, check for the key with the prefix instead.

:command:`get [column/]<prefix> <key> [out <file>]`
    Get the value of the KV pair stored with the URL encoded prefix and key.
    If file is also specified, write the value to the file.

:command:`crc [column/]<prefix> <key>`
    Get the CRC of the KV pair stored with the URL encoded prefix and key. 

:command:`get-size [[column/]<prefix> <key>]`
    Get estimated store size or size of value specified by prefix and key.

:command:`set [column/]<prefix> <key> [ver <N>|in <file>]`
    Set the value of the KV pair stored with the URL encoded prefix and key. 
    The value could be *version_t* or text.

:command:`rm [column/]<prefix> <key>`
    Remove the KV pair stored with the URL encoded prefix and key.

:command:`rm-prefix [column/]<prefix>`
    Remove all KV pairs stored with the URL encoded prefix.

:command:`list-columns`
    List column families present in database.

:command:`store-copy <path> [num-keys-per-tx]`
    Copy all KV pairs to another directory specified by ``path``. 
    [num-keys-per-tx] is the number of KV pairs copied for a transaction.

:command:`store-crc <path>`
    Store CRC of all KV pairs to a file specified by ``path``.

:command:`compact`
    Subcommand ``compact`` is used to compact all data of kvstore. It will open
    the database, and trigger a database's compaction. After compaction, some 
    disk space may be released.

:command:`compact-prefix [column/]<prefix>`
    Compact all entries specified by the URL encoded prefix. 
   
:command:`compact-range [column/]<prefix> <start> <end>`
    Compact some entries specified by the URL encoded prefix and range.

:command:`destructive-repair`
    Make a (potentially destructive) effort to recover a corrupted database.
    Note that in the case of rocksdb this may corrupt an otherwise uncorrupted
    database--use this only as a last resort!

:command:`stats`
    Prints statistics from underlying key-value database. This is only for informative purposes.
    Format and information content may vary between releases. For RocksDB information includes
    compactions stats, performance counters, memory usage and internal RocksDB stats. 

Availability
============

**ceph-kvstore-tool** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

:doc:`ceph <ceph>`\(8)
