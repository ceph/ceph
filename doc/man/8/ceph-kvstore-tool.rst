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

:program:`ceph-kvstore-tool` is a kvstore manipulation tool. It allows users to manipule
leveldb/rocksdb's data (like OSD's omap) offline.

Commands
========

:program:`ceph-kvstore-tool` utility uses many commands for debugging purpose
which are as follows:

:command:`list [prefix]`
    Print key of all KV pairs stored with the URL encoded prefix.

:command:`list-crc [prefix]`
    Print CRC of all KV pairs stored with the URL encoded prefix.

:command:`exists <prefix> [key]`
    Check if there is any KV pair stored with the URL encoded prefix. If key
    is also specified, check for the key with the prefix instead.

:command:`get <prefix> <key> [out <file>]`
    Get the value of the KV pair stored with the URL encoded prefix and key.
    If file is also specified, write the value to the file.

:command:`crc <prefix> <key>`
    Get the CRC of the KV pair stored with the URL encoded prefix and key. 

:command:`get-size [<prefix> <key>]`
    Get estimated store size or size of value specified by prefix and key.

:command:`set <prefix> <key> [ver <N>|in <file>]`
    Set the value of the KV pair stored with the URL encoded prefix and key. 
    The value could be *version_t* or text.

:command:`rm <prefix> <key>`
    Remove the KV pair stored with the URL encoded prefix and key.

:command:`rm-prefix <prefix>`
    Remove all KV pairs stored with the URL encoded prefix.

:command:`store-copy <path> [num-keys-per-tx]`
    Copy all KV pairs to another directory specified by ``path``. 
    [num-keys-per-tx] is the number of KV pairs copied for a transaction.

:command:`store-crc <path>`
    Store CRC of all KV pairs to a file specified by ``path``.

:command:`compact`
    Subcommand ``compact`` is used to compact all data of kvstore. It will open
    the database, and trigger a database's compaction. After compaction, some 
    disk space may be released.

:command:`compact-prefix <prefix>`
    Compact all entries specified by the URL encoded prefix. 
   
:command:`compact-range <prefix> <start> <end>`
    Compact some entries specified by the URL encoded prefix and range.

Availability
============

**ceph-kvstore-tool** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

:doc:`ceph <ceph>`\(8)
