:orphan:

======================================================
 ceph-monstore-tool -- ceph monstore manipulation tool
======================================================

.. program:: ceph-monstore-tool

Synopsis
========

| **ceph-monstore-tool** <store path> <cmd> [args|options]


Description
===========

:program:`ceph-monstore-tool` is used to manipulate MonitorDBStore's data
(monmap, osdmap, etc.) offline. It is similar to `ceph-kvstore-tool`.

Note:
    Ceph-specific options take the format `--option-name=VAL`
    DO NOT FORGET THE EQUALS SIGN. ('=')
    for example, `dump-keys --debug-rocksdb=0`

    Command-specific options must be passed after a `--`
    for example, `get monmap -- --version 10 --out /tmp/foo`

Commands
========

:program:`ceph-monstore-tool` uses many commands for debugging purposes:

:command:`store-copy <path>`
    Copy the store to PATH.

:command:`get monmap [-- options]`
    Get monmap (version VER if specified) (default: last committed).

:command:`get osdmap [-- options]`
    Get osdmap (version VER if specified) (default: last committed).

:command:`get msdmap [-- options]`
    Get msdmap (version VER if specified) (default: last committed).

:command:`get mgr [-- options]`
    Get mgrmap (version VER if specified) (default: last committed).

:command:`get crushmap [-- options]`
    Get crushmap (version VER if specified) (default: last committed).

:command:`get-key <prefix> <key> [-- options]`
    Get key to FILE (default: stdout).

:command:`remove-key <prefix> <key> [-- options]`
    Remove key.

:command:`dump-keys`
    Dump store keys to FILE (default: stdout).

:command:`dump-paxos [-- options]`
    Dump Paxos transactions  (-- -- help for more info).

:command:`dump-trace FILE  [-- options]`
    Dump contents of trace file FILE (-- --help for more info).

:command:`replay-trace FILE  [-- options]`
    Replay trace from FILE (-- --help for more info).

:command:`random-gen [-- options]`
    Add randomly genererated ops to the store (-- --help for more info).

:command:`rewrite-crush [-- options]`
    Add a rewrite commit to the store

:command:`rebuild`
    Rebuild store.

Availability
============

**ceph-monstore-tool** is part of Ceph, a massively scalable, open-source,
distributed storage system. See the Ceph documentation at
https://docs.ceph.com for more information.


See also
========

:doc:`ceph <ceph>`\(8)
