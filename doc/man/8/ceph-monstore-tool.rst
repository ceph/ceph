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

:program:`ceph-monstore-tool` is similar to `ceph-kvstore-tool`. It allows
users to manipulate MonitorDBStore's data (monmap, osdmap, etc.) offline.
The default RocksDB debug level is `0`. This can be changed using `--debug`.

Please Note:
    Ceph-specific options should be in the format `--option-name=VAL`
    (specifically, do not forget the '='!!)
    Command-specific options need to be passed after a `--`
    e.g., `get monmap --debug -- --version 10 --out /tmp/foo`

Commands
========

:program:`ceph-monstore-tool` utility uses many commands for debugging purpose
which are as follows:

:command:`store-copy <path>`
    Copies the store to PATH.

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

:command:`get osd_snap <key> [-- options]`
    Get osd_snap key (`purged_snap` or `purged_epoch`).

:command:`dump-keys`
    Dumps store keys to FILE (default: stdout).

:command:`dump-paxos [-- options]`
    Dumps Paxos transactions  (-- -- help for more info).

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

:command:`rm <prefix> <key>`
    Remove specified key from the store.

Availability
============

**ceph-kvstore-tool** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at https://docs.ceph.com for more information.


See also
========

:doc:`ceph <ceph>`\(8)
