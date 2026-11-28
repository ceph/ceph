============================
 CephFS Change Notification
============================

The MDS can publish a stream of path-level change events for the CephFS
namespace. This is the design of the producer: what it hooks, what it
emits and what it does not do. Operator documentation is in
:ref:`cephfs-change-notification`.

Scope
=====

The feature answers the requests for change notification in CephFS
(trackers `#62215 <https://tracker.ceph.com/issues/62215>`_ and
`#77895 <https://tracker.ceph.com/issues/77895>`_). A kernel client only
sees changes made through its own mount, so a notification covering the
whole namespace has to come from the MDS.

In scope: namespace operations (create, mkdir, unlink, rmdir, rename, hard
link), truncates and client data flushes; a Kafka endpoint and a local
file endpoint for tests; per-rank ordering with detectable loss. Out of
scope: full ``fsnotify`` parity, kernel-client ``inotify``, write
debouncing, path filtering, and audit use cases.

Hook points
===========

``Server::journal_and_reply()`` is the single point every journaled
namespace mutation passes through, and it has what an event needs before
the update is committed: the operation (the ``EUpdate`` type string:
``mknod``, ``openc``, ``symlink``, ``link_local``, ``link_remote``,
``mkdir``, ``unlink_local``, ``unlink_remote``, ``rename``, ``setattr``,
``open_truncate``) and the affected dentry, whose path is built with
``CDentry::make_path_string()``. The request supplies two things the journal
entry does not (``ChangeNotifier::OpHint``):

* a rename's source dentry (``mdr->dn[1].back()``). The inode's primary
  dentry is a different name when a hard link is renamed, and may not be
  linked yet when the rename follows its create closely;
* whether a ``setattr`` changes the size. A truncate is reported as
  ``CLOSE_WRITE``; other setattrs are metadata and are not reported.

Renames whose source is in the MDS-internal tree (``~mdsN`` and its stray
directories) are housekeeping and are not reported: stray migration off a
stopping rank, and reintegration of an unlinked inode into a hard link
that already names it.

``Locker::_do_cap_update()`` covers client data flushes, which do not pass
through ``journal_and_reply()``: a flush carrying dirty ``Fw``/``Fx`` state
is reported as ``CLOSE_WRITE`` under the inode's primary dentry, or under a
hard link if the primary is in a stray directory. Directories and snapshot
inodes are skipped.

While notification is disabled each hook costs one relaxed atomic load.

Event format
============

``src/mds/ChangeNotifyFormat.h`` holds the format as pure functions so it
can be tested without an MDS: one JSON object per message; a move with
both ends in the watch root as one message with ``src_*`` and ``dest_*``
members; a move into the root as the destination half only; a move out as
a ``DELETE`` of the source. Mask values follow the reference consumer's
``CEPH_MDS_NOTIFY_*`` numbering, which borrows inotify's names but not its
values; ``0x10000`` marks a directory. Paths are relative to
``mds_notify_root``. Each byte of a path that is not well-formed UTF-8
(``check_utf8()`` from ``common/utf8.h``) is replaced by U+FFFD, so every
record is valid JSON; such events are counted in ``invalid_utf8``.

Every record ends with ``"rank"``, ``"epoch"`` and ``"seq"``.
``epoch`` is the rank's incarnation: the monitor sets ``inc`` to the FSMap
epoch whenever a daemon takes a rank (``FSMap::promote()``), and
``MDSRank::handle_mds_map()`` passes it to ``ChangeNotifier::set_epoch()``
before the rank serves requests. No MDS-side state is persisted. ``seq``
starts at 1 in each epoch and is assigned when an event is queued, before
the capacity check, so every later loss leaves a hole.

Delivery
========

``ChangeNotifier`` is owned by ``MDSRank``. The hooks build a record and
push it onto a bounded queue (``PositionedQueue`` in
``src/mds/ChangeNotifyQueue.h``, ``mds_notify_queue_size`` records,
drop-the-newest). A drain thread hands queued records to a
``NotifyEndpoint`` every 10 ms. It is not woken per event, because a
condition-variable notify is a system call under ``mds_lock``; it is woken
early when the queue is half full. The drain thread stamps the stream
position, so the commit path only builds the record body.

The endpoint and the drain thread are built at construction, and only if
an endpoint is configured, in every daemon that holds a rank (a
standby-replay daemon included). ``mds_notify_enable`` switches event
production at runtime.

``FileEndpoint`` appends to a local regular file with blocking writes.
``KafkaEndpoint`` (``src/mds/ChangeNotifyKafka.cc``) is a fire-and-forget
idempotent librdkafka producer: ``rd_kafka_produce()`` does not block, a
full librdkafka queue (``mds_notify_kafka_max_queue``) is a refused send,
and a delivery report callback counts messages librdkafka gives up on.
Idempotence keeps per-partition order and avoids duplicates from
librdkafka's internal retries. librdkafka logs go to the MDS log through a
log callback, mapped to debug levels as in ``rgw_kafka.cc``. Messages are
keyed ``<fs_name>.mds.<rank>``; the MDS does not know its fscid (the
``MDSMap`` it receives has none), while the file system name cannot change
while a rank is held.

``ChangeNotifier::shutdown()``, called from
``MDSRankDispatcher::shutdown()`` on ``SIGTERM`` and from the destructor,
stops accepting events, drains the queue and flushes the endpoint for at
most 500 ms. A respawn (``exec``) or a crash loses whatever is queued.

Build
=====

The notifier core and the file endpoint are always built. Only the Kafka
endpoint is optional, behind ``WITH_MDS_KAFKA_ENDPOINT`` (off by default),
in the same way RGW gates its Kafka endpoint. Without it, configuring
brokers leaves notification off with an error in ``last_error``. librdkafka
is discovered once at the top level, inside a function so its submodule
settings stay out of the top-level scope, and only when RGW's or the MDS's
Kafka endpoint is built.

Configuration and administration
================================

Options are ``mds_notify_*`` in ``src/common/options/mds.yaml.in``. The
endpoint options are ``startup``; ``mds_notify_enable`` and
``mds_notify_root`` are ``runtime`` and tracked by the config observer.
``mds_notify_enable`` is the only switch. The admin socket command is
``notify status``.

Secrets (SASL password, client key passphrase) are read from files named by
configuration when the endpoint is built; only the paths are stored,
logged or reported.

Consumer contract
=================

The reference consumer is OpenCloud's posixfs watcher (reva), which maps
CREATE, DELETE, MOVED_TO and CLOSE_WRITE to an idempotent rescan of the
path. It needs one JSON object per message with the members above, paths
relative to the root it watches, the ``CEPH_MDS_NOTIFY_*`` mask values, one
action per message, and partitions that are not shared between separate
consumers. The producer does not filter paths.

Testing
=======

* ``src/test/mds/TestChangeNotifier.cc`` (``unittest_mds_change_notifier``)
  covers the format, UTF-8 replacement, watch-root paths, operation and
  rename classification, and the numbering queue. It needs no MDS.
* ``qa/tasks/cephfs/test_mds_notify.py``
  (``qa/suites/fs/functional/tasks/mds_notify.yaml``) drives the operations
  through a client mount and checks the records the MDS wrote to its file
  endpoint, the stream position, the runtime options and the disabled
  case. It skips on an MDS without the ``notify status`` command.
