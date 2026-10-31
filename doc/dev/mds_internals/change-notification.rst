============================
 CephFS Change Notification
============================

The MDS can publish a stream of path-level change events for the CephFS
namespace, so that software which keeps its own view of a CephFS tree can
learn about changes made by other clients without rescanning. This is the
design of that producer: what it hooks, what it emits, what it guarantees
and what it deliberately does not do. Operator-facing configuration is in
:ref:`cephfs-change-notification`.

Request and scope
=================

The feature answers a long-standing request for change notification in
CephFS (trackers `#62215 <https://tracker.ceph.com/issues/62215>`_ and
`#77895 <https://tracker.ceph.com/issues/77895>`_): the kernel client only
sees changes made through its own mount, and a notification facility that
covers the whole namespace has to be produced by the MDS, which is the
authority for metadata.

The design targets the "Approach C" hook points discussed on #77895 and
keeps the delivery mechanism deliberately small:

* in scope: namespace operations (create, mkdir, unlink, rmdir, rename,
  hardlink) and client write flushes; delivery to Kafka, plus a local file
  sink for tests; per-rank ordering and documented loss semantics;
* out of scope: the ceph-mgr REST notification store from the backup
  design, UDP endpoints, kernel-client ``inotify``/``fanotify``
  (tracker #1296), cap-protocol changes beyond what the hooks need,
  full ``fsnotify`` parity (ACCESS, MODIFY, ATTRIB, OPEN, CLOSE_NOWRITE),
  write-burst debouncing and close detection, path filtering policy, and
  audit or ransomware-detection use cases.

Hook points
===========

``Server::journal_and_reply()`` (``src/mds/Server.cc``) is the single
choke point for journaled namespace mutations, and the call frame has
everything a namespace event needs before the mutation is committed:

* the affected dentry (``dn``, or the source inode ``in`` for a rename),
  from which the path is built with
  ``CDentry::make_path_string(p, /*projected=*/true)``;
* the operation, as the ``EUpdate::type`` string: ``mknod``, ``openc``,
  ``symlink``, ``link_local``, ``link_remote``, ``mkdir``, ``unlink_local``,
  ``unlink_remote``, ``rename``. All namespace operations journal as
  ``EUpdate``; the op string is what distinguishes them.

Two subtleties, both handled in ``ChangeNotifier::journal_op()``:

* a rename journals with ``srci`` and ``destdn``, and at hook time the
  source dentry is still ``in->get_parent_dn()`` (the move is applied from
  the finish callback). When a rename is pipelined behind the source's
  create (``touch f; mv f g``), the actual parent is not linked yet, and
  the source dentry is recovered from the inode's projected parent stack
  (``in->get_oldest_parent_dn()``);
* an unlink is a directory when the metablob records a dir inode, or, for
  files, when the dentry's linkage still says so.

``Locker::_do_cap_update()`` (``src/mds/Locker.cc``) is the second hook,
for the cap-flush path that bypasses ``journal_and_reply()``: a client
write or truncate flush emits ``CLOSE_WRITE`` from the inode's projected
parent. Directory metadata churn, snapshot inodes and flushes of unlinked
(stray-dir) inodes are skipped, since those have no user-visible path.

The capture points are read-only with respect to lock and journal state:
the hooks classify and copy paths, and nothing they do can fail an
operation.

Event format
============

One JSON object per message, matching what existing consumers dispatch on
(see `Consumer contract`_). The format lives in
``src/mds/ChangeNotifyFormat.h`` as pure functions, so it can be asserted
without an MDS:

* a single event: ``{"mask": <int>, "path": "<relative path>"}``;
* a move, both ends inside the watch root: one message with
  ``src_mask``/``src_path`` (MOVED_FROM) and ``dest_mask``/``dest_path``
  (MOVED_TO);
* a move in from outside the watch root: the destination half only;
* a move out of the watch root: a DELETE event on the source, because the
  reference consumer has no MOVED_FROM-only action.

Masks are the Linux ``inotify`` bits, and exactly one action is reported
per message (a mask never combines CREATE with DELETE and so on). Paths
are relative to the watch root (``mds_notify_root``) and events outside it
are not reported.

Delivery architecture
=====================

``ChangeNotifier`` is owned by ``MDSRank`` (one per active rank). The
commit-path hooks only build a record and push it onto a bounded in-memory
queue; a single drain thread per rank hands records to a
``NotifyEndpoint``:

* the queue is bounded by ``mds_notify_queue_size`` and drops the newest
  record when full: the metadata path never waits for the transport;
* the endpoint interface is non-blocking (``send()`` takes the record or
  returns false) and has a file implementation (tests) and a Kafka
  implementation;
* the Kafka endpoint is a small fire-and-forget librdkafka producer. Its
  in-flight bound is ``queue.buffering.max.messages``
  (``mds_notify_kafka_max_queue``) and ``message.timeout.ms``
  (``mds_notify_kafka_message_timeout``) bounds internal retries. A full
  or unreachable endpoint drops records instead of blocking;
* drops are counted by cause: ``dropped_queue`` (the MDS queue was full)
  vs ``dropped_endpoint`` (the endpoint refused or gave up). ``notify
  status`` reports both, the endpoint configuration, and the last error;
* there are no journal entries, no delivery acks and no retries of our
  own: a Kafka outage is invisible to the file system, which is what
  keeps notification off the correctness path.

librdkafka is already an optional Ceph dependency (RGW's bucket
notification endpoint), built from the in-tree submodule or the system
package, so a Kafka endpoint introduces no new external dependency. The
CMake option ``WITH_MDS_NOTIFY`` (off by default) gates all of this, and
the top-level librdkafka discovery is shared with RGW. RGW's Kafka manager
is deliberately not reused: it is shaped for RGW's durable,
many-short-lived-connection model (ack callbacks, per-connection state),
while the MDS contract is fire-and-forget with a bounded queue. A shared
client in ``src/common`` that both RGW and the MDS link is a possible
future direction, independent of this work.

Multi-rank semantics
====================

Every active MDS rank runs its own notifier and emits for the subtrees it
owns, with its own queue and endpoint:

* per-rank order holds; there is no cross-rank order, and no cluster-wide
  sequence number;
* an operation is reported once, by the rank that journals it, including
  cross-rank renames (reported by the destination's rank with both paths)
  and cross-rank hardlinks (``link_remote``/``unlink_remote``);
* a subtree that migrates between ranks emits nothing for the migration
  itself; events simply continue from the new rank, and events queued on
  the old rank can arrive after events the new rank produced;
* every Kafka message is keyed ``mds.<rank>``, so a rank's events stay in
  order on one partition even on a multi-partition topic (per-rank order
  is preserved; the consumer's single-partition requirement is about
  consumer groups, not about the key).

Guarantees
==========

The producer guarantees events for every namespace operation the MDS
journals (one action per message); per-rank ordering; and at-least-once
reporting of operations that are re-executed, which is what makes a
client retry after an MDS failover appear as a duplicate rather than a
loss.

What can be lost: records still in the rank's queue, or in the endpoint's
in-flight buffer, when the rank fails; and everything past a drop when a
queue bound is reached. Loss is silent on the wire (there is no sequence
number or gap indication), and is bounded by the queue sizes, the drain
cadence and ``message.timeout.ms``. Consumers are expected to act
idempotently on each event (a rescan of the path is the natural action),
and can bound staleness with the recursive change time CephFS maintains
on directories (``ceph.dir.rctime``, a stock vxattr), rescanning
directories whose ``rctime`` is newer than their last sweep.

Configuration and administration
================================

Options are under ``mds_notify_*`` in ``src/common/options/mds.yaml.in``;
the endpoint options are ``startup`` (the endpoint is built when the
daemon starts), ``mds_notify_enable`` and ``mds_notify_root`` are runtime.
The admin socket commands are ``notify status``, ``notify enable`` and
``notify disable``, registered in ``MDSDaemon::set_up_admin_socket()``
behind ``WITH_MDS_NOTIFY``.

Secret material (the SASL password and an encrypted client key's
passphrase) is always read from a file named by configuration; only the
path is stored, logged or reported, and the values are wiped from memory
once librdkafka has copied them. A missing secret file leaves the
notifier off rather than starting it with degraded authentication. This
differs from RGW's notification configuration on purpose: RGW's password
arrives per-notification from the pubsub layer, while the MDS has a single
cluster-scoped endpoint whose natural carrier is a file.

Consumer contract
=================

The reference consumer is OpenCloud's posixfs watcher (reva), which maps
CREATE, DELETE, MOVED_TO and CLOSE_WRITE to an idempotent rescan of the
path. It requires: one JSON object per message with the field names above;
paths relative to the same root the consumer watches; inotify mask values;
one action per message; and a topic whose partitions are not shared
between separate consumers (a single partition in practice, since the
consumer group divides partitions between members). The producer sends
relative, non-escaping paths and never combines actions, so a stock
consumer needs no changes. The producer does not filter paths: dotfiles,
``.oc-nodes``-style housekeeping directories and trash are emitted, and
filtering is the consumer's job.

Testing
=======

* ``src/test/mds/TestChangeNotifier.cc`` (``unittest_mds_change_notifier``)
  asserts the wire format: escaping, record assembly including the
  boundary-rename cases, watch-root relative paths, and operation
  classification. It needs no MDS and runs in a default build.
* ``qa/tasks/cephfs/test_mds_notify.py`` (run by
  ``qa/suites/fs/functional/tasks/mds_notify.yaml``) drives the six
  consumed operations through a client mount and checks masks and paths
  against the records the MDS wrote to the file endpoint, plus the admin
  socket surface and the disabled-notifier case.
* ``qa/workunits/fs/notify/change_notify.sh`` is a workunit-level check of
  the same operations that works from producer counters alone (and reads
  the sink over ssh when given a host and path).
