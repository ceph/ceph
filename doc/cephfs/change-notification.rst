.. _cephfs-change-notification:

============================
 CephFS Change Notification
============================

CephFS can notify an external consumer about changes to the file system
namespace, so software that keeps its own view of a CephFS tree (a sync
agent, a backup index, a collaboration gateway) can learn about changes
made by other clients without rescanning the whole tree.

Each active MDS rank reports the namespace operations it journals and the
client data flushes it handles, as path-level events. Events go to a
Kafka topic, or to a local file for testing. Delivery is fire-and-forget:
the producer never blocks the metadata path, and when it cannot keep up it
drops events and counts them (see `Monitoring`_).

.. note:: This is not an audit log or a replication stream. See
   `Guarantees and limitations`_.

Configuring the endpoint
========================

Notification is off by default. The endpoint is chosen when the MDS starts
(the endpoint options are ``startup`` options); ``mds_notify_enable`` then
switches event production on or off at runtime::

    ceph config set mds mds_notify_kafka_brokers kafka-1:9092,kafka-2:9092
    ceph config set mds mds_notify_kafka_topic cephfs-changes
    ceph config set mds mds_notify_root /volumes/collab
    ceph config set mds mds_notify_enable true

``mds_notify_root`` is the *watch root*: paths in events are relative to
it, and changes outside it are not reported. It defaults to ``/``.
``mds_notify_enable`` and ``mds_notify_root`` take effect on running
daemons. As with any option, a value in a daemon's local ``ceph.conf``
takes precedence over ``ceph config set``.

Every daemon that holds a rank and has an endpoint configured builds the
endpoint, including a standby-replay daemon, which connects to Kafka but
produces nothing until it takes over the rank.

Encrypted or authenticated brokers use ``mds_notify_kafka_security_protocol``
(``SSL``, ``SASL_PLAINTEXT`` or ``SASL_SSL``) and the related options::

    ceph config set mds mds_notify_kafka_security_protocol SASL_SSL
    ceph config set mds mds_notify_kafka_sasl_mechanism SCRAM-SHA-512
    ceph config set mds mds_notify_kafka_sasl_username cephmds
    ceph config set mds mds_notify_kafka_sasl_password_file /etc/ceph/kafka.pw
    ceph config set mds mds_notify_kafka_ssl_ca_location /etc/ceph/kafka-ca.pem

Secrets (the SASL password, the passphrase of an encrypted client key) are
always read from a *file* when the MDS starts; only the path is stored in
the configuration. If a secret file cannot be read, notification stays off
and the reason is logged. ``SASL_PLAINTEXT`` sends the credentials in clear;
prefer ``SASL_SSL``.

The Kafka endpoint is compiled into the MDS only when Ceph is built with
the ``WITH_MDS_KAFKA_ENDPOINT`` CMake option (off by default). Without it,
configuring Kafka brokers leaves notification off, and ``notify status``
and the MDS log say why.

The file endpoint (``mds_notify_file``) appends one event per line to a
regular file on a local file system and is meant for testing; it takes
precedence over Kafka when both are set. Its writes are blocking: a FIFO
without a reader or a hung network file system stops delivery and delays
MDS shutdown (not the metadata path).

The event stream
================

Every event is one JSON object, delivered as one Kafka message::

    {"mask": 16, "path": "projects/alpha/report.txt", "rank": 0, "epoch": 12, "seq": 41}

A rename carries both ends in one message::

    {"mask": 0, "path": "", "src_mask": 512, "src_path": "projects/alpha/report.txt", "dest_mask": 1024, "dest_path": "projects/alpha/final.txt", "rank": 0, "epoch": 12, "seq": 42}

``mask`` values follow the numbering of the reference consumer (OpenCloud's
posixfs watcher). The names come from ``inotify`` but the values are not
the Linux ``IN_*`` values. The events reported are:

* ``16``: a file, symlink or hard link was created.
* ``65552``: a directory was created (``16`` with the directory bit).
* ``4`` (``CLOSE_WRITE``): the content changed. A client flushed dirty file
  data state to the MDS, or the file was truncated. The MDS does not see
  ``close(2)``; one flush can cover several writes.
* ``32``: a file or hard link was removed.
* ``65568``: a directory was removed (``32`` with the directory bit).
* ``512`` and ``1024``: the source and destination of a rename.

``65536`` marks an event whose target is a directory. One message carries
one action. Metadata changes (mode, owner, times, extended attributes,
layouts, snapshots) are not reported, nor is the MDS's internal
housekeeping of unlinked files.

Paths are relative to the watch root and never start with ``/``. A rename
reports the name that was renamed. A rename across the watch root reports
only the half inside it: a move in as the destination alone, a move out as
a removal of the source.

Records are always valid JSON. File names are arbitrary bytes, so each byte
of a name that is not well-formed UTF-8 is replaced by U+FFFD. Such a path
no longer names the file; a consumer seeing U+FFFD should rescan the
parent directory. ``notify status`` counts these events (``invalid_utf8``).

The last three members are the event's stream position (see `Detecting
lost events`_). Kafka messages are keyed ``<fs_name>.mds.<rank>``, so a
rank's events stay in order on one partition. Use a single-partition topic
if more than one consumer reads it, since a consumer group divides
partitions between its members.

Monitoring
==========

``notify status`` reports the endpoint configuration and the producer's
counters on the rank it is sent to::

    ceph tell mds.0 notify status

``queued`` counts events accepted from the metadata path and ``sent``
events handed to the endpoint (for Kafka: accepted by librdkafka; the
endpoint section also shows ``delivered``, acknowledged by the broker).
Drops are split by cause: ``dropped_queue`` (the MDS queue was full),
``dropped_endpoint`` (the endpoint refused the event) and
``dropped_delivery`` (librdkafka accepted it and later gave up, for example
after ``mds_notify_kafka_message_timeout`` during a broker outage).
``dropped`` is their sum. ``epoch`` and ``next_seq`` give the current
stream position, and ``last_error`` the most recent endpoint error.
librdkafka's own log goes to the MDS log.

The MDS queue holds at most ``mds_notify_queue_size`` events (default
8192), and up to as many again can be in the batch being handed to the
endpoint. librdkafka holds at most ``mds_notify_kafka_max_queue`` events
(default 16384), and during an outage events also expire after
``mds_notify_kafka_message_timeout``. An event is about 100 bytes plus its
path, two paths for a rename. This memory is not part of
``mds_cache_memory_limit``.

Guarantees and limitations
==========================

The MDS hands events to the endpoint in batches about every 10 ms, sooner
when its queue fills.

* Events for every namespace operation the MDS journals, one action per
  message.
* Per-rank order: a rank's events arrive in the order it produced them.
  The Kafka endpoint uses librdkafka's idempotent producer, so its internal
  retries neither reorder nor duplicate messages. Brokers with ACLs may need
  to allow idempotent writes for the MDS principal.
* No order between ranks: each rank reports the subtrees it owns.
* Events are produced when the operation is submitted to the journal,
  before it is durable. An event can describe an operation that a crash
  then loses; if the client re-sends the request to the next MDS, the
  operation is reported again under the new epoch.
* Events are lost when a queue bound is reached (the newest event is
  dropped) and when a daemon crashes or respawns (``ceph mds fail``,
  removal from the MDS map) with events still queued. On ``SIGTERM`` the
  MDS drains its queue and gives the endpoint up to 500 ms to flush.
* There is no replay. Consumers should treat each event as a hint to
  re-examine a path idempotently, which makes duplicates harmless.

Detecting lost events
---------------------

Each rank numbers its events. ``epoch`` is the rank's incarnation in the
MDS map: it grows every time a daemon takes the rank (restart, failover,
standby-replay takeover). ``seq`` starts at 1 in each epoch and grows by
one for every event the rank produces while notification is enabled. The
number is assigned before the event can be dropped, so a loss anywhere on
the way leaves a hole.

A consumer keeps the last ``(epoch, seq)`` for each stream (each Kafka
message key, or each sink file) and compares every event against it:

* same epoch, ``seq`` is last + 1: in order;
* same epoch, ``seq`` greater than last + 1: ``seq - last - 1`` events were
  lost; rescan (see below);
* same epoch, ``seq`` not greater than last: a duplicate; ignore it;
* higher epoch: the rank restarted and the tail of the previous epoch is
  unknown; treat it as a possible gap;
* lower epoch: a late message from a previous incarnation.

A loss at the end of a burst is only seen when the rank's next event
arrives; ``next_seq`` in ``notify status`` gives a bound.

Recovering from a gap
---------------------

The recursive change time CephFS keeps on directories, ``ceph.dir.rctime``,
advances on every change below a directory and is readable from any
mount::

    getfattr -n ceph.dir.rctime /mnt/cephfs/projects

Rescanning the directories whose ``rctime`` is newer than the consumer's
last sweep recovers from lost events.
