.. _cephfs-change-notification:

============================
 CephFS Change Notification
============================

CephFS can notify an external consumer about changes to the file system
namespace, so software that keeps its own view of a CephFS tree (a sync
agent, a backup index, a collaboration gateway) can learn about changes
made by other clients without rescanning the whole tree.

Notification is produced by the MDS: each active MDS rank reports the
namespace operations it journals and the client write flushes it handles,
and publishes a path-level event for each one. Events are delivered
fire-and-forget to a Kafka topic; a local file sink is available for
tests. Nothing in the file system depends on an event being delivered:
the producer never blocks the metadata path, never holds up the journal,
and drops (counted, see `Monitoring`_) rather than delays.

.. note:: This is not an audit log or a replication stream. The delivery
   guarantees are the ones described under `Guarantees and limitations`_,
   and nothing more.

Configuring the endpoint
========================

Notification is off by default. The endpoint is chosen when the MDS
starts (the endpoint options are ``startup`` options); the producer is
then switched on with ``mds_notify_enable``, which is a runtime option::

    ceph config set mds mds_notify_kafka_brokers kafka-1:9092,kafka-2:9092
    ceph config set mds mds_notify_kafka_topic cephfs-changes
    ceph config set mds mds_notify_root /volumes/collab
    ceph config set mds mds_notify_enable true

``mds_notify_root`` is the *watch root*: emitted paths are relative to it,
and events for paths outside it are not reported. It defaults to ``/``,
which reports paths relative to the file system root.

Encrypted or authenticated brokers use the ``mds_notify_kafka_security_protocol``
family of options (``SSL``, ``SASL_PLAINTEXT`` or ``SASL_SSL``)::

    ceph config set mds mds_notify_kafka_security_protocol SASL_SSL
    ceph config set mds mds_notify_kafka_sasl_mechanism SCRAM-SHA-512
    ceph config set mds mds_notify_kafka_sasl_username cephmds
    ceph config set mds mds_notify_kafka_sasl_password_file /etc/ceph/kafka.pw
    ceph config set mds mds_notify_kafka_ssl_ca_location /etc/ceph/kafka-ca.pem

Secret material is always a *file* that the MDS reads at startup: the SASL
password (``mds_notify_kafka_sasl_password_file``) and the passphrase of an
encrypted client key (``mds_notify_kafka_ssl_key_password_file``). Only the
paths are stored in the configuration, so ``ceph config dump`` never
contains a credential. Files should be readable by the MDS only (mode
``0600``). If a secret file is missing or unreadable, the notifier stays
off and says so in the log; it never starts with degraded authentication.

.. note:: ``mds_notify_kafka_ssl_verify`` exists so a test environment can
   talk to a broker with a self-signed certificate. It defaults to ``true``
   and should not be turned off in production.

The file endpoint (``mds_notify_file``) appends one event per line to a
local file and is meant for testing; it takes precedence over Kafka when
both are set.

The event stream
================

Every event is one JSON object, delivered as one Kafka message::

    {"mask": 16, "path": "projects/alpha/report.txt"}

A rename carries both ends in a single message::

    {"mask": 0, "path": "", "src_mask": 512, "src_path": "projects/alpha/report.txt", "dest_mask": 1024, "dest_path": "projects/alpha/final.txt"}

``mask`` values are the Linux ``inotify`` bits. The operations reported
are:

* ``16``: a file, symlink or hardlink was created.
* ``65552``: a directory was created (``16`` with the directory bit).
* ``4``: data was written and flushed (``CLOSE_WRITE``).
* ``32``: a file or hardlink was removed.
* ``65568``: a directory was removed (``32`` with the directory bit).
* ``512`` and ``1024``: the source (``MOVED_FROM``) and destination
  (``MOVED_TO``) of one rename or move.

The directory bit is ``ONLYDIR`` (``65536``), set on events whose target is
a directory. Only one action is reported per message: a mask never combines
several of the operations above.

Paths are relative to the watch root and never start with ``/``. A rename
that crosses the watch root reports only the half that is inside it: a move
into the watched tree is reported as the destination alone, and a move out
of it is reported as a removal of the source. Metadata operations
(``chmod``, ``chown``, extended attributes, layouts, snapshots) are not
reported at all.

Topic requirements
------------------

The consumer must see *every* event, so the topic must have a **single
partition** if more than one consumer is running (a consumer group divides
partitions between its members rather than giving each member all
messages). The producer keys every message with the rank that produced it
(``mds.<rank>``), so per-rank ordering survives on the topic even when it
has more than one partition, but a single-partition topic is the supported
configuration.

Nothing filters the stream: dotfiles, ``.snap`` directories, trash and
whatever else the file system contains are all reported. Consumers that
want to ignore some paths should filter on their side.

Monitoring
==========

The ``notify status`` admin socket command reports the endpoint
configuration and the producer's counters on the rank it is sent to::

    ceph tell mds.0 notify status

Counters are ``queued`` (accepted from the metadata path), ``sent``
(handed to the endpoint successfully), and drops, split by cause:
``dropped_queue`` (the in-memory queue was full) and ``dropped_endpoint``
(the endpoint refused the event, for example because the broker was
unreachable and librdkafka's buffers were full). ``last_error`` holds the
most recent endpoint error. The counters are per rank and start again when
a rank restarts or migrates between daemons.

``notify enable`` and ``notify disable`` switch the producer on the rank
they are sent to, without a restart:

.. prompt:: bash #

    ceph tell mds.0 notify disable

Queue sizes are bounded (``mds_notify_queue_size`` and
``mds_notify_kafka_max_queue``) so that a stalled consumer or an
unreachable broker cannot grow MDS memory; when a bound is reached the
event is dropped and counted, and the file system is not slowed down.

Guarantees and limitations
==========================

The producer guarantees:

* events for every namespace operation the MDS journals, including
  hardlinks, and one action per message;
* per-rank order: the events one rank produces are delivered in the order
  it produced them (on a single-partition topic, in that order on the
  wire);
* at-least-once reporting of operations that are re-executed after an MDS
  failover (a client retry is a new operation and is reported again).

The producer does not guarantee:

* any order *between* ranks: each rank emits for the subtrees it owns,
  independently of the others;
* delivery of events that were still queued, or still in flight to the
  broker, when a rank failed; that loss is silent and bounded by the queue
  sizes and the endpoint timeout (``mds_notify_kafka_message_timeout``);
* any indication of a gap on the wire: there is no sequence number and no
  replay;
* ordering between a write flush (``CLOSE_WRITE``) and a later rename of
  the same file: flush timing belongs to the client, so a flush event can
  carry the file's name at the time of the flush.

Consumers are expected to treat the stream as a hint to re-examine a path
idempotently, which makes duplicates and reordering harmless.

Recovering from a suspected gap
-------------------------------

Nothing in the notification stream tells a consumer that it missed an
event. A consumer that wants a bound on staleness (for example after a
long broker outage, or after seeing drop counters) can use the recursive
change time that CephFS maintains on directories: ``ceph.dir.rctime``
advances on every change below the directory, including changes in
subdirectories, and is readable from any mount::

    getfattr -n ceph.dir.rctime /mnt/cephfs/projects

Walking the tree and rescanning directories whose ``rctime`` is newer than
the consumer's last sweep recovers deterministically from lost events,
using only a stock CephFS extended attribute.
