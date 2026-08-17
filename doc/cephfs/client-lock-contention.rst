=====================================
CephFS Client Lock and I/O Concurrency
=====================================

The libcephfs and ceph-fuse user-space clients serialize most metadata and
per-file state behind a single mutex, ``Client::client_lock``.  Recent work
narrows the critical sections on the read and write paths so that large
asynchronous I/O (especially 4 MiB ``preadv``/``pwritev`` from Ganesha NFS)
does not stall other threads that also need ``client_lock``.  This page
describes what the lock protects, how the I/O paths use it, and the rules
developers must follow to avoid deadlocks.

See also :doc:`cephfs-io-path` for the high-level data path and
:doc:`capabilities` for cap semantics.


What ``client_lock`` protects
-----------------------------

``client_lock`` is a ``ceph::TrackedLock`` held across nearly all ``Client``
entry points.  While it is held, code may safely touch:

* the inode map, dentry cache, and file-handle table
* per-inode capability state and cap reference counts
* file position, flags, and inline-data metadata
* ObjectCacher bookkeeping tied to each inode's ``ObjectSet``

The lock does **not** need to be held while waiting on RADOS, copying
user buffers, or running ObjectCacher or Filer operations that take
``ObjectCacher::cache_lock`` or ``Objecter`` internal locks.


Why contention mattered
-----------------------

Before these changes, a single asynchronous 4 MiB read or write could hold
``client_lock`` for the entire operation, including:

* blocking in ``get_caps()`` while waiting for MDS-issued capabilities
* copying the full request from the caller's ``iovec`` into a
  ``bufferlist`` (an extra memcpy on every async read submit)
* submitting ObjectCacher or Filer I/O while still holding the lock

With many concurrent async requests (typical for Ganesha NFS with
``Ganesha Async: True``), threads piled up on ``client_lock`` even though
the expensive work did not require it.  Benchmarks showed roughly 40–50%
improvement on aggregate async read and write throughput once the lock was
dropped around the slow sections.


Design principle: async vs sync
-------------------------------

The client distinguishes two I/O modes via the optional ``Context *onfinish``
argument on internal read/write helpers:

**Asynchronous** (``onfinish != nullptr``)
  Drop ``client_lock`` before work that may block on Objecter or
  ObjectCacher locks, before large buffer copies, and before queueing
  completions that will re-enter the client from another thread.

**Synchronous** (``onfinish == nullptr``)
  Keep ``client_lock`` through Filer submit for direct (non-ObjectCacher)
  writes and reads where the baseline did, so request serialization
  matches historical behavior.  Drop the lock only around
  ``C_SaferCond::wait()`` while waiting for I/O completion—not around
  ``filer->read_trunc`` / ``filer->write_trunc`` submit.

Applying the async "drop lock before filer submit" pattern to synchronous
O_DIRECT writes caused a large regression (~40% at 4 MiB) because the
baseline intentionally held ``client_lock`` across ``filer->write_trunc``.
The fix gates the unlock in ``WriteEncMgr_NotBuffered::do_write()`` on
``async == true`` only.


Read path changes
-----------------

Capability acquisition
^^^^^^^^^^^^^^^^^^^^^^

For async reads, ``_read()`` calls ``try_get_caps()`` first.  This
non-blocking check returns ``-EAGAIN`` when caps are not yet available
instead of sleeping under ``client_lock``.  On ``-EAGAIN``, the path falls
back to blocking ``get_caps()`` as before.

Async ``O_DIRECT`` reads set ``want = 0`` so the client does not wait for
``CEPH_CAP_FILE_CACHE`` when data will be fetched from OSDs anyway.

Three read cases
^^^^^^^^^^^^^^^^

After caps are acquired, ``_read()`` branches:

**CASE 1 — ObjectCacher (Fc caps, ``client_oc`` enabled)**
  ``_read_async()`` snapshots inode/ObjectCacher state in an ``InodeOCState``,
  drops ``client_lock``, and calls ``objectcacher->file_read_ex()``.
  For async callers the function returns immediately; completion runs in
  ``C_Read_Async_Finisher``.

**CASE 2 — async OSD read without cache**
  When the client lacks Fc caps or sync read is forced, a
  ``C_Read_Sync_NonBlocking`` context performs the same work as
  ``_read_sync()`` without blocking the calling thread.  Startup queues
  ``start()`` on ``objecter_finisher`` **after** dropping ``client_lock`` so
  filer I/O does not nest ``client_lock`` under Objecter locks.  Each filer
  step completes through ``objecter_finisher``; ``finish_locked()``
  re-acquires ``client_lock`` before touching inode state.

**CASE 3 — blocking read**
  ``_read_sync()`` submits ``filer->read_trunc`` while holding
  ``client_lock``, then drops it only for ``cond->wait()``.

Async ``preadv`` submit
^^^^^^^^^^^^^^^^^^^^^^^

``_preadv_pwritev_locked()`` no longer copies the caller's ``iovec`` into a
``bufferlist`` on async read submit.  The read completes into an internal
buffer; for synchronous ``preadv`` the copy into the user ``iovec`` still
happens after ``_read()`` returns, with a brief lock drop around
``copy_bufferlist_to_iovec()``.


Write path changes
------------------

Deferred ``iovec`` copy
^^^^^^^^^^^^^^^^^^^^^^^

``_write()`` accepts optional ``iovec`` parameters.  For async writes with
non-inline files, it passes the ``iovec`` to ``WriteEncMgr`` via
``set_deferred_iov()`` instead of calling ``append_iovec_to_bufferlist()``
under ``client_lock``.  ``ensure_bl()`` performs the copy later, after the
lock is dropped in ``do_write()``.

Capability acquisition mirrors reads: ``try_get_caps()`` with
``-EAGAIN`` fallback; async ``O_DIRECT`` uses ``want = 0``.

Buffered vs not buffered
^^^^^^^^^^^^^^^^^^^^^^^^

* ``WriteEncMgr_Buffered::do_write()`` — always drops ``client_lock`` before
  ``objectcacher->file_write()`` and calls ``ensure_bl()`` inside the
  unlocked region (ObjectCacher needs ``cache_lock``).

* ``WriteEncMgr_NotBuffered::do_write()`` — calls ``ensure_bl()``, then:

  * **async:** drop ``client_lock``, then ``filer->write_trunc()``
  * **sync:** call ``filer->write_trunc()`` while still holding
    ``client_lock``


Unmount and inode teardown
--------------------------

Inode deletion may call ``objectcacher_purge_set()`` and
``objectcacher_release_set()``, which take ``ObjectCacher::cache_lock``.
These helpers use ``InodeOsetPin`` to keep the inode and ``ObjectSet`` alive,
drop ``client_lock`` for the ObjectCacher call, then re-acquire implicitly
on return.

While ``client_lock`` is dropped during teardown, ``put_inode()`` must not
re-queue the same inode for deferred deletion.  The ``deleting_inodes`` set
(under ``delay_i_lock``) suppresses re-queuing until ObjectCacher release
completes.  ``delay_put_inodes()`` skips inodes present in that set.


Safe use of ``client_lock``: deadlock avoidance
-----------------------------------------------

The client shares the process with ``Objecter``, ``ObjectCacher``, and
finisher threads.  Several locks can be involved in a single I/O.  Follow
these rules when adding or changing code that touches ``client_lock``.


Lock ordering
^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Rule
     - Rationale
   * - Do not hold ``client_lock`` while acquiring ``ObjectCacher::cache_lock``
     - ``ObjecterWriteback`` documents that ``bh_write_commit`` takes
       ``cache_lock``; flush callbacks re-acquire ``client_lock``.  Holding
       both in the wrong order deadlocks.
   * - Do not hold ``client_lock`` across ``Objecter`` RW operations that may
     take ``Objecter::rwlock`` or completion locks
     - Objecter completion threads may need ``client_lock`` via finisher
       wrappers; holding ``client_lock`` while waiting on Objecter inverts
       the order.
   * - Queue work that needs both locks through a finisher
     - ``objecter_finisher`` and ``client_finisher`` break cycles by running
       one side of the handshake without the other lock held.


Pin before unlock
^^^^^^^^^^^^^^^^^

Before ``unique_unlock<client_lock>``, capture anything the unlocked section
needs:

* ``InodeOsetPin`` — ``InodeRef`` plus ``ObjectSet*`` for ObjectCacher calls
* ``InodeOCState`` — layout, snap context, and snap id for read/write/flush
* ``InodeRef`` in completion contexts so the inode outlives the unlocked gap

The pin must remain in scope until the ``unique_unlock`` guard is destroyed.


Finisher and callback helpers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* ``C_Lock_Client_Finisher`` — acquires ``client_lock`` before calling the
  wrapped ``Context::complete()``.  Use when a callback may run on a thread
  that does not hold ``client_lock`` (e.g. after ``filer->write_trunc``).

* ``C_OnFinisher`` + ``objecter_finisher`` — defer starting I/O or
  completing a step so ``client_lock`` is not taken under Objecter locks
  (used by ``C_Read_Sync_NonBlocking::retry()`` and ObjecterWriteback).

* ``ClientLockIfNeeded`` — acquires ``client_lock`` only if the current
  thread does not already hold it.  Use in finisher callbacks that may run
  either from a path already under ``client_lock`` (sync flush) or from a
  bare finisher thread.

Never call ``Context::finish()`` directly on contexts that expect
``finish_locked()`` or ``ClientLockIfNeeded``; the direct path skips
required locking.


ObjectCacher entry points
^^^^^^^^^^^^^^^^^^^^^^^^^

Do not call ``objectcacher->purge_set()``, ``release_set()``, ``file_read``,
or ``file_write`` directly while holding ``client_lock`` unless the helper
already drops the lock.  Use the ``Client::objectcacher_*`` wrappers or
the same ``InodeOsetPin`` + ``unique_unlock`` pattern.


Capability waits
^^^^^^^^^^^^^^^^

Prefer ``try_get_caps()`` on async paths so cap waits do not block unrelated
work under ``client_lock``.  Blocking ``get_caps()`` remains correct when
caps are not immediately available; it may sleep on ``client_lock`` while
waiting for MDS messages—minimize other work in that critical section.


Sync path exception
^^^^^^^^^^^^^^^^^^^

For synchronous non-buffered writes, **keep** ``client_lock`` through
``filer->write_trunc`` submit.  Only async not-buffered writes drop the
lock before filer I/O.  Sync reads similarly keep the lock through filer
submit and drop only for ``cond->wait()``.


Checklist for new I/O code
^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Identify whether the path is async (``onfinish``) or sync.
2. List locks taken in callees (``cache_lock``, Objecter locks, cond waits).
3. Snapshot inode/oset state; pin with ``InodeRef`` or ``InodeOsetPin``.
4. Drop ``client_lock`` before callee; re-acquire in finisher or
   ``ClientLockIfNeeded`` before touching client metadata.
5. For sync filer direct I/O, keep ``client_lock`` through submit unless
   there is a measured reason and regression testing for sync workloads.
6. Add ``ceph_assert(client_lock.is_locked_by_me())`` at entry to helpers
   that require the lock.


Async read flow (CASE 2, no ObjectCacher)
-----------------------------------------

.. ditaa::

            +------------------+
            |  _read()         |
            |  (client_lock)   |
            +--------+---------+
                     |
                     | try_get_caps / get_caps
                     v
            +------------------+
            | drop client_lock |
            | queue starter on |
            | objecter_finisher|
            +--------+---------+
                     |
                     v
            +------------------+
            | C_Read_Sync_     |
            | NonBlocking::    |
            | start / retry    |
            | (no client_lock) |
            +--------+---------+
                     |
                     | filer->read_trunc
                     v
            +------------------+
            | objecter_finisher|
            | -> finish_locked |
            | (client_lock)    |
            +--------+---------+
                     |
                     v
            +------------------+
            | onfinish->       |
            | complete()       |
            +------------------+


Async write flow (not buffered, O_DIRECT)
-----------------------------------------

.. ditaa::

            +------------------+
            |  _write()        |
            |  (client_lock)   |
            +--------+---------+
                     |
                     | try_get_caps; set_deferred_iov
                     v
            +------------------+
            | WriteEncMgr_     |
            | NotBuffered::    |
            | do_write         |
            +--------+---------+
                     |
         +-----------+-----------+
         | async                 | sync
         v                       v
  +--------------+        +--------------+
  | drop lock    |        | keep lock    |
  | ensure_bl()  |        | ensure_bl()  |
  | filer->write |        | filer->write |
  +------+-------+        +------+-------+
         |                       |
         +-----------+-----------+
                     v
            +------------------+
            | I/O completion   |
            | (finisher may    |
            |  use ClientLock  |
            |  IfNeeded)       |
            +------------------+


Related source files
--------------------

* ``src/client/Client.h`` — ``client_lock``, ``ClientLockIfNeeded``,
  ``C_Read_Sync_NonBlocking``, ``C_Lock_Client_Finisher``, ``WriteEncMgr``
* ``src/client/Client.cc`` — ``_read``, ``_write``, ``_read_async``,
  ``objectcacher_*`` helpers, inode teardown
* ``src/client/ClientCaps.cc`` — ``try_get_caps``
* ``src/client/ObjecterWriteback.h`` — finisher wrapping for writeback commits