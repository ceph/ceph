==========================
 Journal Config Reference
==========================

.. index:: journal; journal configuration

Filestore OSDs use a journal for two reasons: speed and consistency.  Note
that since Luminous, the BlueStore OSD back end has been preferred and default.
This information is provided for pre-existing OSDs and for rare situations where
Filestore is preferred for new deployments.

- **Speed:** The journal enables the Ceph OSD Daemon to commit small writes 
  quickly. Ceph writes small, random i/o to the journal sequentially, which 
  tends to speed up bursty workloads by allowing the backing file system more 
  time to coalesce writes. The Ceph OSD Daemon's journal, however, can lead 
  to spiky performance with short spurts of high-speed writes followed by 
  periods without any write progress as the file system catches up to the 
  journal.

- **Consistency:** Ceph OSD Daemons require a file system interface that 
  guarantees atomic compound operations. Ceph OSD Daemons write a description 
  of the operation to the journal and apply the operation to the file system. 
  This enables atomic updates to an object (for example, placement group 
  metadata). Every few seconds--between ``filestore max sync interval`` and
  ``filestore min sync interval``--the Ceph OSD Daemon stops writes and 
  synchronizes the journal with the file system, allowing Ceph OSD Daemons to 
  trim operations from the journal and reuse the space. On failure, Ceph 
  OSD Daemons replay the journal starting after the last synchronization 
  operation.

Ceph OSD Daemons recognize the following journal settings: 


.. confval:: journal_dio

   Enables direct i/o to the journal. Requires ``journal block
   align`` set to ``true``.

   :type: Boolean
   :required: Yes when using ``aio``.
   :default: ``true``



.. confval:: journal_aio

   .. versionchanged:: 0.61 Cuttlefish

   Enables using ``libaio`` for asynchronous writes to the journal.
   Requires ``journal dio`` set to ``true``.

   :type: Boolean
   :required: No.
   :default: Version 0.61 and later, ``true``. Version 0.60 and earlier, ``false``.


.. confval:: journal_block_align

   Block aligns write operations. Required for ``dio`` and ``aio``.

   :type: Boolean
   :required: Yes when using ``dio`` and ``aio``.
   :default: ``true``


.. confval:: journal_max_write_bytes

   The maximum number of bytes the journal will write at
   any one time.

   :type: Integer
   :required: No
   :default: ``10 << 20``


.. confval:: journal_max_write_entries

   The maximum number of entries the journal will write at
   any one time.

   :type: Integer
   :required: No
   :default: ``100``


.. confval:: journal_queue_max_ops

   The maximum number of operations allowed in the queue at
   any one time.

   :type: Integer
   :required: No
   :default: ``500``


.. confval:: journal_queue_max_bytes

   The maximum number of bytes allowed in the queue at
   any one time.

   :type: Integer
   :required: No
   :default: ``10 << 20``


.. confval:: journal_align_min_size

   Align data payloads greater than the specified minimum.

   :type: Integer
   :required: No
   :default: ``64 << 10``


.. confval:: journal_zero_on_create

   Causes the file store to overwrite the entire journal with
   ``0``'s during ``mkfs``.

   :type: Boolean
   :required: No
   :default: ``false``
