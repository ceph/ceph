==========================
 Journal Config Reference
==========================
.. warning:: Filestore was deprecated in the Reef release and is no longer
   supported. Do not use Filestore for new deployments. Use BlueStore instead.

.. index:: journal; journal configuration

Filestore OSDs use a journal for two reasons: speed and consistency.
BlueStore has been the default and recommended OSD back end since Luminous.
This information is provided for reference for clusters with pre-existing
Filestore OSDs that have not yet been migrated to BlueStore.

- **Speed:** The journal enables the Ceph OSD Daemon to commit small writes 
  quickly. Ceph writes small, random I/O to the journal sequentially, which 
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
.. confval:: journal_aio
.. confval:: journal_block_align
.. confval:: journal_max_write_bytes
.. confval:: journal_max_write_entries
.. confval:: journal_align_min_size
.. confval:: journal_zero_on_create
