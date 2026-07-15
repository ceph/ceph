==========================
 Journal Config Reference
==========================
.. warning:: Filestore is not supported. Migrate existing Filestore OSDs to
   BlueStore. See :ref:`rados_operations_bluestore_migration`.

.. index:: journal; journal configuration

The following journal settings are provided for reference to assist with
migration of existing Filestore OSDs to BlueStore.

.. confval:: journal_dio
.. confval:: journal_aio
.. confval:: journal_block_align
.. confval:: journal_max_write_bytes
.. confval:: journal_max_write_entries
.. confval:: journal_align_min_size
.. confval:: journal_zero_on_create
