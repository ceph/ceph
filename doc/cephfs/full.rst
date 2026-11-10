
Handling a full Ceph file system
================================

When a Ceph cluster reaches :confval:`mon_osd_full_ratio`, pools with PGs on
full OSDs are marked with the ``full`` flag.  Most clients then pause writes
to those pools until the flag clears.  Reads are not blocked by the full ratio.

Kernel CephFS clients experience additional fullness behavior.

A full file system leads to ENOSPC results from:

 * Data writes on the client
 * Metadata operations other than deletes and truncates

Because the full condition may not be encountered until
data is flushed to disk (sometime after a ``write`` call has already
returned 0), the ENOSPC error may not be seen until the application
calls ``fsync`` or ``fclose`` (or equivalent) on the file handle.

Calling ``fsync`` is guaranteed to reliably indicate whether the data
made it to disk, and will return an error if it doesn't.  ``fclose`` will
only return an error if buffered data happened to be flushed since
the last write -- a successful ``fclose`` does not guarantee that the
data made it to disk, and in a full-space situation, buffered data
may be discarded after an ``fclose`` if no space is available to persist it.

.. warning::
    If an application appears to be misbehaving on a full file system,
    check that it is performing ``fsync()`` calls as necessary to ensure
    data is on disk before proceeding.

Data writes may be cancelled by the client if they are in flight at the
time the OSD full flag is sent.  Clients update the ``osd_epoch_barrier``
when releasing capabilities on files affected by cancelled operations, in
order to ensure that these cancelled operations do not interfere with
subsequent access to the data objects by the MDS or other clients.  For
more on the epoch barrier mechanism, see :ref:`background_blocklisting_and_osd_epoch_barrier`.
