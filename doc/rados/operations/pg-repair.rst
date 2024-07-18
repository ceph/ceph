============================
Repairing PG Inconsistencies
============================
Sometimes a Placement Group (PG) might become ``inconsistent``. To return the PG
to an ``active+clean`` state, you must first determine which of the PGs has become
inconsistent and then run the ``pg repair`` command on it. This page contains
commands for diagnosing PGs and the command for repairing PGs that have become
inconsistent.

.. highlight:: console

Commands for Diagnosing PG Problems
===================================
The commands in this section provide various ways of diagnosing broken PGs.

To see a high-level (low-detail) overview of Ceph cluster health, run the
following command:

.. prompt:: bash #

   ceph health detail

To see more detail on the status of the PGs, run the following command:

.. prompt:: bash #

   ceph pg dump --format=json-pretty

To see a list of inconsistent PGs, run the following command:

.. prompt:: bash #

   rados list-inconsistent-pg {pool}

To see a list of inconsistent RADOS objects, run the following command:

.. prompt:: bash #

   rados list-inconsistent-obj {pgid}

To see a list of inconsistent snapsets in a specific PG, run the following
command:

.. prompt:: bash #

   rados list-inconsistent-snapset {pgid}


Commands for Repairing PGs
==========================
The form of the command to repair a broken PG is as follows:

.. prompt:: bash #

   ceph pg repair {pgid}

Here ``{pgid}`` represents the id of the affected PG.

For example:

.. prompt:: bash #

   ceph pg repair 1.4

.. note:: PG IDs have the form ``N.xxxxx``, where ``N`` is the number of the
   pool that contains the PG. The command ``ceph osd listpools`` and the
   command ``ceph osd dump | grep pool`` return a list of pool numbers.

More Information on PG Repair
=============================
Ceph stores and updates the checksums of objects stored in the cluster. When a
scrub is performed on a PG, the OSD attempts to choose an authoritative copy
from among its replicas. Only one of the possible cases is consistent. After
performing a deep scrub, Ceph calculates the checksum of an object that is read
from disk and compares it to the checksum that was previously recorded. If the
current checksum and the previously recorded checksum do not match, that
mismatch is considered to be an inconsistency. In the case of replicated pools,
any mismatch between the checksum of any replica of an object and the checksum
of the authoritative copy means that there is an inconsistency. The discovery
of these inconsistencies cause a PG's state to be set to ``inconsistent``.

The ``pg repair`` command attempts to fix inconsistencies of various kinds. If
``pg repair`` finds an inconsistent PG, it attempts to overwrite the digest of
the inconsistent copy with the digest of the authoritative copy. If ``pg
repair`` finds an inconsistent replicated pool, it marks the inconsistent copy
as missing. In the case of replicated pools, recovery is beyond the scope of
``pg repair``.

In the case of erasure-coded and BlueStore pools, Ceph will automatically
perform repairs if ``osd_scrub_auto_repair`` (default ``false``) is set to
``true`` and if no more than ``osd_scrub_auto_repair_num_errors`` (default
``5``) errors are found.

The ``pg repair`` command will not solve every problem. Ceph does not
automatically repair PGs when they are found to contain inconsistencies.

The checksum of a RADOS object or an omap is not always available. Checksums
are calculated incrementally. If a replicated object is updated
non-sequentially, the write operation involved in the update changes the object
and invalidates its checksum. The whole object is not read while the checksum
is recalculated. The ``pg repair`` command is able to make repairs even when
checksums are not available to it, as in the case of Filestore. Users working
with replicated Filestore pools might prefer manual repair to ``ceph pg
repair``.

This material is relevant for Filestore, but not for BlueStore, which has its
own internal checksums. The matched-record checksum and the calculated checksum
cannot prove that any specific copy is in fact authoritative. If there is no
checksum available, ``pg repair`` favors the data on the primary, but this
might not be the uncorrupted replica. Because of this uncertainty, human
intervention is necessary when an inconsistency is discovered. This
intervention sometimes involves use of ``ceph-objectstore-tool``.

External Links
==============
https://ceph.io/geen-categorie/ceph-manually-repair-object/ - This page
contains a walkthrough of the repair of a PG. It is recommended reading if you
want to repair a PG but have never done so.
