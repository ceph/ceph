============================
Repairing PG inconsistencies
============================
Sometimes a placement group might become "inconsistent". To return the
placement group to an active+clean state, you must first determine which
of the placement groups has become inconsistent and then run the "pg
repair" command on it. This page contains commands for diagnosing placement
groups and the command for repairing placement groups that have become
inconsistent.

.. highlight:: console

Commands for Diagnosing Placement-group Problems
================================================
The commands in this section provide various ways of diagnosing broken placement groups.

The following command provides a high-level (low detail) overview of the health of the ceph cluster::

   # ceph health detail

The following command provides more detail on the status of the placement groups::

   # ceph pg dump --format=json-pretty

The following command lists inconsistent placement groups::

   # rados list-inconsistent-pg {pool}

The following command lists inconsistent rados objects::

   # rados list-inconsistent-obj {pgid}

The following command lists inconsistent snapsets in the given placement group::

   # rados list-inconsistent-snapset {pgid}


Commands for Repairing Placement Groups
=======================================
The form of the command to repair a broken placement group is::

   # ceph pg repair {pgid}

Where ``{pgid}`` is the id of the affected placement group.

For example::

   # ceph pg repair 1.4

More Information on Placement Group Repair
==========================================
Ceph stores and updates the checksums of objects stored in the cluster. When a scrub is performed on a placement group, the OSD attempts to choose an authoritative copy from among its replicas. Among all of the possible cases, only one case is consistent. After a deep scrub, Ceph calculates the checksum of an object read from the disk and compares it to the checksum previously recorded. If the current checksum and the previously recorded checksums do not match, that is an inconsistency. In the case of replicated pools, any mismatch between the checksum of any replica of an object and the checksum of the authoritative copy means that there is an inconsistency.

The "pg repair" command attempts to fix inconsistencies of various kinds. If "pg repair" finds an inconsisent placement group, it attempts to overwrite the digest of the inconsistent copy with the digest of the authoritative copy. If "pg repair" finds an inconsistent replicated pool, it marks the inconsistent copy as missing. Recovery, in the case of replicated pools, is beyond the scope of "pg repair".

For erasure coded and bluestore pools, Ceph will automatically repair if osd_scrub_auto_repair (configuration default "false") is set to true and at most osd_scrub_auto_repair_num_errors (configuration default 5) errors are found.

"pg repair" will not solve every problem. Ceph does not automatically repair placement groups when inconsistencies are found in them.

The checksum of an object or an omap is not always available. Checksums are calculated incrementally. If a replicated object is updated non-sequentially, the write operation involved in the update changes the object and invalidates its checksum. The whole object is not read while recalculating the checksum. "ceph pg repair" is able to repair things even when checksums are not available to it, as in the case of filestore. When replicated filestore pools are in question, users might prefer manual repair to "ceph pg repair". 

The material in this paragraph is relevant for filestore, and bluestore has its own internal checksums. The matched-record checksum and the calculated checksum cannot prove that the authoritative copy is in fact authoritative. In the case that there is no checksum available, "pg repair" favors the data on the primary. this might or might not be the uncorrupted replica. This is why human intervention is necessary when an inconsistency is discovered. Human intervention sometimes means using the "ceph-objectstore-tool".

External Links
==============
https://ceph.io/geen-categorie/ceph-manually-repair-object/ - This page contains a walkthrough of the repair of a placement group, and is recommended reading if you want to repair a placement
group but have never done so.
