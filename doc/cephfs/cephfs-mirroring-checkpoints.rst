========================================
CephFS Snapshot Mirroring Checkpoints
========================================

.. contents::
   :depth: 3
   :local:

Introduction
============

CephFS Snapshot Mirroring Checkpoints allow you to mark specific snapshots as important
milestones and track whether they have been successfully replicated to your remote site.
This feature provides visibility into your disaster recovery readiness by showing which
critical data states have been safely mirrored.

**Use Cases:**

- **Compliance & Auditing**: Verify that end-of-day or end-of-month snapshots are replicated
- **Application Consistency**: Ensure application-consistent snapshots reach the DR site
- **Migration Tracking**: Monitor progress when migrating data between sites
- **SLA Verification**: Confirm that critical snapshots meet replication SLAs

What is a Checkpoint?
=====================

A checkpoint is a marker you place on a snapshot to track its replication status. When you
checkpoint a snapshot, the system monitors whether that snapshot (and all previous snapshots)
have been successfully synchronized to the remote filesystem.

**Key Concepts:**

- **Automatic Status Tracking**: Checkpoints automatically detect if a snapshot has already been synced
- **Persistent**: Checkpoint information survives daemon restarts
- **Per-Directory**: Each mirrored directory has its own set of checkpoints

Checkpoint Lifecycle
--------------------

Every checkpoint goes through these states:

.. code-block:: text

    ┌─────────┐
    │ CREATED │  ← Checkpoint added, snapshot not yet synced
    └────┬────┘
         │
         ├──→ ┌──────────┐
         │    │ COMPLETE │  ← Snapshot successfully synced to remote
         │    └──────────┘
         │
         └──→ ┌────────┐
              │ FAILED │  ← Sync failed (will be retried automatically)
              └────────┘

- **created**: Waiting for synchronization or sync in progress
- **complete**: Successfully replicated to remote site
- **failed**: Sync encountered an error (check error message for details)

Prerequisites
=============

Before using checkpoints, ensure:

1. **CephFS Mirroring is Enabled**:

   .. code-block:: bash

       ceph fs snapshot mirror enable <fs_name>

2. **Remote Peer is Configured**:

   .. code-block:: bash

       ceph fs snapshot mirror peer_add <fs_name> <peer_spec>

3. **Directory is Being Mirrored**:

   .. code-block:: bash

       ceph fs snapshot mirror add <fs_name> <dir_path>

4. **Snapshots Exist**: You need snapshots to checkpoint

For detailed mirroring setup, see :doc:`cephfs-mirroring`.

Getting Started
===============

Basic Workflow
--------------

Here's a typical workflow for using checkpoints:

**Step 1: Create Snapshots**

.. code-block:: bash

    # Create snapshots in your mirrored directory
    mkdir /mnt/cephfs/data/.snap/daily_backup_2026_06_01
    mkdir /mnt/cephfs/data/.snap/daily_backup_2026_06_02
    mkdir /mnt/cephfs/data/.snap/daily_backup_2026_06_03

**Step 2: Add Checkpoints**

.. code-block:: bash

    # Mark important snapshots as checkpoints
    ceph fs snapshot mirror checkpoint add myfs /data daily_backup_2026_06_01
    ceph fs snapshot mirror checkpoint add myfs /data daily_backup_2026_06_03

**Step 3: Monitor Status**

.. code-block:: bash

    # Check checkpoint status
    ceph fs snapshot mirror checkpoint ls myfs /data --format json-pretty

**Example Output:**

.. code-block:: json

    {
      "dir_root": "/data",
      "checkpoints": [
        {
          "snap_id": 100,
          "snap_name": "daily_backup_2026_06_01",
          "status": "complete",
          "created_at": "2026-06-01T23:00:00.000000+0000",
          "updated_at": "2026-06-01T23:15:00.000000+0000"
        },
        {
          "snap_id": 102,
          "snap_name": "daily_backup_2026_06_03",
          "status": "created",
          "created_at": "2026-06-03T23:00:00.000000+0000",
          "updated_at": "2026-06-03T23:00:00.000000+0000"
        }
      ]
    }

Command Reference
=================

checkpoint add
--------------

Add a checkpoint to a specific snapshot.

**Syntax:**

.. code-block:: bash

    ceph fs snapshot mirror checkpoint add <fs_name> <dir_path> <snap_name>

**Parameters:**

- ``fs_name``: Name of the filesystem
- ``dir_path``: Path to the mirrored directory (e.g., ``/data``)
- ``snap_name``: Name of the snapshot to checkpoint

**Example:**

.. code-block:: bash

    ceph fs snapshot mirror checkpoint add myfs /data backup_snapshot_1

**Output:**

.. code-block:: json

    {
      "status": "success",
      "message": "checkpoint added for snapshot backup_snapshot_1",
      "dir_root": "/data",
      "snap_id": 123,
      "snap_name": "backup_snapshot_1",
      "checkpoint_status": "created"
    }

**Notes:**

- If the snapshot has already been synced, status will be ``complete`` immediately
- You cannot checkpoint the same snapshot twice
- The snapshot must exist in the directory

checkpoint now
--------------

Add a checkpoint to the most recent snapshot in a directory.

**Syntax:**

.. code-block:: bash

    ceph fs snapshot mirror checkpoint now <fs_name> <dir_path>

**Example:**

.. code-block:: bash

    ceph fs snapshot mirror checkpoint now myfs /data

**Output:**

.. code-block:: json

    {
      "status": "success",
      "message": "checkpoint created on latest snapshot",
      "dir_root": "/data",
      "snap_id": 125,
      "snap_name": "daily_backup_2026_06_03",
      "checkpoint_status": "created"
    }

**Use Case:**

Useful when you want to checkpoint the latest state without knowing the exact snapshot name.

checkpoint ls
-------------

List all checkpoints for a directory.

**Syntax:**

.. code-block:: bash

    ceph fs snapshot mirror checkpoint ls <fs_name> <dir_path> [--format <format>]

**Parameters:**

- ``format``: Output format (``json`` or ``json-pretty``), default is ``json``

**Example:**

.. code-block:: bash

    ceph fs snapshot mirror checkpoint ls myfs /data --format json-pretty

**Output Fields:**

- ``snap_id``: Internal snapshot ID
- ``snap_name``: Snapshot name
- ``status``: Current status (created/complete/failed)
- ``created_at``: When checkpoint was created
- ``updated_at``: Last status update time
- ``error_msg``: Error description (only present if status is failed)

checkpoint remove
-----------------

Remove a checkpoint from a specific snapshot.

**Syntax:**

.. code-block:: bash

    ceph fs snapshot mirror checkpoint remove <fs_name> <dir_path> <snap_name>

**Example:**

.. code-block:: bash

    # Remove checkpoint from a specific snapshot
    ceph fs snapshot mirror checkpoint remove myfs /data backup_snapshot_1

**Output:**

.. code-block:: json

    {
      "status": "success",
      "message": "checkpoint removed for snapshot backup_snapshot_1",
      "dir_root": "/data",
      "snap_name": "backup_snapshot_1"
    }

**Notes:**

- Removing a checkpoint does NOT delete the snapshot itself
- The snapshot and its data remain intact
- This operation only removes the checkpoint tracking metadata
- This operation cannot be undone
- To remove multiple checkpoints, run the command multiple times

Important Notes
===============

Snapshot Deletion Behavior
---------------------------

.. warning::

   **If a snapshot with a checkpoint is deleted, the checkpoint metadata is permanently lost.**

When you delete a snapshot that has a checkpoint:

1. **Before Sync Completes**: The checkpoint is lost and will never reach "complete" status
2. **After Sync Completes**: The checkpoint history is lost, but the data was already replicated

**Impact on Commands:**

- ``checkpoint ls``: Will not show the deleted snapshot's checkpoint
- ``checkpoint remove``: Will fail with "snapshot not found" error

**Best Practice:**

- Do not delete snapshots with active checkpoints until they reach "complete" status
- Use ``checkpoint ls`` to verify checkpoint status before deleting snapshots
- Consider removing checkpoints explicitly before deleting important snapshots

Snapshot Rename Behavior
-------------------------

.. note::

   **Checkpoints survive snapshot renames, but you must use the new snapshot name in commands.**

When you rename a snapshot that has a checkpoint:

1. **Checkpoint Persists**: The checkpoint metadata remains attached to the snapshot
2. **Name Updates Automatically**: ``checkpoint ls`` will show the new snapshot name
3. **Old Name No Longer Works**: You must use the new name for ``checkpoint remove``

**Example:**

.. code-block:: bash

    # Original snapshot with checkpoint
    ceph fs snapshot mirror checkpoint add myfs /data old_name

    # Rename the snapshot (using CephFS snapshot rename)
    # ... snapshot renamed from 'old_name' to 'new_name' ...

    # List checkpoints - shows new name
    ceph fs snapshot mirror checkpoint ls myfs /data
    # Output shows: "snap_name": "new_name"

    # Remove using NEW name (this works)
    ceph fs snapshot mirror checkpoint remove myfs /data new_name

    # Remove using OLD name (this fails)
    ceph fs snapshot mirror checkpoint remove myfs /data old_name
    # Error: snapshot 'old_name' not found

**Best Practice:**

- Always use ``checkpoint ls`` to verify current snapshot names before removing checkpoints
- Update any automation scripts if you rename snapshots

Automatic Retention
-------------------

The ``cephfs-mirror`` daemon automatically prunes old **complete** checkpoints so that
only the newest ones are retained per mirrored directory.

- Default retention: last **10** complete checkpoints (``cephfs_mirror_checkpoint_keep_count``)
- Pruning runs lazily when checkpoints are initialized (directory acquire, checkpoint
  add/now, or daemon restart) — not on a fixed timer
- Checkpoints in **created** or **failed** status are never pruned automatically
- Only checkpoint metadata is removed; the underlying snapshots are not deleted

Additional Resources
====================

- **CephFS Mirroring Guide**: :doc:`cephfs-mirroring`
- **Snapshot Documentation**: :doc:`/cephfs/snapshots`
- **Tracker Issue**: https://tracker.ceph.com/issues/73454