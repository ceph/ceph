.. _mds-scrub:

======================
Ceph File System Scrub
======================

CephFS provides the cluster admin (operator) to check consistency of a file system
via a set of scrub commands. Scrub can be classified into two parts:

#. Forward Scrub: In which the scrub operation starts at the root of the file system
   (or a sub directory) and looks at everything that can be touched in the hierarchy
   to ensure consistency.

#. Backward Scrub: In which the scrub operation looks at every RADOS object in the
   file system pools and maps it back to the file system hierarchy.

This document details commands to initiate and control forward scrub (referred as
scrub thereafter).

.. warning::

   CephFS forward scrubs are started and manipulated on rank 0. All scrub
   commands must be directed at rank 0.

Initiate File System Scrub
==========================

To start a scrub operation for a directory tree use the following command::

   ceph tell mds.<fsname>:0 scrub start <path> [scrubopts] [tag]

where ``scrubopts`` is a comma delimited list of ``recursive``, ``force``, or
``repair`` and ``tag`` is an optional custom string tag (the default is a generated
UUID). An example command is::

   ceph tell mds.cephfs:0 scrub start / recursive
   {
       "return_code": 0,
       "scrub_tag": "6f0d204c-6cfd-4300-9e02-73f382fd23c1",
       "mode": "asynchronous"
   }

Recursive scrub is asynchronous (as hinted by `mode` in the output above).
Asynchronous scrubs must be polled using ``scrub status`` to determine the
status.

The scrub tag is used to differentiate scrubs and also to mark each inode's
first data object in the default data pool (where the backtrace information is
stored) with a ``scrub_tag`` extended attribute with the value of the tag. You
can verify an inode was scrubbed by looking at the extended attribute using the
RADOS utilities.

Scrubs work for multiple active MDS (multiple ranks). The scrub is managed by
rank 0 and distributed across MDS as appropriate.


Monitor (ongoing) File System Scrubs
====================================

Status of ongoing scrubs can be monitored and polled using in `scrub status`
command. This commands lists out ongoing scrubs (identified by the tag) along
with the path and options used to initiate the scrub::

   ceph tell mds.cephfs:0 scrub status
   {
       "status": "scrub active (85 inodes in the stack)",
       "scrubs": {
           "6f0d204c-6cfd-4300-9e02-73f382fd23c1": {
               "path": "/",
               "options": "recursive"
           }
       }
   }

`status` shows the number of inodes that are scheduled to be scrubbed at any point in time,
hence, can change on subsequent `scrub status` invocations. Also, a high level summary of
scrub operation (which includes the operation state and paths on which scrub is triggered)
gets displayed in `ceph status`::

   ceph status
   [...]

   task status:
     scrub status:
         mds.0: active [paths:/]

   [...]

A scrub is complete when it no longer shows up in this list (although that may
change in future releases). Any damage will be reported via cluster health warnings.

Control (ongoing) File System Scrubs
====================================

- Pause: Pausing ongoing scrub operations results in no new or pending inodes being
  scrubbed after in-flight RADOS ops (for the inodes that are currently being scrubbed)
  finish::

   ceph tell mds.cephfs:0 scrub pause
   {
       "return_code": 0
   }

  The ``scrub status`` after pausing reflects the paused state. At this point,
  initiating new scrub operations (via ``scrub start``) would just queue the
  inode for scrub::

   ceph tell mds.cephfs:0 scrub status
   {
       "status": "PAUSED (66 inodes in the stack)",
       "scrubs": {
           "6f0d204c-6cfd-4300-9e02-73f382fd23c1": {
               "path": "/",
               "options": "recursive"
           }
       }
   }

- Resume: Resuming kick starts a paused scrub operation::

   ceph tell mds.cephfs:0 scrub resume
   {
       "return_code": 0
   }

- Abort: Aborting ongoing scrub operations removes pending inodes from the scrub
  queue (thereby aborting the scrub) after in-flight RADOS ops (for the inodes that
  are currently being scrubbed) finish::

   ceph tell mds.cephfs:0 scrub abort
   {
       "return_code": 0
   }

Damages
=======

The types of damage that can be reported and repaired by File System Scrub are:

* DENTRY : Inode's dentry is missing.

* DIR_FRAG : Inode's directory fragment(s) is missing.

* BACKTRACE : Inode's backtrace in the data pool is corrupted.

Evaluate strays using recursive scrub
=====================================

- In order to evaluate strays i.e. purge stray directories in ``~mdsdir`` use the following command::

    ceph tell mds.<fsname>:0 scrub start ~mdsdir recursive

- ``~mdsdir`` is not enqueued by default when scrubbing at the CephFS root. In order to perform stray evaluation
  at root, run scrub with flags ``scrub_mdsdir`` and ``recursive``::

    ceph tell mds.<fsname>:0 scrub start / recursive,scrub_mdsdir
