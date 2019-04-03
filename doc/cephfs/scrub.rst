.. _mds-scrub:

=====================
Ceph Filesystem Scrub
=====================

CephFS provides the cluster admin (operator) to check consistency of a filesystem
via a set of scrub commands. Scrub can be classified into two parts:

#. Forward Scrub: In which the scrub operation starts at the root of the filesystem
   (or a sub directory) and looks at everything that can be touched in the hierarchy
   to ensure consistency.

#. Backward Scrub: In which the scrub operation looks at every RADOS object in the
   filesystem pools and maps it back to the filesystem hierarchy.

This document details commands to initiate and control forward scrub (referred as
scrub thereafter).

Initiate Filesystem Scrub
=========================

To start a scrub operation for a directory tree use the following command

::

   ceph tell mds.a scrub start / recursive
   {
       "return_code": 0,
       "scrub_tag": "6f0d204c-6cfd-4300-9e02-73f382fd23c1",
       "mode": "asynchronous"
   }

Recursive scrub is asynchronous (as hinted by `mode` in the output above). Scrub tag is
a random string that can used to monitor the progress of the scrub operation (explained
further in this document).

Custom tag can also be specified when initiating the scrub operation. Custom tags get
persisted in the metadata object for every inode in the filesystem tree that is being
scrubbed.

::

   ceph tell mds.a scrub start /a/b/c recursive tag0
   {
       "return_code": 0,
       "scrub_tag": "tag0",
       "mode": "asynchronous"
   }


Monitor (ongoing) Filesystem Scrubs
===================================

Status of ongoing scrubs can be monitored using in `scrub status` command. This commands
lists out ongoing scrubs (identified by the tag) along with the path and options used to
initiate the scrub.

::

   ceph tell mds.a scrub status
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
hence, can change on subsequent `scrub status` invocations.


Control (ongoing) Filesystem Scrubs
===================================

- Pause: Pausing ongoing scrub operations results in no new or pending inodes being
  scrubbed after in-flight RADOS ops (for the inodes that are currently being scrubbed)
  finish.

::

   ceph tell mds.a scrub pause
   {
       "return_code": 0
   }

`scrub status` after pausing reflects the paused state. At this point, initiating new scrub
operations (via `scrub start`) would just queue the inode for scrub.

::

   ceph tell mds.a scrub status
   {
       "status": "PAUSED (66 inodes in the stack)",
       "scrubs": {
           "6f0d204c-6cfd-4300-9e02-73f382fd23c1": {
               "path": "/",
               "options": "recursive"
           }
       }
   }

- Resume: Resuming kick starts a paused scrub operation.

::

   ceph tell mds.a. scrub resume
   {
       "return_code": 0
   }

- Abort: Aborting ongoing scrub operations removes pending inodes from the scrub
  queue (thereby aborting the scrub) after in-flight RADOS ops (for the inodes that
  are currently being scrubbed) finish.

::

   ceph tell mds.a. scrub abort
   {
       "return_code": 0
   }
