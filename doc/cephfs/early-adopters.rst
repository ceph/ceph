
CephFS for early adopters
=========================

This pages provides guidance for early adoption of CephFS by users
with an appetite for adventure.  While work is ongoing to build the
scrubbing and disaster recovery tools needed to run CephFS in demanding
production environments, it is already useful for community members to
try CephFS and provide bug reports and feedback.

Setup instructions
==================

Please see the instructions at :doc:`/cephfs/index`.

Most stable configuration
=========================

For the best chance of a happy healthy filesystem, use a **single active MDS** 
and **do not use snapshots**.  Both of these are the default:

* Snapshots are disabled by default, unless they are enabled explicitly by
  an administrator using the ``allow_new_snaps`` setting.
* Ceph will use a single active MDS unless an administrator explicitly sets
  ``max_mds`` to a value greater than 1.  Note that creating additional
  MDS daemons (e.g. with ``ceph-deploy mds create``) is okay, as these will
  by default simply become standbys.  It is also fairly safe to enable
  standby-replay mode.

Which client?
=============

The fuse client is the easiest way to get up to date code, while
the kernel client will often give better performance.

The clients do not always provide equivalent functionality, for example
the fuse client supports client-enforced quotas while the kernel client
does not.

When encountering bugs or performance issues, it is often instructive to
try using the other client, in order to find out whether the bug was
client-specific or not (and then to let the developers know).

Reporting issues
================

If you have identified a specific issue, please report it with as much
information as possible.  Especially important information:

* Ceph versions installed on client and server
* Whether you are using the kernel or fuse client
* If you are using the kernel client, what kernel version?
* How many clients are in play, doing what kind of workload?
* If a system is 'stuck', is that affecting all clients or just one?
* Any ceph health messages
* Any backtraces in the ceph logs from crashes

If you are satisfied that you have found a bug, please file it on
http://tracker.ceph.com.  For more general queries please write
to the ceph-users mailing list.

