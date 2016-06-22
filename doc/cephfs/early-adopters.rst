
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

Which kernel version?
---------------------

Because the kernel client is distributed as part of the linux kernel (not
as part of packaged ceph releases),
you will need to consider which kernel version to use on your client nodes.
Older kernels are known to include buggy ceph clients, and may not support
features that more recent Ceph clusters support.

Remember that the "latest" kernel in a stable linux distribution is likely
to be years behind the latest upstream linux kernel where Ceph development
takes place (including bug fixes).

As a rough guide, as of Ceph 10.x (Jewel), you should be using a least a
4.x kernel.  If you absolutely have to use an older kernel, you should use
the fuse client instead of the kernel client.

This advice does not apply if you are using a linux distribution that
includes CephFS support, as in this case the distributor will be responsible
for backporting fixes to their stable kernel: check with your vendor.

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

