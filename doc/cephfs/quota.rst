CephFS Quotas
=============

CephFS allows quotas to be set on any directory in the system.  The
quota can restrict the number of *bytes* or the number of *files*
stored beneath that point in the directory hierarchy.

Limitations
-----------

#. *Quotas are cooperative and non-adversarial.* CephFS quotas rely on
   the cooperation of the client who is mounting the file system to
   stop writers when a limit is reached.  A modified or adversarial
   client cannot be prevented from writing as much data as it needs.
   Quotas should not be relied on to prevent filling the system in
   environments where the clients are fully untrusted.

#. *Quotas are imprecise.* Processes that are writing to the file
   system will be stopped a short time after the quota limit is
   reached.  They will inevitably be allowed to write some amount of
   data over the configured limit.  How far over the quota they are
   able to go depends primarily on the amount of time, not the amount
   of data.  Generally speaking writers will be stopped within 10s of
   seconds of crossing the configured limit.

#. *Quotas are not yet implemented in the kernel client.* Quotas are
   supported by the userspace client (libcephfs, ceph-fuse) but are
   not yet implemented in the Linux kernel client.

#. *Quotas must be configured carefully when used with path-based
   mount restrictions.* The client needs to have access to the
   directory inode on which quotas are configured in order to enforce
   them.  If the client has restricted access to a specific path
   (e.g., ``/home/user``) based on the MDS capability, and a quota is
   configured on an ancestor directory they do not have access to
   (e.g., ``/home``), the client will not enforce it.  When using
   path-based access restrictions be sure to configure the quota on
   the directory the client is restricted too (e.g., ``/home/user``)
   or something nested beneath it.

Configuration
-------------

Like most other things in CephFS, quotas are configured using virtual
extended attributes:

 * ``ceph.quota.max_files`` -- file limit
 * ``ceph.quota.max_bytes`` -- byte limit

If the attributes appear on a directory inode that means a quota is
configured there.  If they are not present then no quota is set on
that directory (although one may still be configured on a parent directory).

To set a quota::

  setfattr -n ceph.quota.max_bytes -v 100000000 /some/dir     # 100 MB
  setfattr -n ceph.quota.max_files -v 10000 /some/dir         # 10,000 files

To view quota settings::

  getfattr -n ceph.quota.max_bytes /some/dir
  getfattr -n ceph.quota.max_files /some/dir

Note that if the value of the extended attribute is ``0`` that means
the quota is not set.

To remove a quota::

  setfattr -n ceph.quota.max_bytes -v 0 /some/dir
  setfattr -n ceph.quota.max_files -v 0 /some/dir
