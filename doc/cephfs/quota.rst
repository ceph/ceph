CephFS Quotas
=============

CephFS allows quotas to be set on any directory in the file system.  The
quota can restrict the number of *bytes* or the number of *files*
stored beneath that point in the directory hierarchy.

Like most other things in CephFS, quotas are configured using virtual
extended attributes:

 * ``ceph.quota.max_files`` -- file limit
 * ``ceph.quota.max_bytes`` -- byte limit

If the extended attributes appear on a directory that means a quota is
configured there. If they are not present then no quota is set on that
directory (although one may still be configured on a parent directory).

To set a quota, set the extended attribute on a CephFS directory with a
value::

  setfattr -n ceph.quota.max_bytes -v 100000000 /some/dir     # 100 MB
  setfattr -n ceph.quota.max_files -v 10000 /some/dir         # 10,000 files

``ceph.quota.max_bytes`` can also be set using human-friendly units::

  setfattr -n ceph.quota.max_bytes -v 100K /some/dir          # 100 KiB
  setfattr -n ceph.quota.max_bytes -v 5Gi /some/dir           # 5 GiB

.. note:: Values will be strictly cast to IEC units even when SI units
   are input, e.g. 1K to 1024 bytes.

To view quota limit::

  $ getfattr -n ceph.quota.max_bytes /some/dir
  # file: dir1/
  ceph.quota.max_bytes="100000000"
  $
  $ getfattr -n ceph.quota.max_files /some/dir
  # file: dir1/
  ceph.quota.max_files="10000"

.. note:: Running ``getfattr /some/dir -d -m -`` for a CephFS directory will
   print none of the CephFS extended attributes. This is because the CephFS
   kernel and FUSE clients hide this information from the ``listxattr(2)``
   system call. Instead, a specific CephFS extended attribute can be viewed by
   running ``getfattr /some/dir -n ceph.<some-xattr>``.

To remove a quota, set the value of extended attribute to ``0``::

  $ setfattr -n ceph.quota.max_bytes -v 0 /some/dir
  $ getfattr /some/dir -n ceph.quota.max_bytes
  dir1/: ceph.quota.max_bytes: No such attribute
  $
  $ setfattr -n ceph.quota.max_files -v 0 /some/dir
  $ getfattr dir1/ -n ceph.quota.max_files
  dir1/: ceph.quota.max_files: No such attribute

Space Usage Reporting and CephFS Quotas
---------------------------------------
When the root directory of the CephFS mount has quota set on it, the available
space on the CephFS reported by space usage report tools (like ``df``) is
based on quota limit. That is, ``available space = quota limit - used space``
instead of ``available space = total space - used space``.

This behaviour can be disabled by setting following option in client section
of ``ceph.conf``::

    client quota df = false

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

#. *Quotas are implemented in the kernel client 4.17 and higher.*
   Quotas are supported by the userspace client (libcephfs, ceph-fuse).
   Linux kernel clients >= 4.17 support CephFS quotas but only on
   mimic+ clusters.  Kernel clients (even recent versions) will fail
   to handle quotas on older clusters, even if they may be able to set
   the quotas extended attributes.

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

   In case of a kernel client, it needs to have access to the parent
   of the directory inode on which quotas are configured in order to
   enforce them. If quota is configured on a directory path
   (e.g., ``/home/volumes/group``), the kclient needs to have access
   to the parent (e.g., ``/home/volumes``).

   An example command to create such an user is as below::

     $ ceph auth get-or-create client.guest mds 'allow r path=/home/volumes, allow rw path=/home/volumes/group' mgr 'allow rw' osd 'allow rw tag cephfs metadata=*' mon 'allow r'

   See also: https://tracker.ceph.com/issues/55090

#. *Snapshot file data which has since been deleted or changed does not count
   towards the quota.* See also: http://tracker.ceph.com/issues/24284
