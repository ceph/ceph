===
NFS
===

CephFS namespaces can be exported over NFS protocol using the
`NFS-Ganesha NFS server <https://github.com/nfs-ganesha/nfs-ganesha/wiki>`_.

Requirements
============

-  Ceph filesystem (preferably latest stable luminous or higher versions)
-  In the NFS server host machine, 'libcephfs2' (preferably latest stable
   luminous or higher), 'nfs-ganesha' and 'nfs-ganesha-ceph' packages (latest
   ganesha v2.5 stable or higher versions)
-  NFS-Ganesha server host connected to the Ceph public network

Configuring NFS-Ganesha to export CephFS
========================================

NFS-Ganesha provides a File System Abstraction Layer (FSAL) to plug in different
storage backends. `FSAL_CEPH <https://github.com/nfs-ganesha/nfs-ganesha/tree/next/src/FSAL/FSAL_CEPH>`_
is the plugin FSAL for CephFS. For each NFS-Ganesha export, FSAL_CEPH uses a
libcephfs client, user-space CephFS client, to mount the CephFS path that
NFS-Ganesha exports.

Setting up NFS-Ganesha with CephFS, involves setting up NFS-Ganesha's
configuration file, and also setting up a Ceph configuration file and cephx
access credentials for the Ceph clients created by NFS-Ganesha to access
CephFS.

NFS-Ganesha configuration
-------------------------

A sample ganesha.conf configured with FSAL_CEPH can be found here,
`<https://github.com/nfs-ganesha/nfs-ganesha/blob/next/src/config_samples/ceph.conf>`_.
It is suitable for a standalone NFS-Ganesha server, or an active/passive
configuration of NFS-Ganesha servers managed by some sort of clustering
software (e.g., Pacemaker). Important details about the options are
added as comments in the sample conf. There are options to do the following:

- minimize Ganesha caching wherever possible since the libcephfs clients
  (of FSAL_CEPH) also cache aggressively

- read from Ganesha config files stored in RADOS objects

- store client recovery data in RADOS OMAP key-value interface

- mandate NFSv4.1+ access

- enable read delegations (need at least v13.0.1 'libcephfs2' package
  and v2.6.0 stable 'nfs-ganesha' and 'nfs-ganesha-ceph' packages)

Configuration for libcephfs clients
-----------------------------------

Required ceph.conf for libcephfs clients includes:

* a [client] section with ``mon_host`` option set to let the clients connect
  to the Ceph cluster's monitors, usually generated via ``ceph config generate-minimal-conf``, e.g., ::

    [global]
            mon host = [v2:192.168.1.7:3300,v1:192.168.1.7:6789], [v2:192.168.1.8:3300,v1:192.168.1.8:6789], [v2:192.168.1.9:3300,v1:192.168.1.9:6789]

Mount using NFSv4 clients
=========================

It is preferred to mount the NFS-Ganesha exports using NFSv4.1+ protocols
to get the benefit of sessions.

Conventions for mounting NFS resources are platform-specific. The
following conventions work on Linux and some Unix platforms:

From the command line::

  mount -t nfs -o nfsvers=4.1,proto=tcp <ganesha-host-name>:<ganesha-pseudo-path> <mount-point>

Current limitations
===================

- Per running ganesha daemon, FSAL_CEPH can only export one Ceph filesystem
  although multiple directories in a Ceph filesystem may be exported.
