.. _cephfs-nfs:

===
NFS
===

CephFS namespaces can be exported over NFS protocol using the `NFS-Ganesha NFS
server`_.  This document provides information on configuring NFS-Ganesha
clusters manually.  The simplest and preferred way of managing NFS-Ganesha
clusters and CephFS exports is using ``ceph nfs ...`` commands. See
:doc:`/mgr/nfs` for more details. As the deployment is done using cephadm or
rook.

Requirements
============

-  Ceph file system
-  ``libcephfs2``, ``nfs-ganesha`` and ``nfs-ganesha-ceph`` packages on NFS
   server host machine.
-  NFS-Ganesha server host connected to the Ceph public network

.. note::
   It is recommended to use 3.5 or later stable version of NFS-Ganesha
   packages with pacific (16.2.x) or later stable version of Ceph packages.

Configuring NFS-Ganesha to export CephFS
========================================

NFS-Ganesha provides a File System Abstraction Layer (FSAL) to plug in
different storage backends. FSAL_CEPH_ is the plugin FSAL for CephFS. For
each NFS-Ganesha export, FSAL_CEPH_ uses a libcephfs client to mount the
CephFS path that NFS-Ganesha exports.

Setting up NFS-Ganesha with CephFS, involves setting up NFS-Ganesha's and
Ceph's configuration file and CephX access credentials for the Ceph clients
created by NFS-Ganesha to access CephFS.

NFS-Ganesha configuration
-------------------------

Here's a `sample ganesha.conf`_ configured with FSAL_CEPH_. It is suitable
for a standalone NFS-Ganesha server, or an active/passive configuration of
NFS-Ganesha servers, to be managed by some sort of clustering software
(e.g., Pacemaker). Important details about the options are added as comments
in the sample conf. There are options to do the following:

- minimize Ganesha caching wherever possible since the libcephfs clients
  (of FSAL_CEPH_) also cache aggressively

- read from Ganesha config files stored in RADOS objects

- store client recovery data in RADOS OMAP key-value interface

- mandate NFSv4.1+ access

- enable read delegations (need at least v13.0.1 ``libcephfs2`` package
  and v2.6.0 stable ``nfs-ganesha`` and ``nfs-ganesha-ceph`` packages)

.. important::

   Under certain conditions, NFS access using the CephFS FSAL fails. This
   causes an error to be thrown that reads "Input/output error". Under these
   circumstances, the application metadata must be set for the CephFS metadata
   and data pools. Do this by running the following command:

   .. prompt:: bash $

      ceph osd pool application set <cephfs_metadata_pool> cephfs <cephfs_data_pool> cephfs


Configuration for libcephfs clients
-----------------------------------

``ceph.conf`` for libcephfs clients includes a ``[client]`` section with
``mon_host`` option set to let the clients connect to the Ceph cluster's
monitors, usually generated via ``ceph config generate-minimal-conf``.
For example::

    [client]
            mon host = [v2:192.168.1.7:3300,v1:192.168.1.7:6789], [v2:192.168.1.8:3300,v1:192.168.1.8:6789], [v2:192.168.1.9:3300,v1:192.168.1.9:6789]

Mount using NFSv4 clients
=========================

It is preferred to mount the NFS-Ganesha exports using NFSv4.1+ protocols
to get the benefit of sessions.

Conventions for mounting NFS resources are platform-specific. The
following conventions work on Linux and some Unix platforms:

.. code:: bash

    mount -t nfs -o nfsvers=4.1,proto=tcp <ganesha-host-name>:<ganesha-pseudo-path> <mount-point>


.. _FSAL_CEPH: https://github.com/nfs-ganesha/nfs-ganesha/tree/next/src/FSAL/FSAL_CEPH
.. _NFS-Ganesha NFS server: https://github.com/nfs-ganesha/nfs-ganesha/wiki
.. _sample ganesha.conf: https://github.com/nfs-ganesha/nfs-ganesha/blob/next/src/config_samples/ceph.conf
