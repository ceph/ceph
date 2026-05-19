================
 Ceph Internals
================

.. note:: For information on how to use Ceph as a library (from your own
   software), see :doc:`/api/index`.

This section contains documentation about the internal architecture and
implementation details of Ceph components. It is organized by major subsystem
to help developers navigate the codebase.

Getting Started with Development
================================

.. note:: You may also be interested in the :doc:`/dev/developer_guide/index` documentation.

Community Mailing Lists
-----------------------

The ``dev@ceph.io`` list is for discussion about the development of Ceph,
its interoperability with other technology, and the operations of the
project itself.  Subscribe by sending a message to ``dev-join@ceph.io``
with the word `subscribe` in the subject.

Alternatively you can visit https://lists.ceph.io and register.

The ceph-devel@vger.kernel.org list is for discussion
and patch review for the Linux kernel Ceph client component.
Subscribe by sending a message to ``majordomo@vger.kernel.org`` with the line::

 subscribe ceph-devel

in the body of the message.

Starting a Development-mode Ceph Cluster
----------------------------------------

Compile the source and then run the following commands to start a
development-mode Ceph cluster::

	cd build
	OSD=3 MON=3 MGR=3 ../src/vstart.sh -n -x
	# check that it's there
	bin/ceph health

Development Processes
---------------------

.. toctree::
   :maxdepth: 1

   quick_guide
   dev_cluster_deployment
   development-workflow
   testing
   sepia
   continuous-integration
   release-process
   release-checklists

Documentation Processes
-----------------------

.. toctree::
   :maxdepth: 1

   documenting
   generatedocs

Core Architecture
=================

.. toctree::
   :maxdepth: 1

   logs
   logging
   health-reports
   config
   config-key
   context
   encoding
   corpus
   network-encoding
   mempool_accounting
   iana
   cxx

RADOS and Object Store
======================

.. toctree::
   :maxdepth: 1

   object-store
   bluestore
   balancer-design
   deduplication
   zoned-storage
   crush-msr
   blkin
   libs
   osd-class-path

OSD
---

.. toctree::
   :maxdepth: 2

   osd_internals/index
   peering
   placement-group
   erasure-coded-pool
   pool-migration-design
   versions
   crimson/index

Monitor
-------

.. toctree::
   :maxdepth: 1

   mon-bootstrap
   mon-elections
   mon-on-disk-formats
   mon-osdmap-prune

CephFS and MDS
==============

.. toctree::
   :maxdepth: 2

   mds_internals/index
   cephfs-snapshots
   cephfs-mirroring
   cephfs-reclaim
   cephfs-fscrypt
   file-striping
   delayed-delete
   kclient
   vstart-ganesha
   libcephfs_proxy

RADOS Gateway (RGW)
===================

.. toctree::
   :maxdepth: 2

   radosgw/index

RBD
===

.. toctree::
   :maxdepth: 1

   rbd-diff
   rbd-export
   rbd-layering

Messaging and Networking
========================

.. toctree::
   :maxdepth: 1

   messenger
   msgr2
   network-protocol
   rados-client-protocol
   dpdk
   wireshark

Authentication and Security
===========================

.. toctree::
   :maxdepth: 1

   cephx
   cephx_protocol
   session_authentication
   ceph_krb_auth

Management and Orchestration
============================

.. toctree::
   :maxdepth: 2

   cephadm/index
   ceph-volume/index

.. note:: See :doc:`/dev/developer_guide/dash-devel` for dashboard documentation.

Performance
===========

.. toctree::
   :maxdepth: 1

   perf
   perf_counters
   perf_histograms
   cpu-profiler
   cputrace

Platform-Specific
=================

.. toctree::
   :maxdepth: 1

   freebsd
   macos
   kubernetes

