====================
Contributing to Ceph
====================
----------------------
A Guide for Developers
----------------------

:Author: Loic Dachary
:Author: Nathan Cutler
:License: Creative Commons Attribution-ShareAlike (CC BY-SA)

.. note:: The old (pre-2016) developer documentation has been moved to :doc:`/dev/index-old`.

.. contents::
   :depth: 4

Introduction
============

This guide assumes that you are already familiar with Ceph (the distributed
object store and file system designed to provide excellent performance,
reliability and scalability). If not, please refer to the `project website`_ 
and especially the `publications list`_.

.. _`project website`: http://ceph.com 
.. _`publications list`: https://ceph.com/resources/publications/

Bare essentials
===============

Leads
-----

* Project lead: Sage Weil
* RADOS lead: Samuel Just
* RGW lead: Yehuda Sadeh
* RBD lead: Josh Durgin
* CephFS lead: Gregory Farnum
* Build/Ops lead: Ken Dreyer

The Ceph-specific acronyms are explained under `High-level structure`_, below.

History
-------

The Ceph project grew out of the petabyte-scale storage research at the Storage
Systems Research Center at the University of California, Santa Cruz. The
project was funded primarily by a grant from the Lawrence Livermore, Sandia,
and Los Alamos National Laboratories.

Sage Weil, the project lead, published his Ph. D. thesis entitled "Ceph:
Reliable, Scalable, and High-Performance Distributed Storage" in 2007.

DreamHost https://www.dreamhost.com . . .

Inktank . . .

On April 30, 2014 Red Hat announced its takeover of Inktank:
http://www.redhat.com/en/about/press-releases/red-hat-acquire-inktank-provider-ceph

Licensing
---------

Ceph is free software.

Unless stated otherwise, the Ceph source code is distributed under the terms of
the LGPL2.1. For full details, see the file COPYING in the top-level directory
of the source-code distribution:
https://github.com/ceph/ceph/blob/master/COPYING

Source code repositories
------------------------

The source code of Ceph lives on GitHub in a number of repositories below https://github.com/ceph

To make a meaningful contribution to the project as a developer, a working
knowledge of git_ is essential.

.. _git: https://git-scm.com/documentation

Mailing list
------------

The official development email list is ``ceph-devel@vger.kernel.org``.  Subscribe by sending
a message to ``majordomo@vger.kernel.org`` with the line::

 subscribe ceph-devel

in the body of the message.

There are also `other Ceph-related mailing lists`_. 

.. _`other Ceph-related mailing lists`: https://ceph.com/resources/mailing-list-irc/

IRC
---

See https://ceph.com/resources/mailing-list-irc/


High-level structure
====================

Like any other large software project, Ceph consists of a number of components.
Viewed from a very high level, the components are:

* **RADOS**

  RADOS stands for "Reliable, Autonomic Distributed Object Store". In a Ceph
  cluster, all data are stored in objects, and RADOS is the component responsible
  for that. 

  RADOS itself can be further broken down into Monitors, Object Storage Daemons
  (OSDs), and clients.

* **RGW**

  RGW stands for RADOS Gateway. Using the embedded HTTP server civetweb_, RGW
  provides a REST interface to RADOS objects.

  .. _civetweb: https://github.com/civetweb/civetweb

* **RBD**

  RBD stands for RADOS Block Device. It enables a Ceph cluster to store disk
  images, and includes in-kernel code enabling RBD images to be mounted.

* **CephFS**

  CephFS is a distributed file system that enables a Ceph cluster to be used as a NAS.

  File system metadata is managed by Meta Data Server (MDS) daemons.

Building
========

Building from source
--------------------

See http://docs.ceph.com/docs/master/install/build-ceph/

Testing
=======

You can start a development mode Ceph cluster, after compiling the source, with::

	cd src
	install -d -m0755 out dev/osd0
	./vstart.sh -n -x -l
	# check that it's there
	./ceph health

WIP
===

Monitors
--------

MON stands for "Monitor". Each Ceph cluster has a number of monitor processes.
See **man ceph-mon** or http://docs.ceph.com/docs/master/man/8/ceph-mon/ for
some basic information. The monitor source code lives under **src/mon** in the
tree: https://github.com/ceph/ceph/tree/master/src/mon

OSDs
----

OSD stands for Object Storage Daemon. Typically, there is one of these for each
disk in the cluster. See **man ceph-osd** or
http://docs.ceph.com/docs/master/man/8/ceph-osd/ for basic information. The OSD
source code can be found here: https://github.com/ceph/ceph/tree/master/src/osd

librados
--------

RADOS also includes an API for writing your own clients that can communicate
directly with a Ceph cluster's underlying object store. The API includes
bindings for popular scripting languages such as Python. For more information, 
see the documents under https://github.com/ceph/ceph/tree/master/doc/rados/api

Build/Ops
---------

Ceph supports a number of major Linux distributions and provides packaging for them.


