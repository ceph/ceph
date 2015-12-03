============================================
Contributing to Ceph: A Guide for Developers
============================================

:Author: Loic Dachary
:Author: Nathan Cutler
:License: Creative Commons Attribution-ShareAlike (CC BY-SA)

.. note:: The old (pre-2016) developer documentation has been moved to :doc:`/dev/index-old`.

.. contents::
   :depth: 3

Introduction
============

This guide has two aims. First, it should lower the barrier to entry for
software developers who wish to get involved in the Ceph project. Second,
it should serve as a reference for Ceph developers.

We assume that readers are already familiar with Ceph (the distributed
object store and file system designed to provide excellent performance,
reliability and scalability). If not, please refer to the `project website`_ 
and especially the `publications list`_.

.. _`project website`: http://ceph.com 
.. _`publications list`: https://ceph.com/resources/publications/

Since this document is to be consumed by developers, who are assumed to
have Internet access, topics covered elsewhere on the web are treated by
linking. If you notice that a link is broken or if you know of a better
link, `open a pull request`.

The bare essentials
===================

This chapter presents essential information that every Ceph developer needs
to know.

Leads
-----

The Ceph project is led by Sage Weil. In addition, each major project
component has its own lead. The following table shows all the leads and
their nicks on `GitHub`_:

.. _github: https://github.com/ceph/ceph

========= =============== =============
Scope     Lead            GitHub nick
========= =============== =============
Ceph      Sage Weil       liewegas
RADOS     Samuel Just     athanatos
RGW       Yehuda Sadeh    yehudasa
RBD       Josh Durgin     jdurgin
CephFS    Gregory Farnum  gregsfortytwo
Build/Ops Ken Dreyer      ktdreyer
========= =============== =============

The Ceph-specific acronyms in the table are explained under `High-level
structure`_, below.

History
-------

See the `History chapter of the Wikipedia article`_.

.. _`History chapter of the Wikipedia article`: https://en.wikipedia.org/wiki/Ceph_%28software%29#History

Licensing
---------

Ceph is free software.

Unless stated otherwise, the Ceph source code is distributed under the terms of
the LGPL2.1. For full details, see `the file COPYING in the top-level
directory of the source-code tree`_.

.. _`the file COPYING in the top-level directory of the source-code tree`: 
  https://github.com/ceph/ceph/blob/master/COPYING

Source code repositories
------------------------

The source code of Ceph lives on GitHub in a number of repositories below https://github.com/ceph

To make a meaningful contribution to the project as a developer, a working
knowledge of git_ is essential.

.. _git: https://git-scm.com/documentation

Mailing list
------------

Ceph development email discussions take place on
``ceph-devel@vger.kernel.org``.  Subscribe by sending a message to
``majordomo@vger.kernel.org`` with the line::

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

RADOS
-----

RADOS stands for "Reliable, Autonomic Distributed Object Store". In a Ceph
cluster, all data are stored in objects, and RADOS is the component responsible
for that. 

RADOS itself can be further broken down into Monitors, Object Storage Daemons
(OSDs), and clients (librados). Monitors and OSDs are introduced at
:doc:`start/intro`. The client library is explained at :doc:`rados/api`.

RGW
---

RGW stands for RADOS Gateway. Using the embedded HTTP server civetweb_, RGW
provides a REST interface to RADOS objects.

.. _civetweb: https://github.com/civetweb/civetweb

A more thorough introduction to RGW can be found at :doc:`radosgw`.

RBD
---

RBD stands for RADOS Block Device. It enables a Ceph cluster to store disk
images, and includes in-kernel code enabling RBD images to be mounted.

To delve further into RBD, see :doc:`rbd/rbd`.

CephFS
------

CephFS is a distributed file system that enables a Ceph cluster to be used as a NAS.

File system metadata is managed by Meta Data Server (MDS) daemons. The Ceph
file system is explained in more detail at :doc:`cephfs`.

Build/Ops
---------

Ceph is regularly built and packaged for a number of major Linux
distributions. At the time of this writing, these included Debian, Ubuntu,
CentOS, openSUSE, and Fedora.

Building
========

Building from source
--------------------

See instructions at :doc:`install/build-ceph`.

Testing
=======

You can start a development mode Ceph cluster, after compiling the source, with::

	cd src
	install -d -m0755 out dev/osd0
	./vstart.sh -n -x -l
	# check that it's there
	./ceph health

Issue tracker
=============

The Ceph project has its own issue tracker, `http://tracker.ceph.com`_,
powered by `Redmine`_.

.. _`http://tracker.ceph.com`: http://tracker.ceph.com
.. _Redmine: http://www.redmine.org

The tracker has a Ceph project with a number of subprojects loosely
corresponding to the project components listed in `High-level overview`.

Mere `registration`_ automatically grants tracker permissions sufficient to
open new issues and comment on existing ones.

.. _registration: http://tracker.ceph.com/account/register

To report a bug, `jump to the Ceph project`_ and click on `New issue`_

.. _`jump to the Ceph project`: http://tracker.ceph.com/projects/ceph
.. _`New issue`: http://tracker.ceph.com/projects/ceph/issues/new

If you start working on a bug, let the other developers know by adding an
update to the issue.

If your tracker permissions have been escalated, you can change the issue
status as appropriate:

================ ===========================================
Status           Meaning
================ ===========================================
New              Initial status
In Progress      Somebody is working on it
Need Review      Pull request is open with a fix
Pending Backport Fix has been merged, backport(s) pending
Resolved         Fix and backports (if any) have been merged
================ ===========================================

There are other statuses, but these are the most common.

If you do not have permission to change the status yourself, don't worry:
someone will probably change it for you, even without asking. You can ask
on IRC for another developer to change the status for you.

Bugfixing
=========

Without bugs, there would be no software, and without software, there would
be no software developers. This chapter explains the Ceph-specific aspects
of the project's bugfixing workflows.

A good understanding of the `Issue tracker`_ chapter is necessary before
you attempt to fix any bugs.

