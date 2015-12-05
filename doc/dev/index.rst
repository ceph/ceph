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
have Internet access, topics covered elsewhere, either within the Ceph
documentation or elsewhere on the web, are treated by linking. If you
notice that a link is broken or if you know of a better link, please
`report it as a bug`_.

.. _`report it as a bug`: http://tracker.ceph.com/projects/ceph/issues/new

Essentials (tl;dr)
==================

This chapter presents essential information that every Ceph developer needs
to know.

Leads
-----

The Ceph project is led by Sage Weil. In addition, each major project
component has its own lead. The following table shows all the leads and
their nicks on `GitHub`_:

.. _github: https://github.com/

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

The Ceph-specific acronyms in the table are explained under
`Architecture`_, below.

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

The source code of Ceph lives on `GitHub`_ in a number of repositories below
the `Ceph "organization"`_.

.. _`Ceph "organization"`: https://github.com/ceph

To make a meaningful contribution to the project as a developer, a working
knowledge of git_ is essential.

.. _git: https://git-scm.com/documentation

Although the `Ceph "organization"`_ includes several software repositories,
this document covers only one: https://github.com/ceph/ceph.

Issue tracker
-------------

Although `GitHub`_ is used for code, Ceph-related issues (Bugs, Features,
Backports, Documentation, etc.) are tracked at http://tracker.ceph.com,
which is powered by `Redmine`_.

.. _Redmine: http://www.redmine.org

The tracker has a Ceph project with a number of subprojects loosely
corresponding to the project components listed in `Architecture`_.

Mere `registration`_ in the tracker automatically grants permissions
sufficient to open new issues and comment on existing ones.

.. _registration: http://tracker.ceph.com/account/register

To report a bug or propose a new feature, `jump to the Ceph project`_ and
click on `New issue`_.

.. _`jump to the Ceph project`: http://tracker.ceph.com/projects/ceph
.. _`New issue`: http://tracker.ceph.com/projects/ceph/issues/new

Mailing list
------------

Ceph development email discussions take place on the mailing list
``ceph-devel@vger.kernel.org``. The list is open to all. Subscribe by
sending a message to ``majordomo@vger.kernel.org`` with the line: ::

    subscribe ceph-devel

in the body of the message.

There are also `other Ceph-related mailing lists`_. 

.. _`other Ceph-related mailing lists`: https://ceph.com/resources/mailing-list-irc/

IRC
---

In addition to mailing lists, the Ceph community also communicates in real
time using `Internet Relay Chat`_.  

.. _`Internet Relay Chat`: http://www.irchelp.org/

See https://ceph.com/resources/mailing-list-irc/ for how to set up your IRC
client and a list of channels.

Submitting patches
------------------

The canonical instructions for submitting patches are contained in the 
`the file CONTRIBUTING.rst in the top-level directory of the source-code
tree`_. There may be some overlap between this guide and that file.

.. _`the file CONTRIBUTING.rst in the top-level directory of the source-code tree`: 
  https://github.com/ceph/ceph/blob/master/CONTRIBUTING.rst

All newcomers are encouraged to read that file carefully.

Building from source
--------------------

See instructions at :doc:`/install/build-ceph`.

Development-mode cluster
------------------------

You can start a development-mode Ceph cluster, after compiling the source, 
with:

.. code::

    cd src
    install -d -m0755 out dev/osd0
    ./vstart.sh -n -x -l
    # check that it's there
    ./ceph health


Basic workflow
==============

.. epigraph::

    Without bugs, there would be no software, and without software, there would
    be no software developers. 

    --Unknown
    
Having already introduced the `Issue tracker`_ and the `Source code
repositories`_, and having touched upon `Submitting patches`_, we now
describe these in more detail in the context of basic Ceph development
workflows.

Issue tracker conventions
-------------------------

When you start working on an existing issue, it's nice to let the other
developers know this - to avoid duplication of labor. Typically, this is
done by changing the :code:`Assignee` field (to yourself) and changing the
:code:`Status` to *In progress*. Newcomers to the Ceph community typically do not
have sufficient privileges to update these fields, however: they can
simply update the issue with a brief note.

.. table:: Meanings of some commonly used statuses

   ================ ===========================================
   Status           Meaning
   ================ ===========================================
   New              Initial status
   In Progress      Somebody is working on it
   Need Review      Pull request is open with a fix
   Pending Backport Fix has been merged, backport(s) pending
   Resolved         Fix and backports (if any) have been merged
   ================ ===========================================

Pull requests
-------------

The Ceph source code is maintained in the `ceph/ceph repository` on
`GitHub`_.

.. _`ceph/ceph project on GitHub`: https://github.com/ceph/ceph

The `GitHub`_ web interface provides a key feature for contributing code
to the project: the *pull request*.

Newcomers who are uncertain how to use pull requests may read
`this GitHub pull request tutorial`_.

.. _`this GitHub pull request tutorial`: 
   https://help.github.com/articles/using-pull-requests/

For some ideas on what constitutes a "good" pull request, see
the `Git Commit Good Practice`_ article at the `OpenStack Project Wiki`_.

.. _`Git Commit Good Practice`: https://wiki.openstack.org/wiki/GitCommitMessages
.. _`OpenStack Project Wiki`: https://wiki.openstack.org/wiki/Main_Page


Architecture
============

Ceph is a collection of components built on top of RADOS and provide
services (RBD, RGW, CephFS) and APIs (S3, Swift, POSIX) for the user to
store and retrieve data.

See :doc:`/architecture` for an overview of Ceph architecture. The
following sections treat each of the major architectural components
in more detail, with links to code and tests.

.. FIXME The following are just stubs. These need to be developed into
   detailed descriptions of the various high-level components (RADOS, RGW,
   etc.) with breakdowns of their respective subcomponents.

.. FIXME Later, in the Testing chapter I would like to take another look
   at these components/subcomponents with a focus on how they are tested.

RADOS
-----

RADOS stands for "Reliable, Autonomic Distributed Object Store". In a Ceph
cluster, all data are stored in objects, and RADOS is the component responsible
for that. 

RADOS itself can be further broken down into Monitors, Object Storage Daemons
(OSDs), and client APIs (librados). Monitors and OSDs are introduced at
:doc:`/start/intro`. The client library is explained at
:doc:`/rados/api/index`.

RGW
---

RGW stands for RADOS Gateway. Using the embedded HTTP server civetweb_, RGW
provides a REST interface to RADOS objects.

.. _civetweb: https://github.com/civetweb/civetweb

A more thorough introduction to RGW can be found at :doc:`/radosgw/index`.

RBD
---

RBD stands for RADOS Block Device. It enables a Ceph cluster to store disk
images, and includes in-kernel code enabling RBD images to be mounted.

To delve further into RBD, see :doc:`/rbd/rbd`.

CephFS
------

CephFS is a distributed file system that enables a Ceph cluster to be used as a NAS.

File system metadata is managed by Meta Data Server (MDS) daemons. The Ceph
file system is explained in more detail at :doc:`/cephfs/index`.

.. WIP
.. ===
..
.. Building RPM packages
.. ---------------------
..
.. Ceph is regularly built and packaged for a number of major Linux
.. distributions. At the time of this writing, these included CentOS, Debian,
.. Fedora, openSUSE, and Ubuntu.

