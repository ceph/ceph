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

========= ================ =============
Scope     Lead             GitHub nick
========= ================ =============
Ceph      Sage Weil        liewegas
RADOS     Neha Ojha        neha-ojha
RGW       Yehuda Sadeh     yehudasa
RGW       Matt Benjamin    mattbenjamin
RBD       Jason Dillaman   dillaman
CephFS    Patrick Donnelly batrick
Dashboard Lenz Grimmer     LenzGr
MON       Joao Luis        jecluis
Build/Ops Ken Dreyer       ktdreyer
========= ================ =============

The Ceph-specific acronyms in the table are explained in
:doc:`/architecture`.

History
-------

See the `History chapter of the Wikipedia article`_.

.. _`History chapter of the Wikipedia article`: https://en.wikipedia.org/wiki/Ceph_%28software%29#History

Licensing
---------

Ceph is free software.

Unless stated otherwise, the Ceph source code is distributed under the
terms of the LGPL2.1 or LGPL3.0. For full details, see the file
`COPYING`_ in the top-level directory of the source-code tree.

.. _`COPYING`:
  https://github.com/ceph/ceph/blob/master/COPYING

Source code repositories
------------------------

The source code of Ceph lives on `GitHub`_ in a number of repositories below
the `Ceph "organization"`_.

.. _`Ceph "organization"`: https://github.com/ceph

To make a meaningful contribution to the project as a developer, a working
knowledge of git_ is essential.

.. _git: https://git-scm.com/doc

Although the `Ceph "organization"`_ includes several software repositories,
this document covers only one: https://github.com/ceph/ceph.

Redmine issue tracker
---------------------

Although `GitHub`_ is used for code, Ceph-related issues (Bugs, Features,
Backports, Documentation, etc.) are tracked at http://tracker.ceph.com,
which is powered by `Redmine`_.

.. _Redmine: http://www.redmine.org

The tracker has a Ceph project with a number of subprojects loosely
corresponding to the various architectural components (see
:doc:`/architecture`).

Mere `registration`_ in the tracker automatically grants permissions
sufficient to open new issues and comment on existing ones.

.. _registration: http://tracker.ceph.com/account/register

To report a bug or propose a new feature, `jump to the Ceph project`_ and
click on `New issue`_.

.. _`jump to the Ceph project`: http://tracker.ceph.com/projects/ceph
.. _`New issue`: http://tracker.ceph.com/projects/ceph/issues/new

Mailing list
------------

The ``dev@ceph.io`` list is for discussion about the development of Ceph,
its interoperability with other technology, and the operations of the
project itself.

The email discussion list for Ceph development is open to all. Subscribe by
sending a message to ``dev-request@ceph.io`` with the following line in the
body of the message: ::

    subscribe ceph-devel

The ``ceph-devel@vger.kernel.org`` list is for discussion and patch review
for the Linux kernel Ceph client component. Note that this list used to
be an all-encompassing list for developers. So when searching the archives, 
remember that this list contains the generic devel-ceph archives pre mid-2018.

Subscription works in the same way, by sending a message to
``majordomo@vger.kernel.org`` with the line: ::

    subscribe ceph-devel

in the body of the message.


Ceph development email discussions the list is open to all. Subscribe by
sending a message to ``dev-request@ceph.io`` with the line: ::

    subscribe ceph-devel

in the body of the message.

Subscribing to the

There are also `other Ceph-related mailing lists`_.

.. _`other Ceph-related mailing lists`: https://ceph.com/irc/


IRC
---

In addition to mailing lists, the Ceph community also communicates in real
time using `Internet Relay Chat`_.

.. _`Internet Relay Chat`: http://www.irchelp.org/

See ``https://ceph.com/irc/`` for how to set up your IRC
client and a list of channels.

Submitting patches
------------------

The canonical instructions for submitting patches are contained in the
file `CONTRIBUTING.rst`_ in the top-level directory of the source-code
tree. There may be some overlap between this guide and that file.

.. _`CONTRIBUTING.rst`:
  https://github.com/ceph/ceph/blob/master/CONTRIBUTING.rst

All newcomers are encouraged to read that file carefully.

Building from source
--------------------

See instructions at :doc:`/install/build-ceph`.

Using ccache to speed up local builds
-------------------------------------

Rebuilds of the ceph source tree can benefit significantly from use of
`ccache`_.

Many a times while switching branches and such, one might see build failures
for certain older branches mostly due to older build artifacts. These rebuilds
can significantly benefit the use of ccache. For a full clean source tree, one
could do ::

  $ make clean

  # note the following will nuke everything in the source tree that
  # isn't tracked by git, so make sure to backup any log files /conf options

  $ git clean -fdx; git submodule foreach git clean -fdx

ccache is available as a package in most distros. To build ceph with ccache
one can::

  $ cmake -DWITH_CCACHE=ON ..

ccache can also be used for speeding up all builds in the system. for more
details refer to the `run modes`_ of the ccache manual. The default settings
of ``ccache`` can be displayed with ``ccache -s``.

.. note:: It is recommended to override the ``max_size``, which is the size of
   cache, defaulting to 10G, to a larger size like 25G or so. Refer to the
   `configuration`_ section of ccache manual.

To further increase the cache hit rate and reduce compile times in a
development environment, it is possible to set version information and build
timestamps to fixed values, which avoids frequent rebuilds of binaries that
contain this information.

This can be achieved by adding the following settings to the ``ccache``
configuration file ``ccache.conf``::

  sloppiness = time_macros
  run_second_cpp = true

Now, set the environment variable ``SOURCE_DATE_EPOCH`` to a fixed value (a
UNIX timestamp) and set ``ENABLE_GIT_VERSION`` to ``OFF`` when running
``cmake``::

  $ export SOURCE_DATE_EPOCH=946684800
  $ cmake -DWITH_CCACHE=ON -DENABLE_GIT_VERSION=OFF ..

.. note:: Binaries produced with these build options are not suitable for
  production or debugging purposes, as they do not contain the correct build
  time and git version information.

.. _`ccache`: https://ccache.samba.org/
.. _`run modes`: https://ccache.samba.org/manual.html#_run_modes
.. _`configuration`: https://ccache.samba.org/manual.html#_configuration

Development-mode cluster
------------------------

See :doc:`/dev/quick_guide`.

Kubernetes/Rook development cluster
-----------------------------------

See :ref:`kubernetes-dev`

Backporting
-----------

All bugfixes should be merged to the ``master`` branch before being
backported. To flag a bugfix for backporting, make sure it has a
`tracker issue`_ associated with it and set the ``Backport`` field to a
comma-separated list of previous releases (e.g. "hammer,jewel") that you think
need the backport.
The rest (including the actual backporting) will be taken care of by the
`Stable Releases and Backports`_ team.

.. _`tracker issue`: http://tracker.ceph.com/
.. _`Stable Releases and Backports`: http://tracker.ceph.com/projects/ceph-releases/wiki

Guidance for use of cluster log
-------------------------------

If your patches emit messages to the Ceph cluster log, please consult
this: :doc:`/dev/logging`.
