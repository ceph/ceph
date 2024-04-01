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

========= ================== =============
Scope     Lead               GitHub nick
========= ================== =============
RADOS     Radoslaw Zarzynski rzarzynski 
RGW       Casey Bodley       cbodley 
RGW       Matt Benjamin      mattbenjamin
RBD       Ilya Dryomov       dis 
CephFS    Venky Shankar      vshankar
Dashboard Nizamudeen A       nizamial09 
Build/Ops Ken Dreyer         ktdreyer
Docs      Zac Dover          zdover23
========= ================== =============

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

A working knowledge of git_ is essential to make a meaningful contribution to the project as a developer.

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

Slack
-----

Ceph's Slack is https://ceph-storage.slack.com/.

.. _mailing-list:

Mailing lists
-------------

Ceph Development Mailing List
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``dev@ceph.io`` list is for discussion about the development of Ceph,
its interoperability with other technology, and the operations of the
project itself.

The email discussion list for Ceph development is open to all. Subscribe by
sending a message to ``dev-request@ceph.io`` with the following line in the
body of the message::

    subscribe ceph-devel


Ceph Client Patch Review Mailing List
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``ceph-devel@vger.kernel.org`` list is for discussion and patch review
for the Linux kernel Ceph client component. Note that this list used to
be an all-encompassing list for developers. When searching the archives, 
remember that this list contains the generic devel-ceph archives before mid-2018.

Subscribe to the list covering the Linux kernel Ceph client component by sending
a message to ``majordomo@vger.kernel.org`` with the following line in the body
of the message::

    subscribe ceph-devel


Other Ceph Mailing Lists
~~~~~~~~~~~~~~~~~~~~~~~~

There are also `other Ceph-related mailing lists`_.

.. _`other Ceph-related mailing lists`: https://ceph.com/irc/

.. _irc:


IRC
---

In addition to mailing lists, the Ceph community also communicates in real time
using `Internet Relay Chat`_.

.. _`Internet Relay Chat`: http://www.irchelp.org/

The Ceph community gathers in the #ceph channel of the Open and Free Technology
Community (OFTC) IRC network.

Created in 1988, Internet Relay Chat (IRC) is a relay-based, real-time chat
protocol. It is mainly designed for group (many-to-many) communication in
discussion forums called channels, but also allows one-to-one communication via
private message. On IRC you can talk to many other members using Ceph, on
topics ranging from idle chit-chat to support questions. Though a channel might
have many people in it at any one time, they might not always be at their
keyboard; so if no-one responds, just wait around and someone will hopefully
answer soon enough.

Registration
~~~~~~~~~~~~

If you intend to use the IRC service on a continued basis, you are advised to
register an account. Registering gives you a unique IRC identity and allows you
to access channels where unregistered users have been locked out for technical
reasons.

See ``the official OFTC (Open and Free Technology Community) documentation's
registration instructions
<https://www.oftc.net/Services/#register-your-account>`` to learn how to
register your IRC account.

Channels
~~~~~~~~

To connect to the OFTC IRC network, download an IRC client and configure it to
connect to ``irc.oftc.net``. Then join one or more of the channels. Discussions
inside #ceph are logged and archives are available online.

Here are the real-time discussion channels for the Ceph community:

  -  #ceph
  -  #ceph-devel
  -  #cephfs
  -  #ceph-dashboard
  -  #ceph-orchestrators
  -  #sepia


.. _submitting-patches:

Submitting patches
------------------

The canonical instructions for submitting patches are contained in the
file `CONTRIBUTING.rst`_ in the top-level directory of the source-code
tree. There may be some overlap between this guide and that file.

.. _`CONTRIBUTING.rst`:
  https://github.com/ceph/ceph/blob/main/CONTRIBUTING.rst

All newcomers are encouraged to read that file carefully.

Building from source
--------------------

See instructions at :doc:`/install/build-ceph`.

Using ccache to speed up local builds
-------------------------------------
`ccache`_ can make the process of rebuilding the ceph source tree faster. 

Before you use `ccache`_ to speed up your rebuilds of the ceph source tree,
make sure that your source tree is clean and will produce no build failures.
When you have a clean source tree, you can confidently use `ccache`_, secure in
the knowledge that you're not using a dirty tree.

Old build artifacts can cause build failures. You might introduce these
artifacts unknowingly when switching from one branch to another. If you see
build errors when you attempt a local build, follow the procedure below to
clean your source tree.

Cleaning the Source Tree
~~~~~~~~~~~~~~~~~~~~~~~~

.. prompt:: bash $

  ninja clean
  
.. note:: The following commands will remove everything in the source tree 
          that isn't tracked by git. Make sure to back up your log files 
          and configuration options before running these commands.

.. prompt:: bash $

   git clean -fdx; git submodule foreach git clean -fdx

Building Ceph with ccache
~~~~~~~~~~~~~~~~~~~~~~~~~

``ccache`` is available as a package in most distros. To build ceph with
ccache, run the following command.

.. prompt:: bash $

  cmake -DWITH_CCACHE=ON ..

Using ccache to Speed Up Build Times
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ccache`` can be used for speeding up all builds of the system. For more
details, refer to the `run modes`_ section of the ccache manual. The default
settings of ``ccache`` can be displayed with the ``ccache -s`` command.

.. note:: We recommend overriding the ``max_size``. The default is 10G.
          Use a larger value, like 25G. Refer to the `configuration`_ section
          of the ccache manual for more information.

To further increase the cache hit rate and reduce compile times in a
development environment, set the version information and build timestamps to
fixed values. This makes it unnecessary to rebuild the binaries that contain
this information.

This can be achieved by adding the following settings to the ``ccache``
configuration file ``ccache.conf``::

  sloppiness = time_macros
  run_second_cpp = true

Now, set the environment variable ``SOURCE_DATE_EPOCH`` to a fixed value (a
UNIX timestamp) and set ``ENABLE_GIT_VERSION`` to ``OFF`` when running
``cmake``:

.. prompt:: bash $

  export SOURCE_DATE_EPOCH=946684800
  cmake -DWITH_CCACHE=ON -DENABLE_GIT_VERSION=OFF ..

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

.. _backporting:

Backporting
-----------

All bugfixes should be merged to the ``main`` branch before being
backported. To flag a bugfix for backporting, make sure it has a
`tracker issue`_ associated with it and set the ``Backport`` field to a
comma-separated list of previous releases (e.g. "hammer,jewel") that you think
need the backport.
The rest (including the actual backporting) will be taken care of by the
`Stable Releases and Backports`_ team.

.. _`tracker issue`: http://tracker.ceph.com/
.. _`Stable Releases and Backports`: http://tracker.ceph.com/projects/ceph-releases/wiki

Dependabot
----------

Dependabot is a GitHub bot that scans the dependencies in the repositories for
security vulnerabilities (CVEs). If a fix is available for a discovered CVE,
Dependabot creates a pull request to update the dependency.

Dependabot also indicates the compatibility score of the upgrade. This score is
based on the number of CI failures that occur in other GitHub repositories
where the fix was applied. 

With some configuration, Dependabot can perform non-security updates (for
example, it can upgrade to the latest minor version or patch version).

Dependabot supports `several languages and package managers
<https://docs.github.com/en/code-security/dependabot/dependabot-version-updates/about-dependabot-version-updates#supported-repositories-and-ecosystems>`_.
As of July 2022, the Ceph project receives alerts only from pip (based on the
`requirements.txt` files) and npm (`package*.json`). It is possible to extend
these alerts to git submodules, Golang, and Java. As of July 2022, there is no
support for C++ package managers such as vcpkg, conan, C++20 modules.

Many of the dependencies discovered by Dependabot will best be updated
elsewhere than the Ceph Github repository (distribution packages, for example,
will be a better place to update some of the dependencies). Nonetheless, the
list of new and existing vulnerabilities generated by Dependabot will be
useful.

`Here is an example of a Dependabot pull request.
<https://github.com/ceph/ceph/pull/46998>`_

Guidance for use of cluster log
-------------------------------

If your patches emit messages to the Ceph cluster log, please consult
this: :doc:`/dev/logging`.
