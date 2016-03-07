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

Redmine issue tracker
---------------------

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


Issue tracker
=============

See `Redmine issue tracker`_ for a brief introduction to the Ceph Issue Tracker.

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

Basic workflow
==============

The following chart illustrates basic development workflow:

.. ditaa::

            Upstream Code                       Your Local Environment

           /----------\        git clone           /-------------\
           |   Ceph   | -------------------------> | ceph/master |
           \----------/                            \-------------/
                ^                                    |
                |                                    | git branch fix_1
                | git merge                          |
                |                                    v
           /----------------\  git commit --amend   /-------------\
           |  make check    |---------------------> | ceph/fix_1  |
           | ceph--qa--suite|                       \-------------/
           \----------------/                        |
                ^                                    | fix changes
                |                                    | test changes
                | review                             | git commit
                |                                    | 
                |                                    v
           /--------------\                        /-------------\
           |   github     |<---------------------- | ceph/fix_1  |
           | pull request |         git push       \-------------/
           \--------------/

Below we present an explanation of this chart. The explanation is written
with the assumption that you, the reader, are a beginning developer who
has an idea for a bugfix, but do not know exactly how to proceed.

Update the tracker
------------------

Before you start, you should know the `Issue tracker`_ number of the bug
you intend to fix. If there is no tracker issue, now is the time to create
one. 

The tracker is there to explain the issue (bug) to your fellow Ceph
developers, so take care to provide a descriptive title as well as
sufficient information and details in the description.

If you have sufficient tracker permissions, assign the bug to yourself by
changing the ``Assignee`` field.  If your tracker permissions have not yet
been elevated, simply add a comment to the issue to let the other
developers know you are working on the bug.

Upstream code
-------------

This section, and the ones that follow, correspond to the nodes in the
above chart.

The upstream code lives in https://github.com/ceph/ceph.git, which is
sometimes referred to as the "upstream repo", or simply "upstream". As the
chart illustrates, we will make a local copy of this code, modify it, test
our modifications, and submit the modifications back to the upstream repo
for review.

A local copy of the upstream code is made by 

1. forking the upstream repo on GitHub, and 
2. cloning your fork to make a local working copy

See the `the GitHub documentation
<https://help.github.com/articles/fork-a-repo/#platform-linux>`_ for
detailed instructions on forking. In short, if your GitHub username is
"mygithubaccount", your fork of the upstream repo will show up at
https://github.com/mygithubaccount/ceph. Once you have created your fork,
you clone it by doing:

.. code::

    $ git clone https://github.com/mygithubaccount/ceph

While it is possible to clone the upstream repo directly, in this case we
must fork it first, because that (forking) makes it possible to open a
GitHub pull request.

For more information on using GitHub, refer to `GitHub Help
<https://help.github.com/>`_.

Local environment
-----------------

In the local environment created in the previous step, we now have a
copy of the ``master`` branch in ``remotes/origin/master``. Since the fork
(https://github.com/mygithubaccount/ceph.git) is frozen in time and the
upstream repo (https://github.com/ceph/ceph.git, typically abbreviated to
``ceph/ceph.git``) is updated frequently by other developers, we will need
to add the upstream repo as a "remote" so we can fetch from it:

.. code::

    $ git remote add ceph https://github.com/ceph/ceph.git
    $ git fetch ceph

After running these commands, all the branches from ``ceph/ceph.git`` are
downloaded to the local git repo as ``remotes/ceph/$BRANCH_NAME`` and can be
referenced as ``ceph/$BRANCH_NAME`` in certain git commands. 

For example, your local ``master`` branch can be reset to the upstream Ceph
``master`` branch by doing:

.. code::

    $ git fetch ceph
    $ git checkout master
    $ git reset --hard ceph/master

The ``master`` branch of your fork can then be synced to upstream master by
doing:

.. code::

    $ git push -u origin master

Bugfix branch
-------------

Next, create a branch for the bugfix:

.. code::

    $ git checkout master
    $ git branch -b fix_1
    $ git push -u origin fix_1

This creates a ``fix_1`` branch locally and in our GitHub fork. At this
point, the ``fix_1`` branch is identical to the ``master`` branch, but not
for long! You are now ready to modify the code.

Fix bug locally
---------------

At this point, change the status of the tracker issue to "In progress" to
communicate to the other Ceph developers that you have begun working on a
fix. If you don't have permission to change that field, your comment that
you are working on the issue is sufficient.

In the best case, your fix is very simple and requires only minimal testing.
In the typical worst case, fixing the bug is an iterative process involving
trial and error, not to mention skill. Fixing bugs is beyond the scope
of the current discussion.

For a more detailed discussion of the tools available for validating your
bugfixes, see the `Testing`_ chapter.

For now, let us just assume that you have finished work on the bugfix and
that you have tested it and believe it works. Commit the changes to your local
branch and push the changes to your fork like so:

.. code::

    $ git push origin fix_1

GitHub pull request
-------------------

The next step is to open a GitHub pull request. The purpose of this step is
to make your bugfix available to the community of Ceph developers.
Additional testing will be done on it, and it will undergo code review.
In short, this is the point where you "go public" with your modifications.

If you are uncertain how to use pull requests, you may read
`this GitHub pull request tutorial`_.

.. _`this GitHub pull request tutorial`:
   https://help.github.com/articles/using-pull-requests/

For some ideas on what constitutes a "good" pull request, see
the `Git Commit Good Practice`_ article at the `OpenStack Project Wiki`_.

.. _`Git Commit Good Practice`: https://wiki.openstack.org/wiki/GitCommitMessages
.. _`OpenStack Project Wiki`: https://wiki.openstack.org/wiki/Main_Page

Once your pull request is opened, update the `Issue tracker`_ by adding a
comment to the bug. The update can be as simple as:

.. code::

    *PR*: https://github.com/ceph/ceph/pull/$NUMBER_OF_YOUR_PULL_REQUEST

Automated PR validation
-----------------------

When your PR hits GitHub, the Ceph project's `Continuous Integration (CI)
<https://en.wikipedia.org/wiki/Continuous_integration>`_
infrastructure will test it automatically. At the time of this writing
(March 2016), the automated CI testing included a test to check that the
commits in the PR are properly signed (see `Submitting patches`_) and a
``make check`` test.

The latter, ``make check``, builds the PR and runs it through a battery of
tests. These tests run on machines operated by the Ceph Continuous
Integration (CI) team. When the tests complete, the result will be shown
on GitHub in the pull request itself.

You can (and should) also test your modifications before you open a PR. 
Refer to the the `Testing`_ chapter for details.

Integration tests AKA ceph-qa-suite
-----------------------------------

Since Ceph is a complex beast, it may also be necessary to test your fix to
see how it behaves on real clusters running either on real or virtual
hardware. Tests designed for this purpose live in the `ceph-qa-suite
repository`_ and are run via the `teuthology framework`_.

.. _`ceph-qa-suite repository`: https://github.com/ceph/ceph-qa-suite/
.. _`teuthology framework`: https://github.com/ceph/teuthology

If you have access to an OpenStack tenant, you are encouraged to run the
integration tests yourself using `ceph-workbench ceph-qa-suite`_ 
and post the test results to the PR.

.. _`ceph-workbench ceph-qa-suite`: http://ceph-workbench.readthedocs.org/

The Ceph community also uses the `Sepia lab
<http://ceph.github.io/sepia/>`_ where the integration tests can be run on
real hardware. Other developers may add tags like "needs-qa" to your PR.
This allows PRs that need testing to be merged into a single branch and
tested all at the same time. Since teuthology suites can take hours
(even days in some cases) to run, this can save a lot of time.

Integration tests are discussed in more detail in the `Testing`_ chapter.

Code review
-----------

Once your bugfix has been thoroughly tested, or even during this process,
it will be subjected to code review by other developers. This typically
takes the form of correspondence in the PR itself, but can be supplemented
by discussions on `IRC`_ and the `Mailing list`_.

Amending your PR
----------------

While your PR is going through `Testing`_ and `Code review`_, you can
modify it at any time by editing files in your local branch.

After the changes are committed locally (to the ``fix_1`` branch in our
example), they need to be pushed to GitHub so they appear in the PR.

Often it is necessary to modify the PR after it has been opened. This is
done by adding commits to the ``fix_1`` branch and rebasing to modify the
branch's git history. See `this tutorial
<https://www.atlassian.com/git/tutorials/rewriting-history>`_ for a good
introduction to rebasing. When you are done with your modifications,
you will need to force push your branch with:

.. code::

    $ git push --force origin fix_1

Merge
-----

The bugfixing process culminates when one of the project leads decides to
merge your PR.

When this happens, it is a signal for you (or the lead who merged the PR)
to change the `Issue tracker`_ status to "Resolved". Some issues may be
flagged for backporting, in which case the status should be changed to
"Pending Backport" (see the ``Backporting``_ chapter for details).


Testing
=======

Ceph has two types of tests: "make check" tests and integration tests.
The former are run via ``GNU Make <https://www.gnu.org/software/make/>``,
and the latter are run via the `teuthology framework`_. The following two
chapters examine the "make check" and integration tests in detail.

Testing - make check
====================

After compiling Ceph, the ``make check`` command can be used to run the
code through a battery of tests covering various aspects of Ceph. For
inclusion in "make check", a test must:

* bind ports that do not conflict with other tests
* not require root access
* not require more than one machine to run
* complete within a few minutes

While it is possible to run ``make check`` directly, it can be tricky to
correctly set up your environment. Fortunately, a script is provided to
make it easier run "make check" on your code. It can be run from the
top-level directory of the Ceph source tree by doing::

    $ ./run-make-check.sh

You will need a minimum of 8GB of RAM and 32GB of free disk space for this
command to complete successfully. Depending on your hardware, it can take
from 20 minutes to three hours to complete, but it's worth the wait.

When you fix a bug, it's a good idea to add a test. See the `Writing make
check tests`_ chapter.

Further sections
----------------

* Principles of make check tests
* Where to find test results
* How to interpret test results
* Find the corresponding source code
* Writing make check tests
* Make check caveats

Testing - integration tests
===========================

When a test requires multiple machines, root access or lasts for a
longer time (for example, to simulate a realistic Ceph deployment), it
is deemed to be an integration test. Integration tests are organized into
"suites", which are defined in the `ceph-qa-suite repository`_ and run with
the `teuthology-suite`_ command.

A number of integration tests are run on a regular basis in the `Sepia
lab`_ against the official Ceph repositories (on the master development
branch and the stable branches). Traditionally, these tests are called "the
nightlies" because the Ceph core developers used to live and work in
the same time zone and from their perspective the tests were run overnight. 

The results of the nightlies are published at http://pulpito.ceph.com/
and http://pulpito.ovh.sepia.ceph.com:8081/, and are also reported on the
`ceph-qa mailing list <http://ceph.com/resources/mailing-list-irc/>`_ for
analysis.

Running integration tests
-------------------------

Some Ceph developers have access to the `Sepia lab`_ and are allowed to
schedule integration tests there. The developer nick shows in the test
results URL and in the first column of the Pulpito dashboard.

"How can I run the integration tests in my own environment?" you may ask.
One option is to set up a teuthology cluster on bare metal. Though this is
a non-trivial task, it `is` possible. Here are `some notes
<http://docs.ceph.com/teuthology/docs/LAB_SETUP.html>`_ to get you started
if you decide to go this route.

If you have access to an OpenStack tenant, you have another option:
teuthology has an OpenStack backend, which is documented `here
<https://github.com/dachary/teuthology/tree/openstack#openstack-backend>`_.
This OpenStack backend can build packages from a given git commit or
branch, provision VMs in a public or private OpenStack
instance (sometimes referred to as a "cloud"), install the packages and run
integration tests on those VMs. This process is controlled using a tool
called `ceph-workbench ceph-qa-suite`_. This tool also automates publishing
of test results at http://teuthology-logs.public.ceph.com. 

Running integration tests on your code contributions and publishing the
results allows reviewers to verify that changes to the code base do not
cause regressions, or to analyze test failures when they do occur.

teuthology-suite
----------------

Every teuthology cluster, whether bare-metal or cloud-provisioned, has a
so-called "teuthology node" from which tests suites are triggered using the
``teuthology-suite`` command.

A detailed description of each ``teuthology-suite`` option is available
by running the following command on the teuthology node::

   $ teuthology-suite --help

Integration tests are organized into suites for convenience. Before we
discuss the suites, we first explain how to read individual integration
tests.

Integration tests are defined by yaml files found in the ``suites``
subdirectory of the `ceph-qa-suite repository`_ and implemented by python
code found in the ``tasks`` subdirectory. Some tests ("standalone tests")
are defined in a single yaml file, while in others the definition is spread
over multiple files placed within a single directory. 

Reading a standalone test
-------------------------

Here is a commented example using the integration test
`rados/singleton/all/admin-socket.yaml
<https://github.com/ceph/ceph-qa-suite/blob/master/suites/rados/singleton/all/admin-socket.yaml>`_
::

      roles:
      - - mon.a
        - osd.0
        - osd.1
      tasks:
      - install:
      - ceph:
      - admin_socket:
          osd.0:
            version:
            git_version:
            help:
            config show:
            config set filestore_dump_file /tmp/foo:
            perf dump:
            perf schema:

The ``roles`` array determines the composition of the cluster (how
many MONs, OSDs, etc.) on which this test is designed to run, as well
as how these roles will be distributed over the machines in the
testing cluster. In this case, there is only one element in the
top-level array: therefore, only one machine is allocated to the
test. The nested array declares that this machine shall run a MON with
id ``a`` (that is the ``mon.a`` in the list of roles) and two OSDs
(``osd.0`` and ``osd.1``).

The body of the test is in the ``tasks`` array: each element is
evaluated in order, causing the corresponding python file found in the
``tasks`` subdirectory of the `teuthology repository`_ or
`ceph-qa-suite repository`_ to be run. The `install
<https://github.com/ceph/teuthology/blob/master/teuthology/task/install.py>`_
task comes first and installs the Ceph packages on each machine (as
defined by the ``roles`` array). A full description of the ``install``
task is `found in the python file
<https://github.com/ceph/teuthology/blob/master/teuthology/task/install.py>`_
(search for "def task").

The ``ceph`` task, which is documented `here
<https://github.com/ceph/ceph-qa-suite/blob/master/tasks/ceph.py>`_ (again,
search for "def tasks"), starts OSDs and MONs (and possibly MDSs as well)
as required by the ``roles`` array. In this example, it will start one MON
(``mon.a``) and two OSDs (``osd.0`` and ``osd.1``), all on the same
machine.

Once the Ceph cluster is healthy, the ``admin_socket`` task (`source code
<https://github.com/ceph/ceph-qa-suite/blob/master/tasks/admin_socket.py>`_)
starts. The parameter of the ``admin_socket`` task (and any other
task) is a structure which is interpreted as documented in the
task. In this example the parameters are a set of commands to be sent
to the admin socket of ``osd.0``. The task verifies that each of them returns
on success (i.e. exit code zero).

This test can be run with::

  teuthology-suite --suite rados/singleton/all/admin-socket.yaml

Test descriptions 
-----------------

In the previous example, the test was defined by a single yaml file. In
this case, the test description is simply the relative path to the yaml
file in question.

Much more commonly, tests are defined as the concatenation of several yaml
files. In these cases, the description of each test consists of the
subdirectory under `suites/
<https://github.com/ceph/ceph-qa-suite/tree/master/suites>`_ containing the
yaml files followed by an expression in curly braces (``{}``) consisting of
a list of yaml files to be concatenated. For instance the
test description::

  ceph-disk/basic/{distros/centos_7.0.yaml tasks/ceph-disk.yaml}

signifies the concatenation of two files:

* ceph-disk/basic/distros/centos_7.0.yaml
* ceph-disk/basic/tasks/ceph-disk.yaml

How are tests built from directories?
-------------------------------------

As noted in the previous section, most tests are not defined in a single
yaml file, but rather as the concatenation of files collected from a
directory tree within `ceph-qa-suite`_. 

The set of all tests generated from a given subdirectory of
`ceph-qa-suite`_ is called an "integration test suite", or a "teuthology
suite".

Concatenation is triggered by the presence of special files (``%`` and
``+``) within the directory tree in question. This is best explained by
example.

Concatenation using percent
---------------------------

For instance, the `ceph-disk suite
<https://github.com/ceph/ceph-qa-suite/tree/master/suites/ceph-disk/>`_ is
defined by the ``suites/ceph-disk/`` tree, which consists of the
files and subdirectories in the following structure:

  directory: ceph-disk/basic
      file: %
      directory: distros
         file: centos_7.0.yaml
         file: ubuntu_14.04.yaml
      directory: tasks
         file: ceph-disk.yaml

This is interpreted as two tests: (1) the concatenation of centos_7.0.yaml
and ceph-disk.yaml, and (2) the concatenation of ubuntu_14.04.yaml and
ceph-disk.yaml. In accordance with teuthology usage, the test descriptions
are:

1. ceph-disk/basic/{distros/centos_7.0.yaml tasks/ceph-disk.yaml}
2. ceph-disk/basic/{distros/ubuntu_14.04.yaml tasks/ceph-disk.yaml}

(In human terms, this means that the task found in ``ceph-disk.yaml`` is
intended to run on both CentOS 7.0 and Ubuntu 14.04.)

If you look up these files in the `ceph-qa-suite`_ repo, you will notice
that the ``centos_7.0.yaml`` and ``ubuntu_14.04.yaml`` files in the
``suites/ceph-disk/basic/distros/`` directory are implemented as symlinks
to ``distros/centos_7.0.yaml`` and ``distros/ubuntu_14.04.yaml``,
respectively. The practice of using symlinks instead of files enables a
single file to be used in multiple suites.

The special file percent (``%``) is interpreted as a requirement to
generate tests combining all files found in the current directory and
in its direct subdirectories. Without the file percent, the
``ceph-disk`` tree would be interpreted as three standalone tests:

* ceph-disk/basic/distros/centos_7.0.yaml
* ceph-disk/basic/distros/ubuntu_14.04.yaml
* ceph-disk/basic/tasks/ceph-disk.yaml

All the tests generated from the ``suites/ceph-disk/`` directory tree
(also known as the "ceph-disk suite") can be run with::

  $ teuthology-suite --suite ceph-disk

An individual test from the ceph-disk suite can be run by adding the
``--filter`` option::

  $ teuthology-suite \
      --suite ceph-disk/basic \
      --filter 'ceph-disk/basic/{distros/ubuntu_14.04.yaml tasks/ceph-disk.yaml}'

Concatenation using plus
------------------------

For even greater flexibility in sharing yaml files between suites, the
special file plus (``+``) can be used to concatenate files within a
directory. For instance, consider the `suites/rbd/thrash
<https://github.com/ceph/ceph-qa-suite/tree/master/suites/rbd/thrash>`_
tree::

  directory: rbd/thrash
    file: %
    directory: clusters
      file: +
      file: fixed-2.yaml
      file: openstack.yaml
    directory: workloads
      file: rbd_api_tests_copy_on_read.yaml
      file: rbd_api_tests.yaml

This creates two tests:

* rbd/thrash/{clusters/fixed-2.yaml clusters/openstack.yaml workloads/rbd_api_tests_copy_on_read.yaml}
* rbd/thrash/{clusters/fixed-2.yaml clusters/openstack.yaml workloads/rbd_api_tests.yaml}

Because the ``clusters/`` subdirectory contains the special file plus
(``+``), all the other files in that subdirectory (``fixed-2.yaml`` and
``openstack.yaml`` in this case) are concatenated together
and treated as a single file. Without the special file plus, they would
have been combined with the files from the workloads directory to create
a matrix of four tests:

* rbd/thrash/{clusters/openstack.yaml workloads/rbd_api_tests_copy_on_read.yaml}
* rbd/thrash/{clusters/openstack.yaml workloads/rbd_api_tests.yaml}
* rbd/thrash/{clusters/fixed-2.yaml workloads/rbd_api_tests_copy_on_read.yaml}
* rbd/thrash/{clusters/fixed-2.yaml workloads/rbd_api_tests.yaml}

The ``clusters/fixed-2.yaml`` file is shared among many suites to
define the following ``roles``::

  roles:
  - [mon.a, mon.c, osd.0, osd.1, osd.2, client.0]
  - [mon.b, osd.3, osd.4, osd.5, client.1]

The ``rbd/thrash`` suite as defined above, consisting of two tests, 
can be run with::

  $ teuthology-suite --suite rbd/thrash

A single test from the rbd/thrash suite can be run by adding the
``--filter`` option::

  $ teuthology-suite \
      --suite rbd/thrash \
      --filter 'rbd/thrash/{clusters/fixed-2.yaml clusters/openstack.yaml workloads/rbd_api_tests_copy_on_read.yaml}'


Filtering tests by their description
------------------------------------

When a few jobs fail and need to be run again, the ``--filter`` option
will select the tests with a matching description. For instance if the
``rados`` suite fails the `all/peer.yaml <https://github.com/ceph/ceph-qa-suite/blob/master/suites/rados/singleton/all/peer.yaml>`_ test, the following will only run the tests that contain this file::

  teuthology-suite --suite rados --filter all/peer.yaml

The ``--filter-out`` option does the opposite (it matches tests that do
not contain a given string), and can be combined with the ``--filter``
option.

Both ``--filter`` and ``--filter-out`` take a comma-separated list of strings (which
means the comma character is implicitly forbidden in filenames found in the
`ceph-qa-suite repository`_). For instance::

  teuthology-suite --suite rados --filter all/peer.yaml,all/rest-api.yaml

will run tests that contain either
`all/peer.yaml <https://github.com/ceph/ceph-qa-suite/blob/master/suites/rados/singleton/all/peer.yaml>`_
or
`all/rest-api.yaml <https://github.com/ceph/ceph-qa-suite/blob/master/suites/rados/singleton/all/rest-api.yaml>`_

Each string is looked up anywhere in the test description and has to
be an exact match: they are not regular expressions.

Reducing the number of tests
----------------------------

The ``rados`` suite generates thousands of tests out of a few hundred
files. For instance, all tests in the `rados/thrash suite
<https://github.com/ceph/ceph-qa-suite/tree/master/suites/rados/thrash>`_
run for ``ext4``, ``xfs`` and ``btrfs`` because they are combined (via
special file ``%``) with the `fs directory
<https://github.com/ceph/ceph-qa-suite/tree/master/suites/rados/thrash/fs>`_

All these tests are required before a Ceph release is published but it
is too much when verifying a contribution can be merged without
risking a trivial regression. The ``--subset`` option can be used to
reduce the number of tests that are triggered. For instance::

  teuthology-suite --suite rados --subset 0/4000

will run as few tests as possible. The tradeoff is that some tests
will only run on ``ext4`` and not on ``btrfs``, but all files in the
suite will be in at least one test. Understanding the actual logic that
drives this requires reading the teuthology source code.

The ``--limit`` option only runs the first ``N`` tests in the suite:
this is however rarely useful because there is no way to control which test
will be first.

Suites inventory
----------------

The ``suites`` directory of the `ceph-qa-suite repository`_ contains
all the integration tests, for all the Ceph components.

`ceph-deploy <https://github.com/ceph/ceph-qa-suite/tree/master/suites/ceph-deploy>`_
  install a Ceph cluster with ``ceph-deploy`` (`ceph-deploy man page`_)

`ceph-disk <https://github.com/ceph/ceph-qa-suite/tree/master/suites/ceph-disk>`_
  verify init scripts (upstart etc.) and udev integration with
  ``ceph-disk`` (`ceph-disk man page`_), with and without `dmcrypt
  <https://gitlab.com/cryptsetup/cryptsetup/wikis/DMCrypt>`_ support.

`dummy <https://github.com/ceph/ceph-qa-suite/tree/master/suites/dummy>`_
  get a machine, do nothing and return success (commonly used to
  verify the integration testing infrastructure works as expected)
  expected

`fs <https://github.com/ceph/ceph-qa-suite/tree/master/suites/fs>`_
  test CephFS

`kcephfs <https://github.com/ceph/ceph-qa-suite/tree/master/suites/kcephfs>`_
  test the CephFS kernel module

`krbd <https://github.com/ceph/ceph-qa-suite/tree/master/suites/krbd>`_
  test the RBD kernel module

`powercycle <https://github.com/ceph/ceph-qa-suite/tree/master/suites/powercycle>`_
  verify the Ceph cluster behaves when machines are powered off
  and on again

`rados <https://github.com/ceph/ceph-qa-suite/tree/master/suites/rados>`_
  run Ceph clusters including OSDs and MONs, under various conditions of
  stress

`rbd <https://github.com/ceph/ceph-qa-suite/tree/master/suites/rbd>`_
  run RBD tests using actual Ceph clusters, with and without qemu

`rgw <https://github.com/ceph/ceph-qa-suite/tree/master/suites/rgw>`_
  run RGW tests using actual Ceph clusters

`smoke <https://github.com/ceph/ceph-qa-suite/tree/master/suites/smoke>`_
  run tests that exercise the Ceph API with an actual Ceph cluster

`teuthology <https://github.com/ceph/ceph-qa-suite/tree/master/suites/teuthology>`_
  verify that teuthology can run integration tests, with and without OpenStack

`upgrade <https://github.com/ceph/ceph-qa-suite/tree/master/suites/upgrade>`_
  for various versions of Ceph, verify that upgrades can happen
  without disrupting an ongoing workload

.. _`ceph-deploy man page`: ../../man/8/ceph-deploy
.. _`ceph-disk man page`: ../../man/8/ceph-disk

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
