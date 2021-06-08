Basic Workflow
==============

The following chart illustrates the basic Ceph development workflow:

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
           | ninja check    |---------------------> | ceph/fix_1  |
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

This page assumes that you are a new contributor with an idea for a bugfix or
enhancement, but do not know how to proceed. Watch the `Getting Started with
Ceph Development <https://www.youtube.com/watch?v=t5UIehZ1oLs>`_ video for a
practical summary of this workflow.

Updating the tracker
--------------------

Before you start, you should know the :ref:`issue-tracker` (Redmine) number
of the bug you intend to fix. If there is no tracker issue, now is the time to
create one for code changes.  Straightforward documentation cleanup does
not necessarily require a corresponding tracker issue. However, an issue
(ticket) should be created if one is adding new documentation chapters or
files, or for other substantial changes.

The tracker ticket serves to explain the issue (bug) to your fellow Ceph
developers and keep them informed as you make progress toward resolution.  To
this end, please provide a descriptive title and write appropriate information
and details into the description.  When composing the ticket's title, consider "If I
want to search for this ticket two years from now, what keywords will I search
for?"

If you have sufficient tracker permissions, assign the bug to yourself by
setting the ``Assignee`` field.  If your tracker permissions have not been
elevated, simply add a comment with a short message like "I am working on this
issue".

Forking and Cloning the Ceph Repository
---------------------------------------

This section, and the ones that follow, correspond to nodes in the above chart.

The upstream code is found at https://github.com/ceph/ceph.git, which is known
as the "upstream repo", or simply "upstream". As the chart shows, we will make
a local copy of this repository, modify it, test our modifications, then submit
the modifications for review and merging.

A local copy of the upstream code is made by

1. Forking the upstream repo on GitHub, and
2. Cloning your fork to make a local working copy


Forking The Ceph Repository
^^^^^^^^^^^^^^^^^^^^^^^^^^^

See the `GitHub documentation
<https://help.github.com/articles/fork-a-repo/#platform-linux>`_ for
detailed instructions on forking. In short, if your GitHub username is
"mygithubaccount", your fork of the upstream repo will appear at
``https://github.com/mygithubaccount/ceph``. 

Cloning Your Fork  
^^^^^^^^^^^^^^^^^

Once you have created your fork, clone it by running:

.. prompt:: bash $

   git clone https://github.com/mygithubaccount/ceph

You must fork the Ceph repository before you clone it.  Without forking, you cannot 
open a `GitHub pull request
<https://docs.github.com/en/free-pro-team@latest/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request>`_.

For more information on using GitHub, refer to `GitHub Help
<https://help.github.com/>`_.

Configuring Your Local Environment
----------------------------------

In the local environment created in the previous step, you now have a copy of
the ``master`` branch in ``remotes/origin/master``. This fork
(https://github.com/mygithubaccount/ceph.git) is frozen in time and the
upstream repo (https://github.com/ceph/ceph.git, typically abbreviated to
``ceph/ceph.git``) is updated frequently by other contributors. This means that
you must sync your fork periodically. Failure to synchronize your fork may
result in your commits and pull requests failing to merge because they refer to
file contents that have changed since you last synchronized your fork.

Configure your local git environment with your name and email address.  

.. prompt:: bash $

   git config user.name "FIRST_NAME LAST_NAME"
   git config user.email "MY_NAME@example.com"

Add the upstream repo as a "remote" and fetch it:

.. prompt:: bash $

   git remote add ceph https://github.com/ceph/ceph.git
   git fetch ceph

These commands fetch all the branches and commits from ``ceph/ceph.git`` to the
local git repo as ``remotes/ceph/$BRANCH_NAME`` and can be referenced as
``ceph/$BRANCH_NAME`` in local git commands.


Resetting Local Master to Upstream Master
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Your local ``master`` branch can be reset to the upstream Ceph ``master``
branch by running the following commands:

.. prompt:: bash $

   git fetch ceph
   git checkout master
   git reset --hard ceph/master
   git push -u origin master

This procedure should be followed often, in order to keep your local ``master``
in sync with upstream ``master``.

Creating a Bugfix branch
------------------------

Create a branch for your bugfix:

.. prompt:: bash $

   git checkout master
   git checkout -b fix_1
   git push -u origin fix_1

This creates a local branch called ``fix_1`` in our GitHub fork. At this point,
the ``fix_1`` branch is identical to the ``master`` branch, but not for long!
You are now ready to modify the code.  Be careful to always run `git checkout
master` first, otherwise you may find commits from an unrelated branch mixed
with your new work.

Fixing the bug locally
----------------------

In the `Ceph issue tracker <https://tracker.ceph.com>`_, change the status of
the tracker issue to "In progress".  This communicates to other Ceph
contributors that you have begun working on a fix, which helps to avoid
duplication of effort.  If you don't have permission to change that field, your
previous comment that you are working on the issue is sufficient.

Your fix may be very simple and require only minimal testing. But that's not
likely. It is more likely that the process of fixing your bug will be iterative
and will involve trial and error, as well as skill. An explanation of how to
fix bugs is beyond the scope of this document. Instead, we focus on the
mechanics of the process in the context of the Ceph project.

For a detailed discussion of the tools available for validating bugfixes,
see the chapters on testing.

For now, let us assume that you have finished work on the bugfix, that you have
tested the bugfix , and that you believe that it works. Commit the changes to
your local branch using the ``--signoff`` option (here represented as the `s`
portion of the `-as` flag): 

.. prompt:: bash $

   git commit -as

Push the changes to your fork:

.. prompt:: bash $

   git push origin fix_1

Opening a GitHub pull request
-----------------------------

The next step is to open a GitHub pull request (PR). This makes your bugfix
visible to the community of Ceph contributors.  They will review it and may
perform additional testing and / or request changes.

This is the point where you "go public" with your modifications.  Be prepared
to receive suggestions and constructive criticism in the form of comments
within the PR. Don't worry!  The Ceph project is a friendly place!

If you are uncertain how to create and manage pull requests, you may read
`this GitHub pull request tutorial`_.

.. _`this GitHub pull request tutorial`:
   https://help.github.com/articles/using-pull-requests/

For ideas on what constitutes a "good" pull request, see
the `Git Commit Good Practice`_ article at the `OpenStack Project Wiki`_.

.. _`Git Commit Good Practice`: https://wiki.openstack.org/wiki/GitCommitMessages
.. _`OpenStack Project Wiki`: https://wiki.openstack.org/wiki/Main_Page

and our own `Submitting Patches <https://github.com/ceph/ceph/blob/master/SubmittingPatches.rst>`_ document.

Once your pull request (PR) is opened, update the :ref:`issue-tracker` by
adding a comment directing other contributors to your PR. The comment can be
as simple as::

    *PR*: https://github.com/ceph/ceph/pull/$NUMBER_OF_YOUR_PULL_REQUEST

Understanding Automated PR validation
-------------------------------------

When you create or update your PR, the Ceph project's `Continuous Integration
(CI) <https://en.wikipedia.org/wiki/Continuous_integration>`_ infrastructure
automatically tests it. At the time of this writing (September 2020), the
automated CI testing included five tests:

#. a test to check that the commits are properly signed (see :ref:`submitting-patches`):
#. a test to check that the documentation builds
#. a test to check that the submodules are unmodified
#. a test to check that the API is in order
#. a :ref:`make check<make-check>` test

Additional tests may be performed depending on which files your PR modifies.

The :ref:`make check<make-check>` test builds the PR and runs it through a battery of
tests. These tests run on servers operated by the Ceph Continuous
Integration (CI) team. When the tests complete, the result will be shown
on GitHub in the pull request itself.

You should test your modifications before you open a PR.
Refer to the chapters on testing for details.

Notes on PR make check test
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The GitHub :ref:`make check<make-check>` test is driven by a Jenkins instance.

Jenkins merges your PR branch into the latest version of the base branch before
starting tests. This means that you don't have to rebase the PR to pick up any fixes.

You can trigger PR tests at any time by adding a comment to the PR - the
comment should contain the string "test this please". Since a human subscribed
to the PR might interpret that as a request for him or her to test the PR, we
recommend that you address Jenkins directly. For example, write "jenkins retest
this please".  For efficiency a single re-test can also be requested with
e.g. "jenkins test signed".  For reference, a list of these requests is
automatically added to the end of each new PR's description.

If there is a build failure and you aren't sure what caused it, check the
:ref:`make check<make-check>` log. To access it, click on the "details" (next
to the :ref:`make check<make-check>` test in the PR) link to enter the Jenkins web
GUI. Then click on "Console Output" (on the left).

Jenkins is configured to search logs for strings known to have been associated
with :ref:`make check<make-check>` failures in the past. However, there is no
guarantee that these known strings are associated with any given
:ref:`make check<make-check>` failure. You'll have to read through the log to determine the
cause of your specific failure.

Integration tests AKA ceph-qa-suite
-----------------------------------

Since Ceph is complex, it may be necessary to test your fix to
see how it behaves on real clusters running on physical or virtual
hardware. Tests designed for this purpose live in the `ceph/qa
sub-directory`_ and are run via the `teuthology framework`_.

.. _`ceph/qa sub-directory`: https://github.com/ceph/ceph/tree/master/qa/
.. _`teuthology repository`: https://github.com/ceph/teuthology
.. _`teuthology framework`: https://github.com/ceph/teuthology

The Ceph community has access to the `Sepia lab
<https://wiki.sepia.ceph.com/doku.php>`_ where `integration tests`_ can be run
on physical hardware.
Other developers may add tags like "needs-qa" to your PR.  This allows PRs that
need testing to be merged into a single branch and tested all at the same time.
Since teuthology suites can take hours (even days in some cases) to run, this
can save a lot of time.

To request access to the Sepia lab, start `here
<https://wiki.sepia.ceph.com/doku.php?id=vpnaccess>`_.

Integration testing is discussed in more detail in the `integration
tests`_ chapter.

.. _integration tests: ../testing_integration_tests/tests-integration-testing-teuthology-intro

Code review
-----------

Once your bugfix has been thoroughly tested, or even during this process,
it will be subjected to code review by other developers. This typically
takes the form of comments in the PR itself, but can be supplemented
by discussions on :ref:`irc` and the :ref:`mailing-list`.

Amending your PR
----------------

While your PR is going through testing and `Code Review`_, you can
modify it at any time by editing files in your local branch.

After updates are committed locally (to the ``fix_1`` branch in our
example), they need to be pushed to GitHub so they appear in the PR.

Modifying the PR is done by adding commits to the ``fix_1`` branch upon
which it is based, often followed by rebasing to modify the branch's git
history. See `this tutorial
<https://www.atlassian.com/git/tutorials/rewriting-history>`_ for a good
introduction to rebasing. When you are done with your modifications, you
will need to force push your branch with:

.. prompt:: bash $

   git push --force origin fix_1

Why do we take these extra steps instead of simply adding additional commits
the the PR?  It is best practice for a PR to consist of a single commit; this
makes for clean history, eases peer review of your changes, and facilitates
merges.  In rare circumstances it also makes it easier to cleanly revert
changes.

Merging
-------

The bugfix process completes when a project lead merges your PR.

When this happens, it is a signal for you (or the lead who merged the PR)
to change the :ref:`issue-tracker` status to "Resolved". Some issues may be
flagged for backporting, in which case the status should be changed to
"Pending Backport" (see the :ref:`backporting` chapter for details).

See also :ref:`merging` for more information on merging.

Proper Merge Commit Format
^^^^^^^^^^^^^^^^^^^^^^^^^^

This is the most basic form of a merge commit::

       doc/component: title of the commit 

       Reviewed-by: Reviewer Name <rname@example.com>

This consists of two parts:

#. The title of the commit / PR to be merged.
#. The name and email address of the reviewer. Enclose the reviewer's email 
   address in angle brackets.

Using .githubmap to Find a Reviewer's Email Address
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If you cannot find the email address of the reviewer on his or her GitHub
page, you can look it up in the **.githubmap** file, which can be found in
the repository at **/ceph/.githubmap**.

Using "git log" to find a Reviewer's Email Address
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If you cannot find a reviewer's email address by using the above methods, you
can search the git log for their email address. Reviewers are likely to have
committed something before.  If they have made previous contributions, the git
log will probably contain their email address.

Use the following command

.. prompt:: bash [branch-under-review]$

   git log

Using ptl-tool to Generate Merge Commits
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Another method of generating merge commits involves using Patrick Donnelly's
**ptl-tool** pull commits. This tool can be found at
**/ceph/src/script/ptl-tool.py**.  Merge commits that have been generated by
the **ptl-tool** have the following form::

     Merge PR #36257 into master
     * refs/pull/36257/head:
             client: move client_lock to _unmount()
             client: add timer_lock support
     Reviewed-by: Patrick Donnelly <pdonnell@redhat.com>
