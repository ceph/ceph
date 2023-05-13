.. _basic workflow dev guide:

Basic Workflow
==============

The following chart illustrates the basic Ceph development workflow:

.. ditaa::

            Upstream Code                       Your Local Environment

           /----------\        git clone           /-------------\
           |   Ceph   | -------------------------> | ceph/main   |
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
an enhancement, but you do not know how to proceed. Watch the `Getting Started
with Ceph Development <https://www.youtube.com/watch?v=t5UIehZ1oLs>`_ video for
a practical summary of this workflow.

Updating the tracker
--------------------

Find the :ref:`issue-tracker` (Redmine) number of the bug you intend to fix. If
no tracker issue exists, create one. There is only one case in which you do not
have to create a Redmine tracker issue: the case of minor documentation changes.

Simple documentation cleanup does not require a corresponding tracker issue.
Major documentation changes do require a tracker issue. Major documentation
changes include adding new documentation chapters or files, and making 
substantial changes to the structure or content of the documentation.

A (Redmine) tracker ticket explains the issue (bug) to other Ceph developers to
keep them informed as the bug nears resolution. Provide a useful, clear title
and include detailed information in the description. When composing the title
of the ticket, ask yourself "If I need to search for this ticket two years from
now, which keywords am I likely to search for?" Then include those keywords in
the title.

If your tracker permissions are elevated, assign the bug to yourself by setting
the ``Assignee`` field. If your tracker permissions have not been elevated,
just add a comment with a short message that says "I am working on this issue".

Ceph Workflow Overview
----------------------

Three repositories are involved in the Ceph workflow. They are:

1. The upstream repository (ceph/ceph)
2. Your fork of the upstream repository (your_github_id/ceph)
3. Your local working copy of the repository (on your workstation)

The procedure for making changes to the Ceph repository is as follows:

#. Configure your local environment

   #. :ref:`Create a fork<forking>` of the "upstream Ceph"
      repository.

   #. :ref:`Clone the fork<cloning>` to your local filesystem.

#. Fix the bug

   #. :ref:`Synchronize local main with upstream main<synchronizing>`.
         
   #. :ref:`Create a bugfix branch<bugfix_branch>` in your local working copy.
         
   #. :ref:`Make alterations to the local working copy of the repository in your
      local filesystem<fixing_bug_locally>`.
   
   #. :ref:`Push the changes in your local working copy to your fork<push_changes>`.

#. Create a Pull Request to push the change upstream.

   #. Create a Pull Request that asks for your changes to be added into the
      "upstream Ceph" repository.

Preparing Your Local Working Copy of the Ceph Repository 
--------------------------------------------------------

The procedures in this section, "Preparing Your Local Working Copy of the Ceph
Repository", must be followed only when you are first setting up your local
environment. If this is your first time working with the Ceph project, then
these commands are necessary and are the first commands that you should run.

.. _forking:

Creating a Fork of the Ceph Repository
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See the `GitHub documentation
<https://help.github.com/articles/fork-a-repo/#platform-linux>`_ for
detailed instructions on forking. In short, if your GitHub username is
"mygithubaccount", your fork of the upstream repo will appear at
``https://github.com/mygithubaccount/ceph``. 

.. _cloning:

Cloning Your Fork  
^^^^^^^^^^^^^^^^^

After you have created your fork, clone it by running the following command:

.. prompt:: bash $

   git clone https://github.com/mygithubaccount/ceph

You must fork the Ceph repository before you clone it.  If you fail to fork,
you cannot open a `GitHub pull request
<https://docs.github.com/en/free-pro-team@latest/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request>`_.

For more information on using GitHub, refer to `GitHub Help
<https://help.github.com/>`_.

Configuring Your Local Environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The commands in this section configure your local git environment so that it
generates "Signed-off-by:" tags. These commands also set up your local
environment so that it can stay synchronized with the upstream repository.

These commands are necessary only during the initial setup of your local
working copy. Another way to say that is "These commands are necessary
only the first time that you are working with the Ceph repository. They are,
however, unavoidable, and if you fail to run them then you will not be able
to work on the Ceph repository.".

1. Configure your local git environment with your name and email address.  

   .. note::
      These commands will work only from within the ``ceph/`` directory
      that was created when you cloned your fork.

   .. prompt:: bash $

      git config user.name "FIRST_NAME LAST_NAME"
      git config user.email "MY_NAME@example.com"

2. Add the upstream repo as a "remote" and fetch it:

   .. prompt:: bash $

      git remote add ceph https://github.com/ceph/ceph.git
      git fetch ceph

   These commands fetch all the branches and commits from ``ceph/ceph.git`` to
   the local git repo as ``remotes/ceph/$BRANCH_NAME`` and can be referenced as
   ``ceph/$BRANCH_NAME`` in local git commands.

Fixing the Bug
--------------

.. _synchronizing:

Synchronizing Local Main with Upstream Main
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In your local working copy, there is a copy of the ``main`` branch in
``remotes/origin/main``. This is called "local main". This copy of the
main branch (https://github.com/your_github_id/ceph.git) is "frozen in time"
at the moment that you cloned it, but the upstream repo
(https://github.com/ceph/ceph.git, typically abbreviated to ``ceph/ceph.git``)
that it was forked from is not frozen in time: the upstream repo is still being
updated by other contributors. 

Because upstream main is continually receiving updates from other
contributors, your fork will drift farther and farther from the state of the
upstream repo when you cloned it.

Keep your fork's ``main`` branch synchronized with upstream main to reduce drift
between your fork's main branch and the upstream main branch.

Here are the commands for keeping your fork synchronized with the
upstream repository:

.. prompt:: bash $

   git fetch ceph
   git checkout main 
   git reset --hard ceph/main
   git push -u origin main

Follow this procedure often to keep your local ``main`` in sync with upstream
``main``.

If the command ``git status`` returns a line that reads "Untracked files", see
:ref:`the procedure on updating submodules <update-submodules>`.

.. _bugfix_branch:

Creating a Bugfix branch
^^^^^^^^^^^^^^^^^^^^^^^^

Create a branch for your bugfix:

.. prompt:: bash $

   git checkout main 
   git checkout -b fix_1
   git push -u origin fix_1

The first command (git checkout main) makes sure that the bugfix branch
"fix_1" is created from the most recent state of the main branch of the
upstream repository. 

The second command (git checkout -b fix_1) creates a "bugfix branch" called
"fix_1" in your local working copy of the repository. The changes that you make
in order to fix the bug will be committed to this branch.

The third command (git push -u origin fix_1) pushes the bugfix branch from
your local working repository to your fork of the upstream repository.

.. _fixing_bug_locally:

Fixing the bug in the local working copy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. **Updating the tracker**

   In the `Ceph issue tracker <https://tracker.ceph.com>`_, change the status
   of the tracker issue to "In progress".  This communicates to other Ceph
   contributors that you have begun working on a fix, which helps to avoid
   duplication of effort. If you don't have permission to change that field,
   just comment that you are working on the issue. 

#. **Fixing the bug itself**

   This guide cannot tell you how to fix the bug that you have chosen to fix.
   This guide assumes that you know what required improvement, and that you
   know what to do to provide that improvement.

   It might be that your fix is simple and requires only minimal testing. But
   that's unlikely. It is more likely that the process of fixing your bug will
   be iterative and will involve trial, error, skill, and patience. 

   For a detailed discussion of the tools available for validating bugfixes,
   see the chapters on testing.

Pushing the Fix to Your Fork
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
   
You have finished work on the bugfix. You have tested the bugfix, and you
believe that it works. 
   
#. Commit the changes to your local working copy.

   Commit the changes to the `fix_1` branch of your local working copy by using
   the ``--signoff`` option (here represented as the `s` portion of the `-as`
   flag): 

   .. prompt:: bash $

      git commit -as

   .. _push_changes:

#. Push the changes to your fork:

   Push the changes from the `fix_1` branch of your local working copy to the
   `fix_1` branch of your fork of the upstream repository:

   .. prompt:: bash $

      git push origin fix_1
   
   .. note::

      In the command ``git push origin fix_1``, ``origin`` is the name of your
      fork of the upstream Ceph repository, and can be thought of as a nickname
      for ``git@github.com:username/ceph.git``, where ``username`` is your
      GitHub username.

      It is possible that ``origin`` is not the name of your fork. Discover the
      name of your fork by running ``git remote -v``, as shown here:

      .. code-block:: bash

         $ git remote -v
         ceph	https://github.com/ceph/ceph.git (fetch)
         ceph	https://github.com/ceph/ceph.git (push)
         origin	git@github.com:username/ceph.git (fetch)
         origin	git@github.com:username/ceph.git (push)

      The line::
     
         origin git@github.com:username/ceph.git (fetch) 
      
      and the line:: 
        
         origin git@github.com:username/ceph.git (push) 
         
      provide the information that "origin" is the name of your fork of the
      Ceph repository.


Opening a GitHub pull request
-----------------------------

After you have pushed the bugfix to your fork, open a GitHub pull request
(PR). This makes your bugfix visible to the community of Ceph contributors.
They will review it. They may perform additional testing on your bugfix, and
they might request changes to the bugfix.

Be prepared to receive suggestions and constructive criticism in the form of
comments within the PR. 

If you don't know how to create and manage pull requests, read `this GitHub
pull request tutorial`_.

.. _`this GitHub pull request tutorial`:
   https://help.github.com/articles/using-pull-requests/

To learn what constitutes a "good" pull request, see
the `Git Commit Good Practice`_ article at the `OpenStack Project Wiki`_.

.. _`Git Commit Good Practice`: https://wiki.openstack.org/wiki/GitCommitMessages
.. _`OpenStack Project Wiki`: https://wiki.openstack.org/wiki/Main_Page

See also our own `Submitting Patches
<https://github.com/ceph/ceph/blob/main/SubmittingPatches.rst>`_ document.

After your pull request (PR) has been opened, update the :ref:`issue-tracker`
by adding a comment directing other contributors to your PR. The comment can be
as simple as this::

    *PR*: https://github.com/ceph/ceph/pull/$NUMBER_OF_YOUR_PULL_REQUEST

Understanding Automated PR validation
-------------------------------------

When you create or update your PR, the Ceph project's `Continuous Integration
(CI) <https://en.wikipedia.org/wiki/Continuous_integration>`_ infrastructure
automatically tests it. At the time of this writing (May 2022), the automated
CI testing included many tests. These five are among them:

#. a test to check that the commits are properly signed (see :ref:`submitting-patches`):
#. a test to check that the documentation builds
#. a test to check that the submodules are unmodified
#. a test to check that the API is in order
#. a :ref:`make check<make-check>` test

Additional tests may be run depending on which files your PR modifies.

The :ref:`make check<make-check>` test builds the PR and runs it through a
battery of tests. These tests run on servers that are operated by the Ceph
Continuous Integration (CI) team. When the tests have completed their run, the
result is shown on GitHub in the pull request itself.

Test your modifications before you open a PR.  Refer to the chapters
on testing for details.

Notes on PR make check test
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The GitHub :ref:`make check<make-check>` test is driven by a Jenkins instance.

Jenkins merges your PR branch into the latest version of the base branch before
it starts any tests. This means that you don't have to rebase the PR in order
to pick up any fixes.

You can trigger PR tests at any time by adding a comment to the PR - the
comment should contain the string "test this please". Since a human who is
subscribed to the PR might interpret that as a request for him or her to test
the PR, you must address Jenkins directly. For example, write "jenkins retest
this please". If you need to run only one of the tests, you can request it with
a command like "jenkins test signed". A list of these requests is automatically
added to the end of each new PR's description, so check there to find the
single test you need.

If there is a build failure and you aren't sure what caused it, check the
:ref:`make check<make-check>` log. To access the make check log, click the
"details" (next to the :ref:`make check<make-check>` test in the PR) link to
enter the Jenkins web GUI. Then click "Console Output" (on the left).

Jenkins is configured to search logs for strings that are known to have been
associated with :ref:`make check<make-check>` failures in the past. However,
there is no guarantee that these known strings are associated with any given
:ref:`make check<make-check>` failure. You'll have to read through the log to
determine the cause of your specific failure.

Integration tests AKA ceph-qa-suite
-----------------------------------

It may be necessary to test your fix on real Ceph clusters that run on physical
or virtual hardware. Tests designed for this purpose live in the `ceph/qa
sub-directory`_ and are run via the `teuthology framework`_.

.. _`ceph/qa sub-directory`: https://github.com/ceph/ceph/tree/main/qa/
.. _`teuthology repository`: https://github.com/ceph/teuthology
.. _`teuthology framework`: https://github.com/ceph/teuthology

The Ceph community has access to the `Sepia lab
<https://wiki.sepia.ceph.com/doku.php>`_ where `integration tests`_ can be run
on physical hardware.

Other contributors might add tags like `needs-qa` to your PR. This allows PRs
to be merged into a single branch and then efficiently tested together.
Teuthology test suites can take hours (and even days in some cases) to
complete, so batching tests reduces contention for resources and saves a lot of
time.

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
the PR?  It is best practice for a PR to consist of a single commit; this
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

Using a browser extension to auto-fill the merge  message
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you use a browser for merging GitHub PRs, the easiest way to fill in
the merge message is with the `"Ceph GitHub Helper Extension"
<https://github.com/tspmelo/ceph-github-helper>`_ (available for `Chrome
<https://chrome.google.com/webstore/detail/ceph-github-helper/ikpfebikkeabmdnccbimlomheocpgkmn>`_
and `Firefox <https://addons.mozilla.org/en-US/firefox/addon/ceph-github-helper/>`_).

After enabling this extension, if you go to a GitHub PR page, a vertical helper
will be displayed at the top-right corner. If you click on the user silhouette button
the merge message input will be automatically populated.

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

     Merge PR #36257 into main 
     * refs/pull/36257/head:
             client: move client_lock to _unmount()
             client: add timer_lock support
     Reviewed-by: Patrick Donnelly <pdonnell@redhat.com>

Miscellaneous
-------------

--set-upstream
^^^^^^^^^^^^^^

If you forget to include the ``--set-upstream origin x`` option in your ``git
push`` command, you will see the following error message:

::

   fatal: The current branch {x} has no upstream branch.
   To push the current branch and set the remote as upstream, use
      git push --set-upstream origin {x}

To set up git to automatically create the upstream branch that corresponds to
the branch in your local working copy, run this command from within the
``ceph/`` directory:

.. prompt:: bash $

   git config --global push.autoSetupRemote true

Deleting a Branch Locally
^^^^^^^^^^^^^^^^^^^^^^^^^

To delete the branch named ``localBranchName`` from the local working copy, run
a command of this form:

.. prompt:: bash $

   git branch -d localBranchName

Deleting a Branch Remotely
^^^^^^^^^^^^^^^^^^^^^^^^^^

To delete the branch named ``remoteBranchName`` from the remote upstream branch
(which is also your fork of ``ceph/ceph``, as described in :ref:`forking`), run
a command of this form:

.. prompt:: bash $

   git push origin --delete remoteBranchName

Searching a File Longitudinally for a String
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To search for the commit that introduced a given string (in this example, that
string is ``foo``) into a given file (in this example, that file is
``file.rst``), run a command of this form:

.. prompt:: bash $

   git log -S 'foo' file.rst
