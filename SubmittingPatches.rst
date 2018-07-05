==========================
Submitting Patches to Ceph
==========================

Patches to Ceph can be divided into three categories:

    1. patches targeting Ceph kernel code
    2. patches targeting the "master" branch
    3. patches targeting stable branches (e.g.: "nautilus")

Some parts of Ceph - notably RBD and CephFS client code - are maintained within
the Linux Kernel. For patches targeting this code, please refer to the file
"SubmittingPatches-kernel.rst"

The rest of this document assumes that your patch relates to Ceph code that is
maintained in the GitHub repository https://github.com/ceph/ceph

If you have a patch that fixes an issue, feel free to open a GitHub pull request
("PR") targeting the "master" branch, but do read this document first, as it
contains important information for ensuring that your PR passes code review
smoothly.

For patches targeting stable branches (e.g. "nautilus"), please also see
the file "SubmittingPatches-backports.rst".

.. contents::
   :depth: 3

.. _sign_your_work:

Sign your work
--------------

The sign-off is a simple line at the end of the explanation for the
commit, which certifies that you wrote it or otherwise have the right to
pass it on as a open-source patch. The rules are pretty simple: if you
can certify the below:

Developer's Certificate of Origin 1.1
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By making a contribution to this project, I certify that:

   (a) The contribution was created in whole or in part by me and I
       have the right to submit it under the open source license
       indicated in the file; or

   (b) The contribution is based upon previous work that, to the best
       of my knowledge, is covered under an appropriate open source
       license and I have the right under that license to submit that
       work with modifications, whether created in whole or in part
       by me, under the same open source license (unless I am
       permitted to submit under a different license), as indicated
       in the file; or

   (c) The contribution was provided directly to me by some other
       person who certified (a), (b) or (c) and I have not modified
       it.

   (d) I understand and agree that this project and the contribution
       are public and that a record of the contribution (including all
       personal information I submit with it, including my sign-off) is
       maintained indefinitely and may be redistributed consistent with
       this project or the open source license(s) involved.

then you just add a line saying ::

        Signed-off-by: Random J Developer <random@developer.example.org>

using your real name (sorry, no pseudonyms or anonymous contributions.)

Please note that git makes it trivially easy to sign commits. First, set the
following config options::

    $ git config --list | grep user
    user.email=my_real_email_address@example.com
    user.name=My Real Name

Then just remember to use ``git commit -s``. Git will add the ``Signed-off-by``
line automatically.


.. _split_changes:

Separate your changes
---------------------

Group *logical changes* into individual commits.

For example, if your changes include both bug fixes and performance enhancements
for a single component, separate those changes into two or more commits. If your
changes include an API update, and a new feature which uses that new API,
separate those into two patches.

On the other hand, if you make a single change that affects numerous
files, group those changes into a single commit. Thus a single logical change is
contained within a single patch.


.. _title_of_commit:

Title of commit
---------------

The text up to the first empty line in a commit message is the commit
title. It should be a single short line of at most 72 characters,
summarizing the change, and prefixed with the
subsystem or module you are changing. For example::

     mds: add perf counter for finisher of MDSRank

More examples can be obtained from the git history of the master branch::

     git log

It is also conventional to use the imperative mood in the commit title. For
example::

     osd: make the ClassHandler::mutex private


.. _pr_best_practices:

PR best practices
-----------------

PRs should be opened on branches contained in your fork of
https://github.com/ceph/ceph.git - do not push branches directly to
``ceph/ceph.git``.

PRs should target "master". If you need to add a patch to a stable branch, such
as "nautilus", see the file "SubmittingPatches-backports.rst".

The section :ref:`_title_of_commit` explains that commit message
titles should look like this::

    subsystem: short description

Use this "subsystem: short description" convention not just for the individual
commit titles, but also for the title of the PR itself. (The PR titles feed
directly into the script that generates release notes and it is tedious to clean
up non-conformant PR titles at release time.  This document places no limit on
the length of PR titles, but be aware that they are subject to editing as part
of the release process.)

 you believe your changes should be backported to stable branches after the PR
is merged, open a tracker issue at https://tracker.ceph.com explaining what the
PR is doing (i.e., the bug being fixed or the feature being added), and fill out
the Backport field in the tracker issue. For example::

    Backport: mimic, nautilus

For information on how backports are done in the Ceph project, refer to the
document SubmittingChanges-backports.rst.


.. _fixes_line:

Fixes line(s)
-------------

If the commit fixes one or more issues tracked by http://tracker.ceph.com,
add a ``Fixes:`` line (or lines) to the commit message, to connect this change
to addressed issue(s) - for example::

     Fixes: http://tracker.ceph.com/issues/12345

This line should be added just before the ``Signed-off-by:`` line.

It helps the reviewer to get more context of this bug, so she/he can hence
update the issue on the bug tracker accordingly.

So a typical commit message for revising the document could look like::

     doc: add "--foo" option to bar

     * update the man page for bar with the newly added "--foo" option.
     * fix a typo

     Fixes: http://tracker.ceph.com/issues/12345
     Signed-off-by: Random J Developer <random@developer.example.org>

If a commit fixes a regression introduced by a different commit, please
add a line referencing the SHA1 of the commit that introduced the regression.
For example::

     Fixes: 9dbe7a003989f8bb45fe14aaa587e9d60a392727


.. _commit_message:

Writing commit messages
-----------------------

(This section is about the body of the commit message. Please also see
:ref:`_title_of_commit` for advice on titling commit messages.)

In the body of your commit message, be as specific as possible. The WORST
descriptions possible include things like "update component X", "bug fix for
component X", or "this patch includes updates for subsystem X.  Please apply."

There's nothing inherently wrong with a long commit message body. But it might
be a sign that the patch could benefit from being split up into smaller patches.
See :ref:`split_changes`.

When you open a GitHub PR with multiple patches, the patches should all be
related in some way and the PR title should state briefly what the patches do
and why they are needed. In other words, the patch series and its description
should be self-contained. This benefits both the patch merger(s) and reviewers.


.. _document_changes:

Document your changes
---------------------

If you have added or modified any user-facing functionality, such as CLI
commands or their output, then the pull request must include appropriate updates
to documentation.

It is the submitter's responsibility to make the changes, and the reviewer's
responsibility to make sure they are not merging changes that do not 
have the needed updates to documentation.

Where there are areas that have absent documentation, or there is no clear place
to note the change that is being made, the reviewer should contact the component
lead, who should arrange for the missing section to be created with sufficient
detail for the PR submitter to document their changes.

When writing and/or editing documentation, follow the Google Developer
Documentation Style Guide: https://developers.google.com/style/
