==========================
Submitting Patches to Ceph
==========================

Patches to Ceph can be divided into three categories:

    1. patches targeting Ceph kernel code
    2. patches targeting the "master" branch
    3. patches targeting stable branches (e.g.: "nautilus")

Some parts of Ceph - notably the RBD and CephFS kernel clients - are maintained
within the Linux Kernel. For patches targeting this code, please refer to the
file ``SubmittingPatches-kernel.rst``.

The rest of this document assumes that your patch relates to Ceph code that is
maintained in the GitHub repository https://github.com/ceph/ceph

If you have a patch that fixes an issue, feel free to open a GitHub pull request
("PR") targeting the "master" branch, but do read this document first, as it
contains important information for ensuring that your PR passes code review
smoothly.

For patches targeting stable branches (e.g. "nautilus"), please also see
the file ``SubmittingPatches-backports.rst``.

.. contents::
   :depth: 3


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

Git can sign off on your behalf
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Please note that git makes it trivially easy to sign commits. First, set the
following config options::

    $ git config --list | grep user
    user.email=my_real_email_address@example.com
    user.name=My Real Name

Then just remember to use ``git commit -s``. Git will add the ``Signed-off-by``
line automatically.


Separate your changes
---------------------

Group *logical changes* into individual commits.

If you have a series of bulleted modifications, consider separating each of
those into its own commit.

For example, if your changes include both bug fixes and performance enhancements
for a single component, separate those changes into two or more commits. If your
changes include an API update, and a new feature which uses that new API,
separate those into two patches.

On the other hand, if you make a single change that affects numerous
files, group those changes into a single commit. Thus a single logical change is
contained within a single patch. (If the change needs to be backported, that
might change the calculus, because smaller commits are easier to backport.)


Describe your changes
---------------------

Each commit has an associated commit message that is stored in git. The first
line of the commit message is the `commit title`_. The second line should be
left blank. The lines that follow constitute the `commit message`_.

A commit and its message should be focused around a particular change.

Commit title
^^^^^^^^^^^^

The text up to the first empty line in a commit message is the commit
title. It should be a single short line of at most 72 characters,
summarizing the change, and prefixed with the
subsystem or module you are changing. Also, it is conventional to use the
imperative mood in the commit title. Positive examples include::

     mds: add perf counter for finisher of MDSRank
     osd: make the ClassHandler::mutex private

More positive examples can be obtained from the git history of the ``master``
branch::

     git log

Some negative examples (how *not* to title a commit message)::

     update driver X
     bug fix for driver X
     fix issue 99999

Further to the last negative example ("fix issue 99999"), see `Fixes line`_.

Commit message
^^^^^^^^^^^^^^

(This section is about the body of the commit message. Please also see
the preceding section, `Commit title`_, for advice on titling commit messages.)

In the body of your commit message, be as specific as possible. If the commit
message title was too short to fully state what the commit is doing, use the
body to explain not just the "what", but also the "why".

For positive examples, peruse ``git log`` in the ``master`` branch. A negative
example would be a commit message that merely states the obvious. For example:
"this patch includes updates for subsystem X. Please apply."

.. _`fixes line`:

Fixes line(s)
^^^^^^^^^^^^^

If the commit fixes one or more issues tracked by http://tracker.ceph.com,
add a ``Fixes:`` line (or lines) to the commit message, to connect this change
to addressed issue(s) - for example::

     Fixes: http://tracker.ceph.com/issues/12345

This line should be added just before the ``Signed-off-by:`` line (see `Sign
your work`_).

It helps reviewers to get more context of this bug and facilitates updating of
the bug tracker. Also, anyone perusing the git history will see this line and be
able to refer to the bug tracker easily.

Here is an example showing a properly-formed commit message::

     doc: add "--foo" option to bar

     This commit updates the man page for bar with the newly added "--foo"
     option.

     Fixes: http://tracker.ceph.com/issues/12345
     Signed-off-by: Random J Developer <random@developer.example.org>

If a commit fixes a regression introduced by a different commit, please also
(in addition to the above) add a line referencing the SHA1 of the commit that
introduced the regression. For example::

     Fixes: 9dbe7a003989f8bb45fe14aaa587e9d60a392727


PR best practices
-----------------

PRs should be opened on branches contained in your fork of
https://github.com/ceph/ceph.git - do not push branches directly to
``ceph/ceph.git``.

PRs should target "master". If you need to add a patch to a stable branch, such
as "nautilus", see the file ``SubmittingPatches-backports.rst``.

In addition to a base, or "target" branch, PRs have several other components:
the `PR title`_, the `PR description`_, labels, comments, etc. Of these, the PR
title and description are relevant for new contributors.

PR title
^^^^^^^^

If your PR has only one commit, the PR title can be the same as the commit title
(and GitHub will suggest this). If the PR has multiple commits, do not accept
the title GitHub suggest. Either use the title of the most relevant commit, or
write your own title. In the latter case, use the same "subsystem: short
description" convention described in `Commit title`_ for the PR title, with
the following difference: the PR title describes the entire set of changes,
while the `Commit title`_ describes only the changes in a particular commit. 

Keep in mind that the PR titles feed directly into the script that generates
release notes and it is tedious to clean up non-conformant PR titles at release
time. This document places no limit on the length of PR titles, but be aware
that they are subject to editing as part of the release process.

PR description
^^^^^^^^^^^^^^

In addition to a title, the PR also has a description field, or "body". 

The PR description is a place for summarizing the PR as a whole. It need not
duplicate information that is already in the commit messages. It can contain
notices to maintainers, links to tracker issues and other related information,
to-do lists, etc. The PR title and description should give readers a high-level
notion of what the PR is about, quickly enabling them to decide whether they
should take a closer look.


Flag your changes for backport
------------------------------

If you believe your changes should be backported to stable branches after the PR
is merged, open a tracker issue at https://tracker.ceph.com explaining:

1. what bug is fixed
2. why does the bug need to be fixed in <release>

and fill out the Backport field in the tracker issue. For example::

    Backport: mimic, nautilus

For information on how backports are done in the Ceph project, refer to the
document ``SubmittingPatches-backports.rst``.


Test your changes
-----------------

Before opening your PR, it's a good idea to run tests on your patchset. Doing
that is simple, though the process can take a long time to complete, especially
on older machines with less memory and spinning disks.

The most simple test is to verify that your patchset builds, at least in your
own development environment. The commands for this are::

    ./install-deps.sh
    ./do_cmake.sh
    make

Ceph comes with a battery of tests that can be run on a single machine. These
are collectively referred to as "make check", and can be run by executing the
following command::

    ./run-make-check.sh

If your patchset does not build, or if one or more of the "make check" tests
fails, but the error shown is not obviously related to your patchset, don't let
that dissuade you from opening a PR. The Ceph project has a Jenkins instance
which will build your PR branch and run "make check" on it in a controlled
environment.

Once your patchset builds and passes "make check", you can run even more tests
on it by issuing the following commands::

    cd build
    ../qa/run-standalone.sh

Like "make check", the standalone tests take a long time to run. They also
produce voluminous output. If one or more of the standalone tests fails, it's
likely the relevant part of the output will have scrolled off your screen or
gotten swapped out of your buffer. Therefore, it makes sense to capture the
output in a file for later analysis.


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
