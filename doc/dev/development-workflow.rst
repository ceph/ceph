=====================
Development workflows
=====================

This page explains the workflows a developer is expected to follow to
implement the goals that are part of the Ceph release cycle. It does not
go into technical details and is designed to provide a high level view
instead. Each chapter is about a given goal such as ``Merging bug
fixes or features`` or ``Publishing point releases and backporting``.

A key aspect of all workflows is that none of them blocks another. For
instance, a bug fix can be backported and merged to a stable branch
while the next point release is being published. For that specific
example to work, a branch should be created to avoid any
interference. In practice it is not necessary for Ceph because:

* there are few people involved
* the frequency of backports is not too high
* the reviewers, who know a release is being published, are unlikely
  to merge anything that may cause issues

This ad-hoc approach implies the workflows are changed on a regular
basis to adapt. For instance, ``quality engineers`` were not involved
in the workflow to publish ``dumpling`` point releases. The number of
commits being backported to ``firefly`` made it impractical for developers
tasked to write code or fix bugs to also run and verify the full suite
of integration tests. Inserting ``quality engineers`` makes it
possible for someone to participate in the workflow by analyzing test
results.

The workflows are not enforced when they impose an overhead that does
not make sense. For instance, if the release notes for a point release
were not written prior to checking all integration tests, they can be
committed to the stable branch and the result sent for publication
without going through another run of integration tests.


Release Cycle
=============

::

    Ceph              squid                              tentacle
    Developer          CDS                                  CDS 
    Summit              |                                    |
                        |                                    |
    development         |                                    |
    release             | v19.0.0                ...         | v20.0.0
                   --v--^----^--v---^------^--v-     ---v----^----^---  2024       
                     |          |             |         |
    stable         reef         |             |       squid
    release        v18.2.0      |             |       v19.2.0
                                |             |
    point                    quincy          reef
    release                  v17.2.7         v18.2.1


A new stable release (quincy, reef, squid ...) is published at the same
frequency. Every release (quincy, reef, squid...) is a `Long Term Stable (LTS)
<../../releases>`_.  See `Understanding the release cycle
<../../releases#understanding-the-release-cycle>`_ for more information.


Merging bug fixes or features
=============================

The development branch is ``main`` and the workflow followed by all
developers can be summarized as follows:

* The developer prepares a series of commits
* The developer submits the series of commits via a pull request
* A reviewer is assigned the pull request
* When the pull request looks good to the reviewer, it is merged into
  an integration branch by the tester
* After a successful run of integration tests, the pull request is
  merged by the tester

The ``developer`` is the author of a series of commits. The
``reviewer`` is responsible for providing feedback to the developer on
a regular basis and the developer is invited to ping the reviewer if
nothing happened after a week. After the ``reviewer`` is satisfied
with the pull request, (s)he passes it to the ``tester``. The
``tester`` is responsible for running teuthology integration tests on
the pull request. If nothing happens within a month the ``reviewer`` is
invited to ping the ``tester``.

Resolving bug reports and implementing features
===============================================

All bug reports and feature requests are in the `issue tracker
<https://tracker.ceph.com>`_ and the workflow can be summarized as
follows:

* The reporter creates the issue with priority ``Normal``
* A developer may pick the issue right away
* During a bi-weekly bug scrub, the team goes over all new issue and
  assign them a priority
* The bugs with higher priority are worked on first

Each ``team`` is responsible for a project, managed by :ref:`leads <governance>`.

The ``developer`` assigned to an issue is responsible for it. The
status of an open issue can be:

* ``New``: it is unclear if the issue needs work.
* ``Verified``: the bug can be reproduced or showed up multiple times
* ``In Progress``: the developer is working on it this week
* ``Pending Backport``: the fix needs to be backported to the stable
  releases listed in the backport field

For each ``Pending Backport`` issue, there exists at least one issue in the
``Backport`` tracker to record the work done to cherry pick the necessary
commits from the main branch to the target stable branch. See `the backporter
manual
<https://github.com/ceph/ceph/blob/main/SubmittingPatches-backports.rst>`_ for
more information.

Running and interpreting teuthology integration tests
=====================================================

The :doc:`/dev/sepia` runs `teuthology
<https://github.com/ceph/teuthology/>`_ integration tests `on a regular basis <https://tracker.ceph.com/projects/ceph-releases/wiki/HOWTO_monitor_the_automated_tests_AKA_nightlies#Automated-tests-AKA-nightlies>`_ and the
results are posted on `pulpito <https://pulpito.ceph.com/>`_ and the
`ceph-qa mailing list <https://ceph.com/irc/>`_.

* The job failures are `analyzed by quality engineers and developers
  <https://tracker.ceph.com/projects/ceph-releases/wiki/HOWTO_monitor_the_automated_tests_AKA_nightlies#List-of-suites-and-watchers>`_
* If the cause is environmental (e.g. network connectivity), an issue
  is created in the `sepia lab project
  <https://tracker.ceph.com/projects/lab/issues/new>`_
* If the bug is known, a pulpito URL to the failed job is added to the issue
* If the bug is new, an issue is created

The ``quality engineer`` is either a developer or a member of the QE
team. There is at least one integration test suite per project:

* `rgw <https://github.com/ceph/ceph/tree/main/qa/suites/rgw>`_ suite
* `CephFS <https://github.com/ceph/ceph/tree/main/qa/suites/fs>`_ suite
* `rados <https://github.com/ceph/ceph/tree/main/qa/suites/rados>`_ suite
* `rbd <https://github.com/ceph/ceph/tree/main/qa/suites/rbd>`_ suite

and many others such as

* `upgrade <https://github.com/ceph/ceph/tree/main/qa/suites/upgrade>`_ suites
* `power-cycle <https://github.com/ceph/ceph/tree/main/qa/suites/powercycle>`_ suite
* ...

Preparing a new release
=======================

A release is prepared in a dedicated branch, different from the
``main`` branch.

* For a stable release it is the branch matching the release code
  name (quincy, reef, etc.)
* For a development release it is the ``next`` branch

The workflow expected of all developers to stabilize the release
candidate is the same as the normal development workflow with the
following differences:

* The pull requests must target the stable branch or next instead of
  main
* The reviewer rejects pull requests that are not bug fixes
* The ``Backport`` issues matching a teuthology test failure and set
  with priority ``Urgent`` must be fixed before the release

Cutting a new stable release
============================

A new stable release can be cut when:

* all ``Backport`` issues with priority ``Urgent`` are fixed
* integration and upgrade tests run successfully

Publishing a new stable release implies a risk of regression or
discovering new bugs during the upgrade, no matter how carefully it is
tested. The decision to cut a release must take this into account: it
may not be wise to publish a stable release that only fixes a few
minor bugs. For instance if only one commit has been backported to a
stable release that is not a LTS, it is better to wait until there are
more.

When a stable release is to be retired, it may be safer to
recommend an upgrade to the next LTS release instead of
proposing a new point release to fix a problem. For instance, the
``quincy`` v17.2.6 release might have bugs related to backfilling which have
been fixed in ``reef`` v18.2.x. A backport fixing these backfilling
bugs might be tested in a draft point release but if
they are large enough to introduce a risk of regression and the older release
is to be retired, users suffering from this bug can
upgrade to the newer LTS to fix it.

* The ``Ceph lead`` decides a new stable release must be published
* The ``release master`` gets approval from all leads
* The ``release master`` writes and commits the release notes
* The ``release master`` informs the ``quality engineer`` that the
  branch is ready for testing
* The ``quality engineer`` runs additional integration tests
* If the ``quality engineer`` discovers new bugs that require an
  ``Urgent Backport``, the release goes back to being prepared, it
  was not ready after all
* The ``quality engineer`` informs the ``publisher`` that the branch
  is ready for release
* The ``publisher`` `creates the packages and sets the release tag
  <../release-process>`_

The person responsible for each role is:

* The ``Ceph lead`` oversees the overall release schedule.
* The ``release master`` for major stable releases ensures releases meet criteria.
* The ``release master`` for stable point releases coordinates backports and publishes point releases.
* The ``quality engineer`` verifies integration tests.
* The ``publisher`` publishes the release artifacts.

Cutting a new development release
=================================

The publication workflow of a development release is the same as
preparing a new release and cutting it, with the following
differences:

* The ``next`` branch is reset to the tip of ``main`` after
  publication
* The ``quality engineer`` is not required to run additional tests,
  the ``release master`` directly informs the ``publisher`` that the
  release is ready to be published.

Automated Backport Auditing
===========================

All backport PRs undergo automated hygiene checks by the `releng-audit` GitHub Actions CI to ensure they meet strict quality standards. These checks include:

* **Commit Parity:** Ensuring all commits in the backport accurately map to the original ``main`` PRs.
* **Conflict Simulation:** Dry-running cherry-picks to detect undocumented conflicts or deviations.
* **Redmine Linkage:** Verifying accurate tracking and release targeting in the issue tracker.

Passing these checks automatically applies the ``releng-audit-pass`` label, which is required for the PR to be merged. If the audit fails, the ``releng-audit-fail`` label is applied and an "anti-spam" push shield halts further automated checks. After fixing the issues, you must manually remove the ``releng-audit-fail`` label or comment ``/audit retest`` to trigger a new audit.

If the CI correctly flags a valid deviation (e.g., a complex but necessary manual conflict resolution), you can request an override. Ping the Component Lead or the ``#ceph-upstream-releases`` Slack channel to request a review. Any user with "maintain" or "admin" rights on the repository, or a member of the ``@ceph/ceph-release-manager`` team, can bypass the check by commenting ``/audit override`` or manually applying the ``releng-audit-override`` label.

For the granular technical requirements and rules, see the `Cherry-picking rules <https://github.com/ceph/ceph/blob/main/SubmittingPatches-backports.rst#cherry-picking-rules>`_ in the backporter manual.

Publishing point releases and backporting
=========================================

The publication workflow of the point releases is the same as
preparing a new release and cutting it, with the following
differences:

* The ``backport`` field of each issue contains the code name of the
  stable release
* There is exactly one issue in the ``Backport`` tracker for each
  stable release to which the issue is backported
* All commits are cherry-picked with ``git cherry-pick -x`` to
  reference the original commit

.. note:: If a backport is appropriate, the submitter is responsible for
   determining appropriate target stable branches to which backports must be
   made.

See `the backporter manual
<https://github.com/ceph/ceph/blob/main/SubmittingPatches-backports.rst>`_ for
more information.
