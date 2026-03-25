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
basis to adapt. For instance, **quality engineers** were not involved
in the workflow to publish ``dumpling`` point releases. The number of
commits being backported to ``firefly`` made it impractical for developers
tasked to write code or fix bugs to also run and verify the full suite
of integration tests. Inserting **quality engineers** makes it
possible for someone to participate in the workflow by analyzing test
results.

The workflows are not enforced when they impose an overhead that does
not make sense. For instance, if the release notes for a point release
were not written prior to checking all integration tests, they can be
committed to the stable branch and the result sent for publication
without going through another run of integration tests.

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
<http://tracker.ceph.com>`_ and the workflow can be summarized as
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
commits from the ``main`` branch to the target stable branch. See `the backporter
manual
<https://github.com/ceph/ceph/blob/main/SubmittingPatches-backports.rst>`_ for
more information.

Running and interpreting teuthology integration tests
=====================================================

The :doc:`/dev/sepia` runs `teuthology
<https://github.com/ceph/teuthology/>`_ integration tests `on a regular basis <http://tracker.ceph.com/projects/ceph-releases/wiki/HOWTO_monitor_the_automated_tests_AKA_nightlies#Automated-tests-AKA-nightlies>`_ and the
results are posted on `pulpito <http://pulpito.ceph.com/>`_ and the
`ceph-qa mailing list <https://ceph.com/irc/>`_.

* The job failures are `analyzed by quality engineers and developers
  <http://tracker.ceph.com/projects/ceph-releases/wiki/HOWTO_monitor_the_automated_tests_AKA_nightlies#List-of-suites-and-watchers>`_
* If the cause is environmental (e.g. network connectivity), an issue
  is created in the `sepia lab project
  <http://tracker.ceph.com/projects/lab/issues/new>`_
* If the bug is known, a pulpito URL to the failed job is added to the issue
* If the bug is new, an issue is created

The **quality engineer** is either a developer or a member of the QE
team. There is at least one integration test suite per project:

* `rgw <https://github.com/ceph/ceph/tree/main/qa/suites/rgw>`_ suite
* `CephFS <https://github.com/ceph/ceph/tree/main/qa/suites/fs>`_ suite
* `rados <https://github.com/ceph/ceph/tree/main/qa/suites/rados>`_ suite
* `rbd <https://github.com/ceph/ceph/tree/main/qa/suites/rbd>`_ suite

and many others such as

* `upgrade <https://github.com/ceph/ceph/tree/main/qa/suites/upgrade>`_ suites
* `powercycle <https://github.com/ceph/ceph/tree/main/qa/suites/powercycle>`_ suite
* ...

Preparing and Cutting a New Release
===================================

The process for preparing and cutting new Ceph releases is orchestrated by the
Ceph Release Manager (CRM). For a detailed breakdown of the release lifecycle,
merge windows, and branch discipline, please see the `Ceph Release Manager
(CRM) <../crm>`_ documentation.

To see the exact steps taken to publish a release, including building, signing,
and artifact generation, refer to the `Ceph Release Process
<release-process>`_ and the `Release Checklists <release-checklists>`_.

The person responsible for the release process is:

* Patrick Donnelly is the `Ceph Release Manager (CRM) <../crm>`_
 
Publishing point releases and backporting
=========================================

The publication workflow of point releases follows the standard release
preparation and cutting process, with the following additions for
backporting:

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
