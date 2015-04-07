=====================
Development workflows
=====================

This page explains the workflows a developer is expected to follow to
implement the goals that are part of the Ceph lifecycle. It does not
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
* the reviewers know a release is being published are unlikely to
  merge anything that may cause issues

This ad-hoc approach implies the workflows are changed on a regular
basis to adapt. For instance, ``quality engineers`` were not involved
in the workflow to publish dumpling point releases. The number of
commits being backported to firefly made it impractical for developers
tasked to write code or fix bugs to also run and verify the full suite
of integration tests. Inserting ``quality engineers`` makes it
possible for someone to participate in the workflow by analyzing test
results.

The workflows are not enforced when they impose an overhead that does
not make sense. For instance, if the release notes for a point release
were not written prior to checking all integration tests, they can be
commited to the stable branch and the result sent for publication
without going through another run of integraiton tests.

Lifecycle
=========

::

    Ceph              hammer                             infernalis
    Developer          CDS                                  CDS 
    Summit              |                                    |
                        |                                    |
    development         |                                    |
    release             |  v0.88  v0.89  v0.90   ...         |  v0.95
                   --v--^----^--v---^------^--v-     ---v----^----^---  2015       
                     |          |             |         |
    stable         giant        |             |      hammer
    release        v0.87        |             |      v0.94
                                |             |          
    point                    firefly       dumpling
    release                  v0.80.8       v0.67.12


Four times a year, the development roadmap is discussed online during
the `Ceph Developer Summit <http://wiki.ceph.com/Planning/CDS/>`_. A
new stable release (argonaut, cuttlefish, dumpling, emperor, firefly,
giant, hammer ...) is published at the same frequency. Every other
release is a long time support (dumpling, firefly, hammer, ...) which
means point releases are published more often. In 2014 point releases
(i.e. dumpling 0.67.11, dumpling 0.67.12 ...) are published up to
eighteen months after the publication of a stable release. Once or
twice a month, a development release is published.

Merging bug fixes or features
=============================

The development branch is ``master`` and the workflow followed by all
developers can be summarized as follows:

* The developer prepares a series of commits
* The developer submits the series of commits via a pull request
* A reviewer is assigned the pull request
* When the pull request looks good to the reviewer, it is merged into
  an integration branch by the tester
* After a successfull run of integration tests, the pull request is
  merged by the tester

The ``developer`` is the author of a series of commits. The
``reviewer`` is responsible for providing feedback to the developer on
a regular basis and the developer is invited to ping the reviewer if
nothing happened after a week. After the ``reviewer`` is satisfied
with the pull request, (s)he passes it to the ``tester``. The
``tester`` is responsible for running teuthology integration tests on
the pull request. If nothing happens within a month the reviewer is
invited to ping the tester.

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

Each ``team`` is responsible for a project:

* rgw lead is Yehuda Sadeh
* CephFS lead is Gregory Farnum
* rados lead is Samuel Just
* rbd lead is Josh Durgin

The ``developer`` assigned to an issue is responsible for it. The
status of an open issue can be:

* ``New``: it is unclear if the issue needs work.
* ``Verified``: the bug can be reproduced or showed up multiple times
* ``In Progress``: the developer is working on it this week
* ``Pending Backport``: the fix needs to be backported to the stable
  releases listed in the backport field

Running and interpreting teuthology integration tests
=====================================================

The :doc:`/dev/sepia` runs `teuthology
<https://github.com/ceph/teuthology/>`_ integration tests and the
results are posted on `pulpito <http://pulpito.ceph.com/>`_ and the
`ceph-qa mailing list <http://ceph.com/resources/mailing-list-irc/>`_.

* The quality engineer analyzes the integration job failure
* If the cause is environmental (e.g. network connectivity), an issue
  is created in the `sepia lab project
  <http://tracker.ceph.com/projects/lab/issues/new>`_
* If the bug is known a pulpito URL to failed job is added to the issue
* If the bug is new, an issue is created

The ``quality engineer`` is either a developer or a member of the QE
team. There is at least one integration test suite per project:

* `rgw <https://github.com/ceph/ceph-qa-suite/tree/master/suites/rgw>`_ suite
* `CephFS <https://github.com/ceph/ceph-qa-suite/tree/master/suites/fs>`_ suite
* `rados <https://github.com/ceph/ceph-qa-suite/tree/master/suites/rados>`_ suite
* `rbd <https://github.com/ceph/ceph-qa-suite/tree/master/suites/rbd>`_ suite

and a many others such as

* `upgrade <https://github.com/ceph/ceph-qa-suite/tree/master/suites/upgrade>`_ suites
* `power-cyle <https://github.com/ceph/ceph-qa-suite/tree/master/suites/powercycle>`_ suite
* ...

Preparing a new release
=======================

A release is prepared in a dedicated branch, different from the
``master`` branch.

* For a stable releases it is the branch matching the release code
  name (dumpling, firefly, etc.)
* For a development release it is the ``next`` branch

The workflow expected of all developers to stabilize the release
candidate is the same as the normal development workflow with the
following differences:

* The pull requests must target the stable branch or next instead of
  master
* The reviewer rejects pull requests that are not bug fixes
* The issues matching a teuthology integration test failure is set
  with severity ``Critical`` if it must be fixed before the release

Cutting a new stable release
============================

When all bugs with severity ``Critical`` are fixed for the release
being prepared, it is published as follows:

* The ``release master`` gets approval from all leads
* The ``release master`` writes and commits the release notes
* The ``release master`` informs the ``quality engineer`` that the
  branch is ready for testing
* The ``quality engineer`` runs additional integration tests
* If the ``quality engineer`` discovers new bugs with severity
  ``Critical``, the relase goes back to being prepared, it was not
  ready after all
* The ``quality engineer`` informs the ``publisher`` that the branch
  is ready for release
* The ``publisher`` `creates the packages and sets the release tag
  <../release-process>`_

The person responsible for each role is:

* Sage Weil is the ``release master`` for major stable releases
  (Firefly 0.80, Giant 0.87 etc.)
* Loic Dachary is the ``release master`` for stable point releases
  (Dumpling 0.68.12, Giant 0.87.1 etc.)
* Yuri Weinstein is the ``quality engineer``
* Alfredo Deza is the ``publisher``

Cutting a new development release
=================================

The publication workflow of a development release is the same as
preparing a new release and cutting it, with the following
differences:

* The ``next`` branch is reset to the tip of ``master`` after
  publication
* The ``quality engineer`` is not required to run additional tests,
  the ``release master`` directly informs the ``publisher`` that the
  release is ready to be published.

Publishing point releases and backporting
=========================================

The publication workflow of the point releases is the same as
preparing a new release and cutting it, with the following
differences:

* The ``backport`` field of each issue contains the code name of the
  stable release
* All commits are cherry-picked with ``git cherry-pick -x`` to
  reference the original commit
