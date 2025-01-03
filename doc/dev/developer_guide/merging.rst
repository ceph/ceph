.. _merging:

Commit merging:  scope and cadence
==================================

Commits are merged into branches according to criteria specific to each phase
of the Ceph release lifecycle. This chapter codifies these criteria.

Development releases (i.e. x.0.z)
---------------------------------

What ?
^^^^^^

* Features
* Bug fixes

Where ?
^^^^^^^

Features are merged to the *main* branch. Bug fixes should be merged to the
corresponding named branch (e.g. *nautilus* for 14.0.z, *pacific* for 16.0.z,
etc.). However, this is not mandatory - bug fixes and documentation
enhancements can be merged to the *main* branch as well, since the *main*
branch is itself occasionally merged to the named branch during the development
releases phase. In either case, if a bug fix is important it can also be
flagged for backport to one or more previous stable releases.

When ?
^^^^^^

After each stable release, candidate branches for previous releases enter
phase 2 (see below).  For example: the *jewel* named branch was created when
the *infernalis* release candidates entered phase 2. From this point on,
*main* was no longer associated with *infernalis*. After he named branch of
the next stable release is created, *main* will be occasionally merged into
it.

Branch merges
^^^^^^^^^^^^^

* The latest stable release branch is merged periodically into main.
* The main branch is merged periodically into the branch of the stable release.
* The main is merged into the stable release branch
  immediately after each development (x.0.z) release.

Stable release candidates (i.e. x.1.z) phase 1
----------------------------------------------

What ?
^^^^^^

* Bug fixes only

Where ?
^^^^^^^

The stable release branch (e.g. *jewel* for 10.0.z, *luminous*
for 12.0.z, etc.) or *main*.  Bug fixes should be merged to the named
branch corresponding to the stable release candidate (e.g. *jewel* for
10.1.z) or to *main*. During this phase, all commits to *main* will be
merged to the named branch, and vice versa. In other words, it makes
no difference whether a commit is merged to the named branch or to
*main* - it will make it into the next release candidate either way.

When ?
^^^^^^

After the first stable release candidate is published, i.e. after the
x.1.0 tag is set in the release branch.

Branch merges
^^^^^^^^^^^^^

* The stable release branch is merged periodically into *main*.
* The *main* branch is merged periodically into the stable release branch.
* The *main* branch is merged into the stable release branch
  immediately after each x.1.z release candidate.

Stable release candidates (i.e. x.1.z) phase 2
----------------------------------------------

What ?
^^^^^^

* Bug fixes only

Where ?
^^^^^^^

The stable release branch (e.g. *mimic* for 13.0.z, *octopus* for 15.0.z
,etc.). During this phase, all commits to the named branch will be merged into
*main*. Cherry-picking to the named branch during release candidate phase 2
is performed manually since the official backporting process begins only when
the release is pronounced "stable".

When ?
^^^^^^

After the CLT announces that it is time for phase 2 to happen.

Branch merges
^^^^^^^^^^^^^

* The stable release branch is occasionally merged into main.

Stable releases (i.e. x.2.z)
----------------------------

What ?
^^^^^^

* Bug fixes
* Features are sometime accepted
* Commits should be cherry-picked from *main* when possible
* Commits that are not cherry-picked from *main* must pertain to a bug unique to
  the stable release
* See also the `backport HOWTO`_ document

.. _`backport HOWTO`:
  http://tracker.ceph.com/projects/ceph-releases/wiki/HOWTO#HOWTO

Where ?
^^^^^^^

The stable release branch (*hammer* for 0.94.x, *infernalis* for 9.2.x,
etc.)

When ?
^^^^^^

After the stable release is published, i.e. after the "vx.2.0" tag is set in
the release branch.

Branch merges
^^^^^^^^^^^^^

Never
