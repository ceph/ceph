What is Merged Where and When?
===============================

Commits are merged into branches according to criteria that change
during the lifecycle of a Ceph release. This chapter is the inventory
of what can be merged in which branch at a given point in time.

Development releases (i.e. x.0.z)
---------------------------------

What ?
^^^^^^

* features
* bug fixes

Where ?
^^^^^^^

Features are merged to the master branch. Bug fixes should be merged
to the corresponding named branch (e.g. "jewel" for 10.0.z, "kraken"
for 11.0.z, etc.). However, this is not mandatory - bug fixes can be
merged to the master branch as well, since the master branch is
periodically merged to the named branch during the development
releases phase. In either case, if the bugfix is important it can also
be flagged for backport to one or more previous stable releases.

When ?
^^^^^^

After the stable release candidates of the previous release enters
phase 2 (see below).  For example: the "jewel" named branch was
created when the infernalis release candidates entered phase 2. From
this point on, master was no longer associated with infernalis. As
soon as the named branch of the next stable release is created, master
starts getting periodically merged into it.

Branch merges
^^^^^^^^^^^^^

* The branch of the stable release is merged periodically into master.
* The master branch is merged periodically into the branch of the
  stable release.
* The master is merged into the branch of the stable release
  immediately after each development x.0.z release.

Stable release candidates (i.e. x.1.z) phase 1
----------------------------------------------

What ?
^^^^^^

* bug fixes only

Where ?
^^^^^^^

The branch of the stable release (e.g. "jewel" for 10.0.z, "kraken"
for 11.0.z, etc.) or master.  Bug fixes should be merged to the named
branch corresponding to the stable release candidate (e.g. "jewel" for
10.1.z) or to master. During this phase, all commits to master will be
merged to the named branch, and vice versa. In other words, it makes
no difference whether a commit is merged to the named branch or to
master - it will make it into the next release candidate either way.

When ?
^^^^^^

After the first stable release candidate is published, i.e. after the
x.1.0 tag is set in the release branch.

Branch merges
^^^^^^^^^^^^^

* The branch of the stable release is merged periodically into master.
* The master branch is merged periodically into the branch of the
  stable release.
* The master is merged into the branch of the stable release
  immediately after each x.1.z release candidate.

Stable release candidates (i.e. x.1.z) phase 2
----------------------------------------------

What ?
^^^^^^

* bug fixes only

Where ?
^^^^^^^

The branch of the stable release (e.g. "jewel" for 10.0.z, "kraken"
for 11.0.z, etc.). During this phase, all commits to the named branch
will be merged into master. Cherry-picking to the named branch during
release candidate phase 2 is done manually since the official
backporting process only begins when the release is pronounced
"stable".

When ?
^^^^^^

After Sage Weil decides it is time for phase 2 to happen.

Branch merges
^^^^^^^^^^^^^

* The branch of the stable release is merged periodically into master.

Stable releases (i.e. x.2.z)
----------------------------

What ?
^^^^^^

* bug fixes
* features are sometime accepted
* commits should be cherry-picked from master when possible

* commits that are not cherry-picked from master must be about a bug unique to
  the stable release
* see also `the backport HOWTO`_

.. _`the backport HOWTO`:
  http://tracker.ceph.com/projects/ceph-releases/wiki/HOWTO#HOWTO

Where ?
^^^^^^^

The branch of the stable release (hammer for 0.94.x, infernalis for 9.2.x,
etc.)

When ?
^^^^^^

After the stable release is published, i.e. after the "vx.2.0" tag is set in
the release branch.

Branch merges
^^^^^^^^^^^^^

Never
