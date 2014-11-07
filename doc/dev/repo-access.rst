Notes on Ceph repositories
==========================

Special branches
----------------

* ``master``: current tip (integration branch)
* ``next``: pending release (feature frozen, bugfixes only)
* ``last``: last/previous release
* ``dumpling``, ``cuttlefish``, ``bobtail``, ``argonaut``, etc.: stable release branches
* ``dumpling-next``: backports for stable release, pending testing

Rules
-----

The source repos are all on github.

* Any branch pushed to ceph.git will kick off builds that will either
  run unit tests or generate packages for gitbuilder.ceph.com.  Try
  not to generate unnecessary load.  For private, unreviewed work,
  only push to branches named ``wip-*``.  This avoids colliding with
  any special branches.

* Nothing should every reach a special branch unless it has been
  reviewed.

* Preferred means of review is via github pull requests to capture any
  review discussion.

* For multi-patch series, the pull request can be merged via github,
  and a Reviewed-by: ... line added to the merge commit.

* For single- (or few-) patch merges, it is preferable to add the
  Reviewed-by: directly to the commit so that it is also visible when
  the patch is cherry-picked for backports.

* All backports should use ``git cherry-pick -x`` to capture which
  commit they are cherry-picking from.
