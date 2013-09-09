Notes on Ceph repositories and test lab
=======================================

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


Teuthology lab hardware
-----------------------

* 96 plana: 8 core, 4 disk, 1gig and 10gig networks.  Used for nightly runs.

* 64 burnupi: Dell R515s: 6 core, 8 disk, 1gig and 10gig networks, moderate CPU.  crummy SAS expanders.

* 120 mira: 8 core, 8 disks, 1gig network.  older hardware, flakier disks

* 8 vercoi: 24 core, RAID; VM hosts

* 4 senta: 25 core: VM hosts

* 36 saya: Calxeda ARM7 nodes (for testing)

* 24 tala: Calxeda ARM7 nodes (gitbuilder)

* ~200 vps: VMs running on mira hardware

Locking machines
----------------

* All tests pull their builds from gitbuilder.ceph.com.

* Anybody can lock machines with ``teuthology-lock --lock-many NUM
  --machine-type TYPE``.

* Machines are locked as ``whoami''@``hostname -s``.  --owner to
  choose otherwise.

* Automated tests current run on the ``plana``; please avoid locking
  these for personal use.

* To unlock, please use ``teuthology-nuke -t list.yaml -r -u``, which
  will reboot and clean up any leftover test state before unlocking
  (or fail to unlock).  It looks for a ``targets::`` section in the
  yaml, so the regular job yaml will work.  You can get a list of all
  locked machines with ``teuthology-lock --list-targets``.

* ``teuthology-lock -a --brief`` or ``teuthology-lock --summary`` to
  see what is locked and by whom.

* Be conscientious about scheduling entire qa runs.  Coordinate
  utilization on IRC.  Make sure you are running the latest version
  ceph-qa-suite.git and teuthology.git.

* Results for scheduled runs appear in /a/$jobname on the teuthology
  machine.  ``ls -alt | head`` to find them.
