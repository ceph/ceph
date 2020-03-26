===============
Release kickoff
===============

These steps should be taken when starting a new major release, just after
the previous release has been tagged (vX.2.0) and that tag has been merged
back into master.

Versions and tags
-----------------

- [ ] Update CMakeLists.txt VERSION (right at the top to X.0.0)
- [ ] Update src/ceph_release with the new release name, number, and type ('dev')
- [ ] Initial tag vX.0.0 (so that we can distinguish from (and sort
  after) the backported (X-1).2.Z versions.


Define release names and constants
----------------------------------

Make sure X (and, ideally, X+1) is defined:

- [ ] src/common/ceph_releases.h (ceph_release_t)
- [ ] src/common/ceph_strings.cc (ceph_release_name())
- [ ] src/include/rados.h (CEPH_RELEASE_* and MAX)
- [ ] src/mon/mon_types.h (ceph::features::mon::FEATURE_* and related structs and helpers; note that monmaptool CLI test output will need adjustment)

Scripts
~~~~~~~

- [ ] src/script/backport-create-issue (releases())
- [ ] src/script/backport-resolve-issue (releases(), ver_to_release().. but for X-1)
- [ ] src/script/ceph-backport.sh (try_known_milestones())
- [ ] src/script/ceph-release-notes (X-1)

Misc
~~~~
- [ ] update src/ceph-volume/ceph_volume/__init__.py (__release__)
