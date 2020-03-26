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

- [x] src/common/ceph_releases.h (ceph_release_t)
- [x] src/common/ceph_strings.cc (ceph_release_name())
- [x] src/include/rados.h (CEPH_RELEASE_* and MAX)
- [ ] src/mon/mon_types.h (ceph::features::mon::FEATURE_* and related structs and helpers; note that monmaptool CLI test output will need adjustment)

Scripts
~~~~~~~

- [x] src/script/backport-resolve-issue (releases(), ver_to_release().. but for X-1)
- [x] src/script/ceph-release-notes (X-1)

Misc
~~~~
- [x] update src/ceph-volume/ceph_volume/__init__.py (__release__)

Feature bits
------------

- [ ] ensure that SERVER_X is defined
- [ ]


Compatsets
----------

- [x] mon/Monitor.h (CEPH_MON_FEATURE_INCOMPAT_X)
- [x] mon/Monitor.cc (include in get_supported_features)
- [x] mon/Monitor.cc (apply_monmap_to_compatset_features())
- [x] mon/Monitor.cc (calc_quorum_requirements())

