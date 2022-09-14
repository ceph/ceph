==================
Release checklists
==================

Dev Kickoff
===========

These steps should be taken when starting a new major release, just after
the previous release has been tagged (vX.2.0) and that tag has been merged
back into master.

X is the release we are just starting development on.  X-1 is the one
that was just released (X-1).2.0.

Versions and tags
-----------------

- [x] Update CMakeLists.txt VERSION (right at the top to X.0.0)
- [x] Update src/ceph_release with the new release name, number, and type ('dev')
- [ ] Initial tag vX.0.0 (so that we can distinguish from (and sort
  after) the backported (X-1).2.Z versions.


Define release names and constants
----------------------------------

Make sure X (and, ideally, X+1) is defined:

- [x] src/common/ceph_releases.h (`ceph_release_t`)
- [x] src/common/ceph_strings.cc (`ceph_release_name()`)
- [x] src/include/rados.h (`CEPH_RELEASE_*` and `MAX`)
- [x] src/mon/mon_types.h (`ceph::features::mon::FEATURE_*` and related structs and helpers; note that monmaptool CLI test output will need adjustment)
- [x] src/mds/cephfs_features.h (`CEPHFS_CURRENT_RELEASE`)

Scripts
~~~~~~~

- [x] src/script/backport-resolve-issue (`releases()`, `ver_to_release()`... but for X-1)
- [x] src/script/ceph-release-notes (X-1)
- [ ] ceph-build.git scripts/build_utils.sh `release_from_version()`

Misc
~~~~
- [x] update src/ceph-volume/ceph_volume/__init__.py (`__release__`)
- [x] update src/tools/monmaptool.cc (`min_mon_release` and corresponding output in `src/test/cli/monmaptool`)
- [x] update src/cephadm/cephadm (`DEFAULT_IMAGE_RELEASE` to X)

Feature bits
------------

- [x] ensure that `SERVER_X` is defined
- [x] change any features `DEPRECATED` in release X-3 are now marked `RETIRED`.
- [ ] look for features that (1) were present in X-2 and (2) have no
  client dependency and mark them `DEPRECATED` as of X.


Compatsets
----------

- [x] mon/Monitor.h (`CEPH_MON_FEATURE_INCOMPAT_X`)
- [x] mon/Monitor.cc (include in `get_supported_features()`)
- [x] mon/Monitor.cc (`apply_monmap_to_compatset_features()`)
- [x] mon/Monitor.cc (`calc_quorum_requirements()`)
- [x] test/cli/monmaptool/feature-set-unset-list.t (`supported`, `persistent`)

Mon
---

- [x] qa/standalone/mon/misc adjust `TEST_mon_features` (add X cases and adjust `--mon-debug-no-require-X`)
- [x] mon/MgrMonitor.cc adjust `always_on_modules`
- [x] common/options/global.yaml.in define `mon_debug_no_require_X`
- [x] common/options/global.yaml.in remove `mon_debug_no_require_X-2`
- [x] mon/OSDMonitor.cc `create_initial`: adjust new `require_osd_release`, and add associated `mon_debug_no_require_X`
- [x] mon/OSDMonitor.cc `preprocess_boot`: adjust "disallow boot of " condition to disallow X if `require_osd_release` < X-2.
- [x] mon/OSDMonitor.cc: adjust "osd require-osd-release" to (1) allow setting X, and (2) check that all mons *and* OSDs have X
- [x] mon/MonCommands.h: adjust "osd require-osd-release" allows options to include X
- [x] qa/workunits/cephtool/test.sh: adjust `require-osd-release` test


Code cleanup
------------

- [ ] search code for "after X-1" or "X" for conditional checks
- [ ] search code for X-2 and X-3 (`CEPH_FEATURE_SERVER_*` and
  `ceph_release_t::*`)
- [ ] search code for `require_osd_release`
- [ ] search code for `min_mon_release`

QA suite
--------

- [x] create qa/suites/upgrade/(X-1)-x
- [x] remove qa/suites/upgrade/(X-3)-x-*
- [x] create qa/releases/X.yaml
- [x] create qa/suites/rados/thrash-old-clients/1-install/(X-1).yaml



First release candidate
=======================

- [ ] src/ceph_release: change type to `rc`
- [ ] opt-in to all telemetry channels, generate telemetry reports, and verify no sensitive details (like pools names) are collected


First stable release
====================

- [ ] src/ceph_release: change type `stable`
- [ ] generate new object corpus for encoding/decoding tests - see :doc:`corpus`
- [ ] src/cephadm/cephadm: update `LATEST_STABLE_RELEASE`
