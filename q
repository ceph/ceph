[33m581f22da52[m[33m ([m[1;36mHEAD -> [m[1;32mbucket_name_rules[m[33m, [m[1;33mtag: v14.2.9[m[33m)[m 14.2.9
[33mc7da604cb1[m rgw: reject control characters in response-header actions
[33m87a63d1743[m rgw: EPERM to ERR_INVALID_REQUEST
[33mfce0b26744[m rgw: reject unauthenticated response-header actions
[33mf6c5ad8a5f[m msg/async/crypto_onwire: fix endianness of nonce_t
[33m47c7e62354[m msg/async/ProtocolV2: avoid AES-GCM nonce reuse vulnerabilities
[33m3785e7545a[m msg/async: rename outcoming_bl -> outgoing_bl in AsyncConnection.
[33m2d095e947a[m[33m ([m[1;33mtag: v14.2.8[m[33m)[m 14.2.8
[33m2885574277[m Merge PR #33569 into nautilus
[33mf071465e2b[m mgr/volumes: unregister job upon async threads exception
[33md7e0d07101[m Merge pull request #33346 from yaarith/backport-nautilus-pr-32903
[33m2b1d255d4e[m Merge PR #33526 into nautilus
[33m7abad7dba0[m test: verify purge queue w/ large number of subvolumes
[33m6440d38e66[m test: pass timeout argument to mount::wait_for_dir_empty()
[33m9c455de61d[m mgr/volumes: access volume in lockless mode when fetching async job
[33m97ce2bd8ad[m Merge PR #33498 into nautilus
[33m24c0e72d4d[m mgr: drop reference to msg on return
[33ma1c7b3fefa[m Merge pull request #33470 from neha-ojha/wip-mgsr2-order-nautilus
[33m0079d1ccd3[m qa/suites/upgrade/mimic-x/stress-split: fix msgr2 vs nautilus ordering
[33m392f4714af[m Merge pull request #33378 from badone/wip-badone-testing
[33mea8e0b086e[m nautilus: qa/ceph-ansible: ansible-version and ceph_ansible
[33m21d78c0b87[m mgr/devicehealth: fix telemetry stops sending device reports after 48 hours
[33m70fa3cf234[m mgr/devicehealth: factor _get_device_metrics out of show_device_metrics
[33m4d5b840850[m Merge pull request #33278 from smithfarm/wip-44085-nautilus
[33mcc0b4d4f1e[m Merge pull request #33277 from smithfarm/wip-43722-nautilus
[33m1597e2ae97[m Merge pull request #33276 from smithfarm/wip-44082-nautilus
[33m59eedd8f52[m Merge pull request #32844 from smithfarm/wip-43239-nautilus
[33m21a2166957[m Merge pull request #33337 from jan--f/wip-44153-nautilus
[33m5410fcd16d[m Merge pull request #33334 from jan--f/wip-44152-nautilus
[33m104f6cad73[m ceph-volume: don't remove vg twice when zapping filestore
[33m23866324fb[m ceph-volume: pass journal_size as Size not string
[33md430eceb60[m Merge pull request #33301 from jan--f/wip-43871-nautilus-failed-cp
[33mdc39f214be[m Merge pull request #33297 from jan--f/wip-44135-nautilus
[33m7f6ebdc4de[m ceph-volume: batch bluestore fix create_lvs call
[33m4e6231fe5c[m ceph-volume: avoid calling zap_lv with a LV-less VG
[33m10a8372ee9[m Merge pull request #33151 from shyukri/wip-43877-nautilus
[33m9a360a9f64[m Merge pull request #33149 from shyukri/wip-43874-nautilus
[33m5d418256f8[m Merge pull request #33008 from smithfarm/wip-43922-nautilus
[33m081f5cef38[m Merge pull request #33152 from shyukri/wip-43879-nautilus
[33m01099f6668[m Merge pull request #33095 from k0ste/wip-43979-nautilus
[33m7a46e53000[m Merge pull request #32908 from smithfarm/wip-43821-nautilus
[33m11256ac1ea[m Merge pull request #33170 from k0ste/wip-43727-nautilus
[33mc4deea5ace[m Merge pull request #33168 from k0ste/wip-44057-nautilus
[33m3611588b2e[m Merge pull request #33157 from shyukri/wip-43924-nautilus
[33m066b4f0d9d[m Merge pull request #33155 from shyukri/wip-43916-nautilus
[33m61a8864ead[m Merge pull request #33147 from shyukri/wip-43989-nautilus
[33mb24c49095e[m Merge pull request #33142 from shyukri/wip-44000-nautilus
[33m0f85792f82[m Merge pull request #33082 from k0ste/wip-43974-nautilus
[33mec1f782383[m Merge pull request #32948 from yaarith/wip-telemetry-serial-nautilus-yh
[33mf203d8b830[m Merge pull request #33007 from smithfarm/wip-43928-nautilus
[33mc7657a715d[m Merge pull request #32931 from smithfarm/wip-43819-nautilus
[33m4e9023a943[m Merge pull request #32905 from smithfarm/wip-43731-nautilus
[33m11be0f17e5[m Merge pull request #32856 from zhengchengyao/nautilus_no_mon_update
[33m065868905b[m doc: update mondb recovery script
[33ma1b24fa45e[m ceph-monstore-tool: correct the key for storing mgr_command_descs
[33m12310ddf4d[m ceph-monstore-tool: rename mon-ids in initial monmap
[33mbda388c19a[m common/bl: fix the dangling last_p issue.
[33m6dc0091b9b[m qa/suites/rados/multimon/tasks/mon_clock_with_skews: whitelist MOST_DOWN
[33mc506fbe0e3[m qa/suites/rados/multimon/tasks/mon_clock_with_skews: disable ntpd etc
[33mda9b42ec81[m Merge pull request #33254 from jan--f/wip-44112-nautilus
[33m47d3e52a82[m Merge pull request #33253 from jan--f/wip-44109-nautilus
[33m39e15a7b4e[m Merge pull request #33240 from jan--f/wip-44035-nautilus
[33mf9a31b4095[m Merge pull request #33239 from jan--f/wip-43984-nautilus
[33m17a6e4ab4b[m ceph-volume: use get_device_vgs in has_common_vg
[33m6acc68f2e7[m ceph-volume: add is_ceph_device unit tests
[33m7df3e636e5[m ceph-volume: fix is_ceph_device for lvm batch
[33m4ea0c07e17[m Merge pull request #33242 from jan--f/wip-44047-nautilus
[33m32dcee3bd1[m Merge pull request #32919 from smithfarm/wip-43780-nautilus
[33m6bc12183e1[m Merge pull request #33183 from smithfarm/wip-43846-nautilus
[33m987767eb14[m Merge pull request #33115 from batrick/i43790
[33m0728c085a8[m Merge pull request #32921 from smithfarm/wip-43784-nautilus
[33me95f718289[m Merge pull request #32918 from smithfarm/wip-43777-nautilus
[33m79346f0e74[m Merge pull request #32917 from smithfarm/wip-43733-nautilus
[33m63e487a44c[m Merge pull request #32756 from batrick/i43347
[33m8664ee67d1[m Merge pull request #31905 from batrick/i43046
[33mce39bb7b8f[m Merge pull request #32916 from smithfarm/wip-43729-nautilus
[33mb03adf9013[m Merge pull request #32807 from smithfarm/wip-43770-nautilus
[33m0c500d17e4[m Merge pull request #32910 from smithfarm/wip-43503-nautilus
[33m91d0d48ffc[m Merge pull request #33122 from ajarr/wip-ajarr-mgr-volumes-nautilus
[33mb9e8232b27[m qa/standalone/misc/ok-to-stop: improve test
[33m38fc067f67[m qa/standalone/ceph-helpers: add wait_for_peered
[33m768a47e98b[m mgr/DaemonServer: fix 'osd ok-to-stop' for EC pools
[33m1d48c4df8b[m ceph-volume: add unit test test_safe_prepare_osd_already_created
[33mdff4c69d52[m ceph-volume: skip osd creation when already done
[33md16e2bc479[m ceph-volume: add available property in target specific flavors
[33mffbf252299[m ceph-volume: remove stderr in has_bluestore_label()
[33ma095f5800b[m ceph-volume: fix has_bluestore_label() function
[33mfd3afb2064[m Merge pull request #33238 from jan--f/wip-31700-notracker-nautilus
[33mcad770415f[m ceph-volume: fix various lvm list issues
[33m6b8b86ab78[m ceph-volume: add get_device_lvs to easily retrieve all lvs per device
[33m30dc9359e7[m ceph-volume: fix lvm list
[33m6cb2aad6fe[m ceph-volume: delete test_lvs_list_is_created_just_once
[33m27bd05e916[m ceph-volume: update tests since listing.py got heavily modified
[33m66bbe1651d[m ceph-volume: refactor devices/lvm/listing.py
[33m17cba1fe09[m ceph-volume: add new method in api/lvm.py
[33m65f830e44f[m ceph-volume: add helper methods to get only first LVM devs
[33m136a0feaa8[m ceph-volume: filter based on tags for api.lvm.get_* methods
[33m2d564653c3[m Merge pull request #33232 from jan--f/wip-43871-nautilus
[33m9fe99d3b6a[m Merge pull request #33231 from jan--f/wip-43849-nautilus
[33m3b514df2cf[m mgr/volumes: fix py2 compat issue
[33m96bcad5e41[m mgr/volumes: type convert uid and gid to int
[33m6265db4e9c[m test: add subvolume clone tests
[33m41ca4b7f75[m mgr/volumes: allow force removal of incomplete failed clones
[33me994382a63[m mgr/volumes: asynchronous cloner module
[33mb05daa5519[m mgr/volumes: purge thread uses new async interface
[33m13ce8310c4[m mgr/volumes: fetch oldest clone entry
[33m478a38b5a4[m mgr/volumes: add clone specific commands
[33mc181702b2b[m mgr/volumes: interface for fetching cloned subvolume status
[33m067b94e588[m mgr/volumes: add protect/unprotect and snap clone interface
[33m628496a47c[m mgr/volumes: handle transient subvolume states
[33m140a5c032f[m mgr/volumes: interface for creating a cloned subvolume
[33mf54469448e[m mgr/volumes: get/set property for subvolume mode/uid/gid
[33m69c6da6d26[m mgr/volumes: module to track pending clone operations
[33m5bba5984f4[m mgr/volumes: add operation state machine table
[33m340c2d886e[m mgr/volumes: fail removing subvolume with snapshots
[33m9a86695fec[m mgr/volumes: remove stale subvolume module
[33mc469aef16b[m test: auto-upgrade subvolume test
[33mc21fa0c30f[m mgr/volumes: tie everything together to implement versioned subvolumes
[33mca4cc7166e[m mgr/volumes: provide subvolume create/remove/open APIs
[33m4aefbb3e29[m mgr/volumes: implement subvolume based on subvolume template
[33mbe0746524e[m mgr/volumes: implement subvolume group based on group template
[33m0afdfbf1d1[m mgr/volumes: implement trash as a subvolume group
[33mcb592a6228[m mgr/volumes: snapshot util module
[33mf6e36a85a1[m mgr/volumes: template for implementing groups and subvolumes
[33m7c0adc0f6e[m mgr/volumes: implement filesystem volume module
[33mc8a5ac00ef[m mgr/volumes: lock module to serialize volume operations
[33m4c17f917be[m mgr/volumes: introduce volume specification module
[33m082baca9e9[m mgr/volumes: add fs_util helper module
[33m1cfa39ec61[m mgr/volumes: drop obsolete comment in _cmd_fs_volume_create
[33mb701323f7d[m mgr/volumes: cleanup on fs create error
[33mf2304fdb3b[m mgr/volumes: move up 'confirm' validation
[33m6e6b55d782[m mgr/volumes: remove unsed variable
[33mb1f85d8965[m mgr/volumes: guard volume delete by waiting for pending ops
[33ma9819fe280[m mgr/volumes: cleanup libcephfs handles when stopping
[33m2da46f644d[m mgr/volumes: refactor dir handle cleanup
[33me81eb4aa9b[m mgr/volumes: cleanup leftovers from earlier purge job implementation
[33md2586a6d24[m qa/tasks: Nothing to clean up if the volume was not created
[33m8dd64e8eb4[m qa/tasks: Fix the volume ls in test_volume_rm
[33m6e01b15f07[m qa/tasks: tests for 'fs volume create' and 'fs volume ls'
[33ma6489c7e4a[m qa/tasks: remove subvolume, subvolumegroup and their snapshots with --force
[33m556c7ee48b[m qa/tasks: Fix the commands success
[33m9243d59cbf[m qa/tasks: Fix raises that doesn't re-raise
[33m97ea800e55[m test: use distinct subvolume/group/snapshot names
[33m622d923329[m pybind/cephfs: add method that stats symlinks without following
[33ma4df3188b3[m Merge pull request #33116 from batrick/i43137
[33m00317e84df[m ceph-volume: batch bluestore fix create_lvs call
[33mbf67433239[m ceph-volume: remove redefinition of [LV,PV,VG]_FIELDS
[33m3dc93ffae1[m tests: fix tests after batch sizing was fixed
[33m50685cf70b[m lvm/batch: adjust devices for byte based size calculation
[33m31fb57fccd[m lvm: add sizing arguments to prepare and create.
[33m3f8ed660c6[m util/disk: extend Size class
[33m1a4a731a07[m Merge pull request #33220 from yuriw/wip-yuriw-clients-upgrades-nautilus
[33m1224727558[m qa/tests: added client-upgrade-nautilus suite to be used on octopus release
[33mfdb60ea59c[m Merge pull request #32863 from shyukri/wip-43341-nautilus
[33m3ea3a9cf82[m Merge pull request #33217 from jan--f/wip-32242-notrack-nautilus
[33meac1044738[m ceph-volume: add methods to pass filters to pvs, vgs and lvs commands
[33m3b0f093779[m Merge pull request #32912 from smithfarm/wip-43509-nautilus
[33m04391ea995[m lvm/deactivate: add unit tests, remove --all
[33m3e8c4dcbef[m Merge pull request #33209 from jan--f/wip-deactivate-nautilus
[33mb20a52adb4[m Merge pull request #32915 from smithfarm/wip-43628-nautilus
[33mae50da1d73[m Merge pull request #32914 from smithfarm/wip-43624-nautilus
[33m99802a34bd[m Merge pull request #32913 from smithfarm/wip-43573-nautilus
[33m859e88930c[m Merge pull request #32909 from smithfarm/wip-43343-nautilus
[33m9b2c28addc[m Merge pull request #32602 from batrick/i43558
[33m0e34ed8dac[m Merge pull request #32600 from batrick/i43506
[33mae181f7b96[m Merge pull request #30843 from smithfarm/wip-41853-nautilus
[33m644f316546[m lvm: add deactivate subcommand
[33m3a5ea621f3[m util/system: add unmount_tmpfs helper
[33m5a8d72bea8[m api/lvm: add get_lv_by_osd_id method
[33m44b7312ef2[m api/lvm: add deactivate method to Volume class
[33m3f8a4c0056[m Merge pull request #32868 from shyukri/wip-42945-nautilus
[33m3abb9b21e5[m doc: update ceph-volume lvm prepare
[33m9089bf3e07[m ceph-volume: make lvm report fields into constants
[33m670351a9d5[m ceph-volume: api/lvm create or reuse a vg
[33m1b0aa07b28[m Merge pull request #33202 from jan--f/wip-43853-nautilus
[33mfe056744ce[m Merge pull request #33200 from jan--f/wip-42898-nautilus
[33m34dfcbb89d[m Merge pull request #32870 from shyukri/wip-43117-nautilus
[33m13f8029b61[m Merge pull request #32877 from shyukri/wip-43570-nautilus
[33md067bf3f18[m Merge pull request #32873 from shyukri/wip-43201-nautilus
[33m885fbbd342[m Merge pull request #32874 from shyukri/wip-43321-nautilus
[33m83bf978c3a[m Merge pull request #32864 from shyukri/wip-43462-nautilus
[33m4744db7bf0[m Merge pull request #32860 from shyukri/wip-43281-nautilus
[33m7d55a95aba[m Merge pull request #31616 from jan--f/wip-42800-nautilus
[33mb1045cfbda[m ceph-volume/batch: fail on filtered devices when non-interactive
[33m15f88a7b67[m ceph-volume: refactor tests for refactored get_devices
[33m1964650fa1[m ceph-volume: refactor get_devices, don't use os.path.realpath
[33mac764642ef[m Merge pull request #32558 from shyukri/wip-43275-nautilus
[33m4cdbe5431a[m Merge pull request #32556 from shyukri/wip-43022-nautilus
[33m6036662bc0[m Merge pull request #32998 from neha-ojha/wip-min-alloc-nautilus
[33mb57c249b47[m Merge pull request #32846 from smithfarm/wip-43256-nautilus
[33maf24a4a734[m Merge pull request #32997 from neha-ojha/wip-32939-nautilus
[33m02cb84c36e[m Merge pull request #32901 from smithfarm/wip-43631-nautilus
[33m84ab66212a[m Merge pull request #32858 from smithfarm/wip-43473-nautilus
[33m66c57a0c6f[m Merge pull request #32845 from smithfarm/wip-43245-nautilus
[33m3ff464664c[m Merge pull request #32774 from dzafman/wip-43726-nautilus
[33m0e0b51946e[m rgw: update the hash source for multipart entries during resharding Fixes: https://tracker.ceph.com/issues/43583
[33m0253205ef3[m mgr/pg_autoscaler: calculate pool_pg_target using pool size
[33me9ff205a29[m mgr/telemetry: split entity_name only once (handle ids with dots)
[33m654071d20f[m mgr/grafana: sum pg states for cluster
[33m191ef972a8[m monitoring/grafana,prometheus: add per-pool pg states support
[33m1bd19295d5[m mgr/prometheus: pg counters per pool descriptions
[33m133dd7e4f6[m mgr/prometheus: pg count by pool
[33mb07431242f[m mgr/prometheus: report per-pool pg states
[33m4b60c0ef4d[m mon/ConfigMonitor: only propose if leader
[33m682f231b7c[m mon: Don't put session during feature change
[33md1d725f593[m rgw: fix one part of the bulk delete(RGWDeleteMultiObj_ObjStore_S3)fails but no error messages Fixes: None Signed-off-by: Snow Si <silonghu@inspur.com>
[33m0918862c3f[m rgw: maybe coredump when reload operator happened
[33m7b595aa65f[m OSD: Allow 64-char hostname to be added as the "host" in CRUSH
[33m24a84c5779[m mon/MgrMonitor.cc: warn about missing mgr in a cluster with osds
[33meb0c52a312[m ceph-volume: Dereference symlink in lvm list
[33m34374469cb[m ceph-volume: use correct extents when using db-devices and >1 osds_per_device
[33ma098289129[m ceph-volume: fix the type mismatch, covert the tries and interval to int from string. Fixes: https://tracker.ceph.com/issues/43186
[33m2dab9b20fa[m ceph-volume: import mock.mock instead of unittest.mock (py2)
[33m2c6ca6cb5e[m lvm/activate.py: clarify error message: fsid refers to osd_fsid
[33m259dd74433[m ceph-volume: util: look for executable in $PATH
[33me28dea68b0[m Merge pull request #30689 from smithfarm/wip-42120-nautilus
[33m5b95c39934[m Merge pull request #32857 from smithfarm/wip-43471-nautilus
[33m78240e373d[m Merge pull request #32848 from smithfarm/wip-43346-nautilus
[33m48ed41fee9[m Merge pull request #32847 from smithfarm/wip-43319-nautilus
[33m5030d73c12[m Merge pull request #32843 from smithfarm/wip-43099-nautilus
[33m443a6cf900[m Merge pull request #32773 from dzafman/wip-43246-nautilus
[33mfe2035f3fe[m Merge branch 'nautilus' into wip-42120-nautilus
[33m250a778fe8[m Merge pull request #32716 from smithfarm/wip-43650-nautilus
[33m5b624a49b5[m Merge pull request #32715 from smithfarm/wip-43620-nautilus
[33mf5d69c45d8[m Merge pull request #32259 from smithfarm/wip-43243-nautilus
[33m49587d8c90[m Merge pull request #32769 from liewegas/fix-42566-nautilus
[33mc03baa327d[m qa: test volumes plugin mount cleanup
[33m11570355f2[m pybind/mgr/volumes: use py3 items iterator
[33me48394d9cd[m pybind/mgr/volumes: print errors in cleanup timer
[33m9e11deb5d5[m qa: improve variable name
[33m5e1d712b3c[m qa: test mgr cephfs mount blacklist
[33mf00276b8cd[m mds: track high water mark for purges
[33m3cf7870134[m qa: use correct variable for exception debug
[33mc47ea1e219[m mds: mark purge queue protected members private
[33m3fd5cbbc82[m Merge pull request #32930 from rhcs-dashboard/wip-43845-nautilus
[33mf94715a097[m Merge pull request #32827 from rhcs-dashboard/wip-43811-nautilus
[33ma38345e690[m Merge pull request #31808 from bk201/wip-42956-nautilus
[33m539cc8ba11[m Merge pull request #31792 from rhcs-dashboard/wip-42936-nautilus
[33m08239d5642[m Merge pull request #31190 from rhcs-dashboard/wip-42294-nautilus
[33m5362d0bcc6[m Merge pull request #32304 from ricardoasmarques/wip-43333-nautilus
[33m1bbdf03091[m Merge pull request #32888 from shyukri/wip-42033-nautilus
[33m6599f5a9da[m mgr/telemetry: check get_metadata return val
[33m3c7d09e200[m mgr/telemetry: anonymizing smartctl report itself
[33mc4dc4f0bb6[m mgr/telemetry: added 'telemetry show-device' command
[33mcb48be5a69[m mgr: drop session with Ceph daemon when not ready
[33m372d184fa2[m mgr: improve debug message information
[33m83e9e9c7f2[m qa: force creation of fs with EC default data pool
[33m7a41fab8de[m qa: add tests for adding EC data pools
[33m1ee9f2cdf8[m mon/MDSMonitor: warn when creating fs with default EC data pool
[33m95baab0135[m qa: add tests for CephFS admin commands
[33m3f7f0c72f1[m mds: skip tell command scrub on multimds
[33mc9ec465946[m mds: throttle scrub start for multiple active MDS
[33m4802061055[m mds: fix assert(omap_num_objs <= MAX_OBJECTS) of OpenFileTable
[33m27eb4d924c[m mgr/dashboard: disable 'Add Capability' button when all the capabilities are added
[33mb2296bf89e[m mgr/dashboard: check if user has config-opt permissions before retrieving settings.
[33med548f1e1e[m mgr/dashboard: Using wrong identifiers in RGW user/bucket datatables
[33mc23c6173cb[m qa: ignore slow ops for ffsb workunit
[33m5a5fe02b07[m qa: save MDS epoch barrier
[33mfa08059956[m qa: prefer rank_asok
[33m1a3bfd7d28[m test: test case for openfiletable MAX_ITEMS_PER_OBJ value verification
[33mb603319701[m mds/OpenFileTable: match MAX_ITEMS_PER_OBJ to osd_deep_scrub_large_omap_object_key_threshold Fixes: https://tracker.ceph.com/issues/42515
[33mc703b29bba[m mds: Reorganize class members in OpenFileTable header
[33m4dd5aee430[m qa: only restart MDS between tests
[33m4a0f6d7284[m mon: print FSMap if standbys exist
[33m215ad503b4[m mgr/dashboard: Unable to remove an iSCSI gateway that is already in use
[33mc15b0b4124[m Merge branch 'nautilus-saved' into nautilus
[33m3d58626ebe[m[33m ([m[1;33mtag: v14.2.7[m[33m)[m 14.2.7
[33m20fd89ebe1[m rgw: drop the partial message check while reading messages
[33mdad86a25d9[m rgw_file: avoid string::front() on empty path
[33m9155cdf187[m mon: elector: return after triggering a new election
[33m3099d97a84[m Merge pull request #32979 from leseb/bkp-32828
[33mcb923fd935[m common/options: bluestore 64k min_alloc_size for HDD
[33m698ef897bf[m common/options: Set bluestore min_alloc size to 4K
[33m9e277e1815[m Merge pull request #32221 from smithfarm/wip-43168-nautilus
[33m4bfaf2307e[m mon/MgrMonitor.cc: add always_on_modules to the output of "ceph mgr module ls"
[33md20e0d5779[m ceph-volume: add db and wal support to raw mode
[33m3a1b38922e[m ceph-volume: fix raw list
[33mf1114cdb71[m ceph-volume: remove unnecessary comment
[33m8dd70b6be1[m ceph-volume: remove osd-id and osd-fsid flag
[33m19e2786ddd[m ceph-volume: remove "create" call from raw mode
[33m18a475e751[m Merge pull request #32835 from smithfarm/wip-43812-nautilus
[33m608d250716[m Merge pull request #32834 from smithfarm/wip-43789-nautilus
[33m4e05a6519a[m Merge pull request #32833 from smithfarm/wip-43786-nautilus
[33m4ae4e1af63[m Merge pull request #32832 from smithfarm/wip-43782-nautilus
[33m921fe98188[m Merge pull request #32504 from smithfarm/wip-43477-nautilus
[33mb8eef7c327[m Merge pull request #32437 from pritha-srivastava/wip-42385-nautilus
[33m6500cf17dc[m Merge pull request #32239 from smithfarm/wip-43300-nautilus
[33m3e5577af21[m Merge pull request #32825 from smithfarm/wip-43779-nautilus
[33m2747599aa4[m Merge pull request #32824 from smithfarm/wip-43728-nautilus
[33m783c8d7428[m Merge pull request #32822 from smithfarm/wip-43576-nautilus
[33m3c8a436cd3[m Merge pull request #32821 from smithfarm/wip-43574-nautilus
[33m65a5752e9d[m Merge pull request #32820 from smithfarm/wip-43571-nautilus
[33m7817cde9b1[m Merge pull request #32819 from smithfarm/wip-43373-nautilus
[33m4ab531f1d8[m Merge pull request #32818 from smithfarm/wip-43372-nautilus
[33m24b13e92fd[m Merge pull request #32759 from smithfarm/wip-43203-nautilus
[33m7df437831f[m Merge pull request #32489 from bzed/clang-build-fix
[33m3b7f83c2be[m Merge pull request #32057 from smithfarm/wip-43010-nautilus
[33m3b6da927f8[m Merge pull request #32900 from smithfarm/wip-43830-nautilus
[33mcad90dab41[m Merge pull request #32842 from smithfarm/wip-43735-nautilus
[33m9173e52b5e[m Merge pull request #32841 from smithfarm/wip-43511-nautilus
[33m4dd4c4828e[m Merge pull request #32840 from smithfarm/wip-43508-nautilus
[33md695c48baa[m Merge pull request #32839 from smithfarm/wip-43501-nautilus
[33m3a3d5c3ef9[m Merge pull request #32837 from smithfarm/wip-43237-nautilus
[33mb338ffa038[m mgr/telemetry: fix device serial number anonymization
[33md45fbf17a3[m qa/tasks/mgr/dashboard: set pg_num to 32
[33m1e5623e3ed[m mgr/pg_autoscaler: default to pg_num[_min] = 32
[33m4047f4716e[m Merge pull request #32815 from tchaikov/nautilus-pr-30284
[33me1fe280d61[m Merge pull request #32441 from tchaikov/nautilus-boost-1.72
[33m7f8f8f4d48[m qa: ignore trimmed cache items for dead cache drop
[33m5948820d42[m qa: use unit test comparisons
[33m2bf8993f6f[m client: Add is_dir check before changing directory
[33m0213b7e7a1[m client: disallow changing fuse_default_permissions option at runtime
[33m68d876ff36[m mds: note client features when rejecting client
[33m0c7b930ded[m mds: add hex indicator to printed feature flags
[33mc13fcaf8d5[m cephfs-journal-tool: fix the usage
[33m68daca8b7a[m cephfs-journal-tool: fix crash with empty argv
[33m6d1ce3faa6[m mds: fix revoking caps after after stale->resume circle
[33m39fb115691[m mount.ceph: give a hint message when no mds is up or cluster is laggy
[33m871279ef9e[m client: fix incorrect debug message
[33m5b9a2b756e[m mon/Session: only index osd ids >= 0
[33ma1e0d623d5[m librbd: don't call refresh from mirror::GetInfoRequest state machine
[33mfb34981fdd[m crush/CrushWrapper: behave with empty weight vector
[33m3a36e63ff7[m cmake: no need to link libglobal and libblkid for testing crush
[33m178122e83b[m test/crush: no need to use libglobal for testing crush
[33md8ff6bc963[m common/util: use ifstream to read from /proc files
[33m548e59e7fc[m common,tools: make sure the destination buffer can handle the size of the string
[33m961c9f8e64[m mon/ConfigMonitor: put global/ keys in global section
[33m529ed32d58[m mon/ConfigMonitor: transition old keys to have global/ prefix
[33m60bb98aaad[m mon/ConfigMonitor: always prefix global config keys with global/
[33ma9e12a8073[m mon/ConfigMonitor: switch to use pending_cleanup boost::optional
[33me7afc576d9[m mds: don't add metadata to session close message
[33m504dbcc94a[m mds: complete all the replay op when mds is restarted ,no matter the session is closed or not.
[33m658d215718[m mds: reject sessionless messages
[33m2ab5460062[m nautilus: monitoring: add details to Prometheus' alerts (#32892)
[33m8c5c82fbe7[m Merge pull request #32838 from smithfarm/wip-43241-nautilus
[33mfd82467d14[m monitoring: add details to Prometheus' alerts
[33m33bb1bad74[m rgw: Adding 'iam' namespace for Role and User Policy related REST APIs.
[33m16790a881d[m mon/ConfigMonitor: clean out bad config records
[33maae3121b68[m mon/ConfigMonitor: make 'config get' on fsid work
[33mdda98e513f[m common/options: remove NO_MON_UPDATE from mon_dns_srv_name
[33mde2c3774fe[m mon/ConfigMonitor: do not 'config get' on NO_MON_UPDATE options
[33me9ec2eea86[m mon/ConfigMonitor: do not set NO_MON_UPDATE values
[33mdf0fee2679[m Merge pull request #32781 from idryomov/wip-doc-fs-authorize-fix-nautilus
[33m04110b4953[m common: fix deadlocky inflight op visiting in OpTracker.
[33m9de6523b93[m librbd: allow remove snapshot with child from non-existent pool
[33med207a143a[m rgw: fix radosgw-admin zone/zonegroup placement get command
[33m9331abc542[m osd/PeeringState.cc: don't let num_objects become negative
[33m2da6ab2dd8[m Merge pull request #32208 from smithfarm/wip-43280-nautilus
[33m375f56750b[m qa/suites/rados/thrash: force normal pg log length with cache tiering
[33m8a7f06d101[m osd/PeeringState.cc: skip peer_purged when discovering all missing
[33mc64beb68ef[m Merge pull request #32079 from smithfarm/wip-43143-nautilus
[33mb0b5d9119b[m common/config: update values when they are removed via mon
[33m563070dfaa[m os/bluestore/BlueStore.cc: set priorities for compression stats
[33m43ec36a389[m osd/OSD: enhance osd numa affinity compatibility
[33m8df71c790d[m osd/osd: set_numa_affinty: add an err log when it unable to          identify cluster interface's numa node
[33m62ecab00cb[m doc/rbd: documented 'rbd compression hint' config option
[33m61be332cdf[m librbd: support compression allocation hints to the OSD
[33md1693abb72[m librbd: skip stale child with non-existent pool for list_descendants
[33m75f4b31f33[m librbd: fix error param of ioctx when passing to `list_descendants`
[33m8fc22dc539[m rbd:the progress percent info exceeds 100%
[33md7d52d5d60[m librbd: remove pool objects when removing a namespace
[33m4e38db2494[m qa: kernel.sh: unlock before rolling back
[33m8a531b26e4[m qa: krbd_exclusive_option.sh: update for recent kernel changes
[33m73e6265e8a[m librbd: fix rbd_open_by_id, rbd_open_by_id_read_only
[33mb3d662aad4[m Merge pull request #32245 from smithfarm/wip-41106-nautilus
[33m0e32748dd6[m rgw: support radosgw-admin zone/zonegroup placement get command
[33m3abd3aed29[m rgw: fix opslog operation field as per Amazon s3
[33m41ebfb259d[m rgw: the http response code of delete bucket should not be 204-no-content
[33mfdd19a2ad1[m rgw: Incorrectly calling ceph::buffer::list::decode_base64 in bucket policy
[33m14069d135e[m Merge pull request #32746 from jan--f/wip-43026-nautilus
[33mfa32c7fded[m rgw: Select the std::bitset to resolv ambiguity
[33mffcaac8035[m selinux: Allow ceph to read udev db
[33ma68ca7df1f[m rgw: fix bugs in listobjectsv1
[33m8497f7c4c6[m rgw/pubsub: support eventId in push mode
[33m319543fb34[m rgw/pubsub: fix recerds/event json format to match documentation
[33mbd179359d9[m cls/rgw: when object is versioned and lc transition it, the object is becoming non-current
[33m7c8e940d51[m rgw: return error if lock log shard fails
[33ma075cd441e[m rgw: move forward marker even in case of many non-existent indexes
[33m4676b41ae2[m rgw: fix a bug that bucket instance obj can't be removed after resharding completed.
[33mda595d3097[m rgw:[bug] civetweb mg_write with offset.
[33m8cac737318[m rgw: build_linked_oids_for_bucket and build_buckets_instance_index should return negative value if it fails
[33mf845b0bca7[m rgw: data sync markers include timestamp from datalog entry
[33m9282dd32ae[m radosgw-admin: sync status displays id of shard furthest behind
[33m63df041ebd[m Merge pull request #32469 from zhengchengyao/diff_nautilus
[33m5229ce7b86[m Merge pull request #32299 from callithea/wip-42139-nautilus
[33md06252cad0[m Merge pull request #31980 from epuertat/wip-43103-nautilus
[33m752dd89c9e[m Merge pull request #31942 from ricardoasmarques/wip-43083-nautilus
[33mf7d352a438[m Merge pull request #31941 from ricardoasmarques/wip-42955-nautilus
[33m6d50e97dfb[m qa/tasks/ceph.conf.template: disable power-of-2 warning
[33mb2c0a7cfbf[m osd/OSDMap: remove remaining g_conf() usage
[33mac4489b521[m PendingReleaseNotes: add note for 14.2.5 so we can backport this
[33m4108c4657e[m osd/OSDMap: health alert for non-power-of-two pg_num
[33m8392c2cb89[m mgr/dashboard: fix improper URL checking
[33m7debe63241[m tests: add missing header cmath to test/mon/test_mon_memory_target.cc
[33m80f6f924a3[m mount.ceph: remove arbitrary limit on size of name= option
[33m2a9c6636e5[m Merge pull request #32123 from dvanders/dvanders_40891
[33mf0b8d0416e[m Merge pull request #32070 from smithfarm/wip-42993-nautilus
[33mbed6811601[m Merge pull request #32069 from smithfarm/wip-42999-nautilus
[33me6f57d4a23[m Merge pull request #32068 from smithfarm/wip-42695-nautilus
[33m70f5f0dea2[m Merge pull request #32733 from leseb/cv-raw-bp
[33m9f69935ab2[m src/rgw/rgw_rest_conn.h: fix build with clang
[33m5eb2f6dfe1[m doc/cephfs/client-auth: description and example are inconsistent
[33m611a86b9fd[m test: Fix wait_for_state() to wait for a PG to get into a state
[33md7bf2800e3[m osd: Use physical ratio for nearfull (doesn't include backfill resserve)
[33m74a2704f78[m mgr/MgrClient: fix open condition fix
[33m85f85d88cd[m mgr/MgrClient: fix open condition
[33m47afc1dfc7[m Merge pull request #31956 from dzafman/wip-balancer3-nautilus
[33m691d0019f2[m Merge pull request #31556 from jan--f/wip-42766-nautilus
[33m96fe1c9eaf[m rgw: apply_olh_log ignores RGW_ATTR_OLH_VER decode error
[33m959430aac2[m rgw: allow apply_olh_log to rewrite the same olh version
[33mf60a8823a2[m rgw: apply_olh_log filters out older instances of same epoch
[33m72caadf8a2[m rgw: factor out decode_olh_info()
[33m625b89b6dd[m cls/rgw: only promote instance entries if they sort after current instance
[33m760f94de8f[m test/rgw: add test_concurrent_versioned_object_incremental_sync
[33m4131535fa7[m rgw: adding mfa code validation when bucket versioning status is changed.
[33mf3dca3bfa3[m[33m ([m[1;31mupstream/cv-raw-bp[m[33m, [m[1;31morigin/cv-raw-bp[m[33m)[m ceph-volume: fix device unittest, mock has_bluestore_label, lint
[33m1015ab21d9[m qa/cephfs: test case for timeout config of individual session
[33m0bf4e1652c[m mds: add command that config individual client session
[33mee21e94cda[m Merge pull request #32743 from leseb/fast-shutdown-bkp
[33m555375f8a2[m doc/ceph-volume: docs for zfs/inventory
[33mb146300c8a[m Merge pull request #31554 from jan--f/wip-42703-nautilus
[33m59f7cec8ce[m Merge pull request #31553 from jan--f/wip-42741-nautilus
[33med8e7c1ead[m qa/standalone/ceph-helpers.sh: remove osd down check
[33m561503f591[m qa/standalone/ceph-helpers.sh: destroy_osd: mark osd down
[33m823359c76f[m osd: add osd_fast_shutdown option (default true)
[33md9516150d9[m Merge pull request #32078 from smithfarm/wip-43141-nautilus
[33m36ef173b90[m Merge pull request #32077 from smithfarm/wip-43138-nautilus
[33m20e1010dc1[m Merge pull request #32075 from smithfarm/wip-43001-nautilus
[33mdd4725d77e[m Merge pull request #32073 from smithfarm/wip-42949-nautilus
[33m87b3bcbe1c[m Merge pull request #32072 from smithfarm/wip-43170-nautilus
[33m590a5a7bdf[m Merge pull request #32233 from s0nea/wip-43159-nautilus
[33m1186ae178e[m ceph-volume: raw: activate: drop --all, --osd-id and --osd-fsid args
[33ma9275d1e12[m raw: fix activate args
[33ma450bcd0b6[m raw: --osd-fsid throughout
[33m658d20ad0e[m ceph-volume: add raw mode
[33mb5cde8128f[m ceph-volume: show devices with bluestore labels and unavailable
[33m24045889bf[m Merge pull request #31789 from rhcs-dashboard/wip-42808-nautilus
[33m92549965d0[m Merge pull request #31741 from joscollin/wip-42886-nautilus
[33mb229aa81ad[m Merge pull request #32071 from smithfarm/wip-42650-nautilus
[33m4a568653e8[m osd: Diagnostic logging for upmap cleaning
[33maaedc54eb4[m kv/RocksDBStore: break out of compaction thread early on shutdown
[33mb82b1d37fd[m kv/RocksDBStore: debug async compaction
[33m8a4e0468c0[m Merge pull request #31802 from batrick/i42943
[33mddcd3660d0[m Merge pull request #32065 from smithfarm/wip-42631-nautilus
[33m9a79117d52[m Merge pull request #32012 from rhcs-dashboard/wip-43123-nautilus
[33m33e8de1326[m Merge pull request #32128 from votdev/issue_43079
[33mbbff997b5f[m Merge pull request #31784 from rhcs-dashboard/wip-42773-nautilus
[33m17218e7c76[m Merge pull request #31999 from rhcs-dashboard/wip-43115-nautilus
[33m7f461f0227[m Merge pull request #32658 from tchaikov/nautilus-remove-seastar-test
[33mcca4b4b4d6[m mgr/dashboard: Standby Dashboards don't handle all requests properly
[33m39a725f728[m test: Sort pool list because the order isn't guaranteed from "balancer pool ls"
[33mcc96a4cb0c[m mgr: Change default upmap_max_deviation to 5
[33m64dc06fe3b[m osdmaptool: Add --upmap-active to simulate active upmap balancing
[33m3833001d1b[m doc: Add upmap options to osdmaptool man page and give example
[33m836d91bc43[m tools: osdmaptool document non-upmap options that were missing
[33me51e0896fd[m test: Fix test case for pool based balancing instead of rule batched
[33mc97fe42ab0[m Partially revert "mgr/balancer: balance pools with same crush_rule in batch"
[33m917a26416c[m Revert "tools: osdmaptool sync with balancer module behavior"
[33m37e4f81b88[m release note: Add pending release notes for already merged code
[33m6870d14103[m tools/osdmaptool.cc: do not use deprecated std::random_shuffle() the use of `std::random_shuffle()` was introduced by b946308 .
[33md5d34e6ece[m test: Add test case based on Xie script in commit comment
[33m731e293d88[m osd: ceph_pg_upmaps() use any overfull when there are still underfull
[33m98c12d24f7[m osd: Create more_underfull with below target that aren't in underfull
[33mf897ee7e09[m osd/OSDMap.cc: don't output over/underfull messages to lderr
[33m7fc25efc0f[m osd: calc_pg_upmaps() pick most overfull remap from try_pg_upmap()
[33m7090d66966[m osd: ceph_pg_upmaps() use max_deviation to determine perfect distribution
[33m19e1476801[m tools: osdmaptool sync with balancer module behavior
[33m976bfe0170[m tools: osdmaptool: Perform upmap calculation as ceph-mgr does
[33mb789357a75[m osd: For balancer crush needs the rule passed to get_parent_of_type()
[33mfaa78c2826[m osdmaptool: Match default max value of 10
[33mad4702f301[m tools: odsmaptool truncate target upmap file
[33m22c0df4a92[m mgr: Fix balancer print
[33ma17426504b[m Merge pull request #32063 from smithfarm/wip-42878-nautilus
[33ma28c9eb0e4[m Merge pull request #31850 from smithfarm/wip-43007-nautilus
[33m9421e94c7d[m Merge pull request #32059 from smithfarm/wip-41954-nautilus
[33m88e98e8221[m Merge pull request #31848 from smithfarm/wip-42989-nautilus
[33m55b67cc357[m Merge pull request #31782 from rhcs-dashboard/wip-42809-nautilus
[33m780379e711[m Merge pull request #31697 from k0ste/wip-42850-nautilus
[33mab58e6e1c2[m cmake: remove seastar tests from "make check"
[33m04cc5982d8[m mgr/dashboard: KeyError on dashboard reload
[33mfcfc9a1eed[m Merge pull request #32651 from tchaikov/nautilus-dts-8-on-arm64
[33m595768272d[m Merge pull request #31791 from rhcs-dashboard/wip-42935-nautilus
[33m8c8a9f0985[m install-deps,rpm: enable devtoolset-8 on aarch64 also
[33m6c02ab2302[m Merge pull request #31646 from batrick/i42818
[33m52a7a61618[m Merge pull request #31082 from smithfarm/wip-42279-nautilus
[33mf51c440152[m Merge pull request #30820 from liewegas/wip-alerts-nautilus
[33m96b75f2f86[m Merge pull request #30765 from smithfarm/wip-42129-nautilus
[33m69a041fece[m Merge pull request #30739 from smithfarm/wip-41849-nautilus
[33mfcda26abcb[m Merge pull request #30009 from smithfarm/wip-40126-nautilus
[33m83a4eb0130[m make-dist: drop Python 2/3 autoselect
[33m028ac37de6[m tools/setup-virtualenv.sh: do not default to python2.7
[33m038845c729[m Merge pull request #32593 from trociny/wip-43500-nautilus
[33mb92c268e50[m Merge pull request #32447 from trociny/wip-43428-nautilus
[33m4e4d48254f[m Merge pull request #32086 from dillaman/wip-42630-nautilus
[33m1b3add3a75[m Merge pull request #32167 from rzarzynski/wip-fips-zeroize-memset_bzero_nautilus
[33m001fc7f2b2[m Merge pull request #31332 from joscollin/wip-41182-nautilus
[33mbcfeb9bd0e[m Merge pull request #31302 from smithfarm/wip-42142-nautilus
[33m58e1b2c2b4[m Merge pull request #31084 from smithfarm/wip-42424-nautilus
[33m539925f62d[m Merge pull request #31083 from smithfarm/wip-42422-nautilus
[33m012ce5c557[m Merge pull request #31081 from smithfarm/wip-42158-nautilus
[33mf774f190ec[m Merge pull request #30769 from smithfarm/wip-41888-nautilus
[33m2ada75de49[m Merge pull request #30767 from smithfarm/wip-42147-nautilus
[33md54bf33bb8[m Merge pull request #30766 from smithfarm/wip-42145-nautilus
[33mc23b538a64[m Merge pull request #30764 from smithfarm/wip-42121-nautilus
[33m1b4644e4eb[m Merge pull request #30763 from smithfarm/wip-42040-nautilus
[33m64511369c2[m Merge pull request #30762 from smithfarm/wip-42035-nautilus
[33m916eec9f1f[m Merge pull request #30761 from smithfarm/wip-41899-nautilus
[33m8129d6e871[m Merge pull request #31518 from trociny/wip-42726-nautilus
[33md10de19562[m Merge pull request #32133 from dillaman/wip-43212-nautilus
[33m717718db83[m logrotate: also sighup rbd-mirror
[33mbcc2653783[m rbd-mirror: reopen all contexts logs on SIGHUP
[33m4064fd20ef[m rbd-mirror: delay local/remote rados initialization until context created
[33m6b316c1a09[m PendingReleaseNotes: reference new MGR profile cap support
[33mb81b01b9b0[m pybind/mgr: use custom exception to handle authorization failures
[33mead2d7c162[m qa/workunits/rbd: add permission tests for mgr profile
[33md90465ec74[m mgr: added placeholder 'osd' and 'mds' profiles
[33mc081a52a4f[m doc/rbd: add new 'profile rbd' mgr caps to examples
[33m6d25716475[m pybind/mgr: test session authorization against specific pools/namespaces
[33m3940af0598[m mgr: python modules can now perform authorization tests
[33m8ba79b836c[m mgr: validate that profile caps are actually valid
[33m6ae3efd2a0[m mgr: added 'profile rbd/rbd-read-only' cap
[33m2b7f0f9754[m mgr: support optional arguments for module and profile caps
[33mb8492f256a[m mgr: add new 'allow module' cap to MgrCap
[33mbbf037d6f7[m mon: dropped daemon type argument for MonCap
[33m647c0e5dac[m mgr: stop re-using MonCap for handling MGR caps
[33m0a0fcc7da4[m rbd-mirror: fix 'rbd mirror status' asok command output
[33m2aaed5705a[m Merge pull request #32520 from sseshasa/wip-43495-nautilus
[33m48a64afe8d[m Merge pull request #32056 from smithfarm/wip-42735-nautilus
[33m41bd3b5d4d[m Merge pull request #32055 from smithfarm/wip-42733-nautilus
[33m3896d7c8ea[m Merge pull request #31683 from smithfarm/wip-42840-nautilus
[33m741ae6a168[m Merge pull request #32058 from smithfarm/wip-43161-nautilus
[33m998499f569[m Merge remote-tracking branch 'origin/nautilus-saved' into nautilus
[33mb1bc0eacd8[m ceph-volume/test: patch VolumeGroups
[33m3effdddc1a[m ceph-volume: minor clean-up of `simple scan` subcommand help
[33mf0aa067ac7[m[33m ([m[1;33mtag: v14.2.6[m[33m)[m 14.2.6
[33mcf2f113d4e[m doc: update inf/infinite option for subvolume resize
[33m41c52396de[m doc: fs subvolume resize command
[33me31e7a0791[m doc: uid, gid for subvolume create and subvolumegroup create commands
[33me9a5afe5f3[m qa/tasks/cbt: install python3 deps
[33m9ca8059cf2[m mon/OSDMonitor: Don't update mon cache settings if rocksdb is not used
[33mdef29c0202[m Merge pull request #32436 from tchaikov/nautilus-cbt-py3
[33m53497b4252[m qa/tasks/cbt: install python3 deps
[33mdaf0990c19[m mon/PGMap.h: disable network stats in dump_osd_stats
[33ma96cc3144c[m osd_stat_t::dump: Add option for ceph-mgr pythonn callers to skip ping network
[33mfbfd30b61d[m Merge pull request #32466 from neha-ojha/wip-43364-2-nautilus
[33m5322502645[m Merge pull request #32064 from smithfarm/wip-42997-nautilus
[33m041e1544fa[m Merge pull request #32062 from smithfarm/wip-42853-nautilus
[33m65c088f8de[m Merge pull request #31740 from smithfarm/wip-42885-nautilus
[33m171524c874[m Merge pull request #31736 from SUSE/wip-42846-nautilus
[33m7087a7b0c9[m Merge pull request #32067 from smithfarm/wip-42899-nautilus
[33m7ced269721[m Merge pull request #31862 from smithfarm/wip-43012-nautilus
[33m99a3b3b6e0[m Merge pull request #31684 from smithfarm/wip-42841-nautilus
[33mf510ae1505[m Merge pull request #31298 from smithfarm/wip-42555-nautilus
[33m811e791b6d[m Merge pull request #31289 from smithfarm/wip-42386-nautilus
[33mc7398dcff8[m Merge pull request #31182 from croit/nautilus-rgw-bucket-stats-num-shards
[33m2016a9711d[m Merge pull request #32050 from joke-lee/rgw-sts-crash-token-not-base64-nautilus
[33m17e5dce17c[m Merge pull request #31844 from smithfarm/wip-42994-nautilus
[33m6c87800adf[m Merge pull request #31028 from smithfarm/wip-42197-nautilus
[33mb4aa1de70b[m Merge pull request #31852 from sseshasa/wip-41810-nautilus
[33m53fe05db20[m librbd: diff iterate with fast-diff now correctly includes parent
[33m479f6c16fb[m mon/PGMap.h: disable network stats in dump_osd_stats
[33m59f711526e[m osd_stat_t::dump: Add option for ceph-mgr pythonn callers to skip ping network
[33m6a8df77084[m Merge pull request #31295 from smithfarm/wip-42537-nautilus
[33m9daff0dcdc[m install-deps.sh: install boost-1.72 for bionic
[33m7118dbddeb[m common,rgw: workaround for boost 1.72
[33m8e75283e30[m make-dist: package boost v1.72 instead of v1.67
[33m1ae0c12453[m cmake: build boost v1.72 instead of v1.67
[33m45166774fe[m cmake: update FindBoost.cmake for 1.72
[33m79f5497b0f[m cmake: do not use CMP0093 unless it is supported
[33md174771d99[m cmake: do not use CMP0074 unless it is supported
[33m2690e682b1[m cmake: update FindBoost.cmake for 1.71
[33mfd2fdc3e78[m Merge pull request #32028 from smithfarm/wip-43140-nautilus
[33m8d4bb6dcd1[m Merge pull request #31779 from rhcs-dashboard/wip-42900-nautilus
[33m673a387d36[m nautilus: mgr/dashboard: fix RGW subuser auto-generate key. (#32240)
[33md0ccee1427[m Merge pull request #32216 from smithfarm/wip-43233-nautilus
[33mce470fbc51[m Merge pull request #31089 from smithfarm/wip-41634-nautilus
[33m08ffd7afda[m Merge pull request #30743 from smithfarm/wip-41978-nautilus
[33m09dd2c12c2[m Merge pull request #30733 from less-is-morr/wip-41636-nautilus
[33m91dd67bcc6[m Merge pull request #30741 from smithfarm/wip-41714-nautilus
[33mafd25bc5c7[m Merge pull request #31301 from smithfarm/wip-42134-nautilus
[33m0244ad269c[m Merge pull request #31359 from smithfarm/wip-42203-nautilus
[33m362dd639bb[m Merge pull request #31735 from SUSE/wip-42739-nautilus
[33mfcf9cdde0b[m Merge pull request #31367 from jan--f/wip-42400-nautilus
[33m9e82c65776[m Merge pull request #32283 from neha-ojha/wip-42913-nautilus
[33m3b0a63406b[m nautilus: mgr/dashboard: A block-manager can not access the po… (#31570)
[33m7d62a11d74[m Merge pull request #32254 from ideepika/wip-43316-nautilus
[33m9f1f458fea[m mgr/dashboard: iSCSI targets not available if any gateway is down
[33mbff2ab99c6[m Merge pull request #32035 from cbodley/wip-qa-rgw-swift-nautilus
[33m79f9295aa7[m Merge pull request #32229 from alimaredia/wip-s3-tests-branch-name-refactor-nautilus
[33mcb1d71cdc8[m nautilus: update s3-test download code for s3-test tasks
[33m75c53d08c0[m mgr/dashboard: Check if `num_sessions` is available
[33me091a5632f[m mgr/dashboard: Prevent deletion of iSCSI IQNs with open sessions
[33m324621e472[m os/bluestore: default bluestore_block_size 1T -> 100G
[33mc1aa1dfc9d[m nautilus: mgr/dashboard: Use serial RGW Admin OPS API calls (#31569)
[33m6b191010e0[m nautilus: mgr/dashboard: Disable event propagation in the help… (#31566)
[33m99e65c891b[m nautilus: mgr/dashboard: Remove title from sparkline tooltips (#31737)
[33me459be89a8[m doc/rados/operations: crush_rule is a name
[33m4af73bfb23[m Merge pull request #32248 from neha-ojha/wip-32197-nautilus
[33m42ade3c769[m qa: add krbd_get_features.t test
[33m683883b417[m qa: krbd_blkroset.t: update for read-only changes
[33m7fe6c3a472[m qa: avoid hexdump skip and length options
[33m51bc52998c[m doc/_templates/page.html: redirect to etherpad
[33mdfdfaaf030[m Merge pull request #31116 from smithfarm/wip-42462-nautilus
[33ma0ff2dafb2[m nautilus: mgr/dashboard: Fix e2e chromedriver problem (#32241)
[33mc383236268[m nautilus: mgr/dashboard: Hardening accessing the metadata
[33mca326724fd[m mgr/dashboard: Fix e2e chromedriver problem
[33mbc1c561cc8[m mgr/dashboard: fix RGW subuser auto-generate key.
[33m25a3118c61[m Merge pull request #31810 from bk201/wip-42948-nautilus
[33macef74bac6[m nautilus: mgr/dashboard: Update translations nautilus (#31759)
[33m2e3aec1044[m Merge pull request #31300 from smithfarm/wip-42259-nautilus
[33m64c4838ead[m install-deps.sh: install python2-{virtualenv,devel} on SUSE if needed
[33m5b2d8116ba[m qa: radosgw-admin: remove dependency on bunch package
[33m8cba1bdb33[m rpm: add rpm-build to SUSE-specific make check deps
[33m192fea3ce2[m mds: audit memset & bzero users for FIPS.
[33mf4423fa169[m osd: audit memset & bzero users for FIPS.
[33ma1dd8d3863[m osdc: audit memset & bzero users for FIPS.
[33m81a4253297[m librbd: audit memset & bzero users for FIPS.
[33m4d3e939034[m librados: audit memset & bzero users for FIPS.
[33mabe9fea507[m rgw: add some missed FIPS zeroization calls.
[33m7b061bce13[m rgw: switch to ceph::crypto::zeroize_for_security().
[33mc2a8a9de42[m rgw: audit memset & bzero users for FIPS.
[33ma81fbe8954[m rgw: fix indentation in parse_rgw_ldap_bindpw().
[33m0496f641d5[m msg/async: switch to ceph::crypto::zeroize_for_security().
[33m596f058fdf[m msg/async: audit memset & bzero users for FIPS.
[33mcccb3ec4e7[m common: switch to ceph::crypto::zeroize_for_security().
[33ma0cbd8fc6a[m common: audit memset & bzero users for FIPS.
[33md4f7430b62[m auth: audit memset & bzero users for FIPS.
[33m41caed22f6[m common: introduce ceph::crypto::zeroize_for_security().
[33m823c497465[m rgw: fix rgw crash when token is not base64 encode
[33m67d7221edc[m mgr/volumes: check for string values in uid/gid
[33m6c404da694[m mgr/dashboard: properly handle a missing rbd-mirror service status
[33m3166cd86bf[m mgr: cull service daemons when the last instance has been removed
[33m1a5288357e[m mgr: ensure new daemons are properly indexed by hostname
[33m0055fbdce2[m pybind / cephfs: remove static typing in LibCephFS.chown
[33m11811e612e[m nautilus: osd: set collection pool opts on collection create, pg load
[33m91c99b8550[m os/bluestore: Add config observer for osd memory specific options.
[33mad5bd132e1[m[33m ([m[1;33mtag: v14.2.5[m[33m)[m 14.2.5
[33me3616adab9[m Merge pull request #32082 from liewegas/workaround-py2-strptime-nautilus
[33m7ad7376974[m mgr/devicehealth: import _strptime directly
[33mbdc4f1e2b4[m mds: tolerate no snaprealm encoded in on-disk root inode
[33m480f3acb6e[m tools/cephfs: make 'cephfs-data-scan scan_links' fix dentry's first
[33m4144d789b9[m mds: remove unnecessary debug warning
[33m140aa533f7[m qa: fs Ignore getfattr errors for ceph.dir.pin
[33m91ecede104[m mds: properly evaluate unstable locks when evicting client
[33mfa63de6a7b[m qa: ignore RECENT_CRASH for multimds snapshot testing
[33m0566dfc76d[m mds: no assert on frozen dir when scrub path
[33m617f8642c0[m monitoring: fix indentation of ceph default alerts
[33m89e2630657[m monitoring: wait before firing osd full alert
[33me5135ee5b0[m mgr/pg_autoscaler: default to pg_num[_min] = 16
[33mc9af8d14a4[m pybind/mgr/pg_autoscaler: implement shutdown method
[33m6b5d902f34[m mgr/pg_autoscaler: only generate target_* health warnings if targets set
[33mb59318702b[m client: add warning when cap != in->auth_cap.
[33m9b03740011[m osd/PeeringState: do not exclude up from acting_recovery_backfill
[33m5402f29617[m Revert "osd/PG: avoid choose_acting picking want with > pool size items"
[33m339d57626d[m common/admin_socket: Increase socket timeouts
[33me49b1e5743[m osd/OSDMap: fix format error ceph osd stat --format json
[33m99d9af3a52[m rgw: url_encode prefixes if requested for List Objects
[33m22a149a425[m rgw: make encode_key a member of ListBucket
[33m784c18d669[m rgw: allow reshard log entries for non-existent buckets to be cancelled
[33m66d63176a8[m rgw: auto-clean reshard queue entries for non-existent buckets
[33ma67a270141[m Merge pull request #32045 from liewegas/fix-mon-autotune-leveldb-nautilus
[33m01671e19be[m mon/OSDMonitor: make memory autotune disable itself if no rocksdb
[33m4f4e1156ae[m Merge pull request #32018 from dzafman/wip-revert-verify-upmap-nautilus
[33ma4f15fc6ae[m qa/rgw: add missing force-branch: ceph-nautilus for swift tasks
[33m297ce6092a[m ceph-mon: keep v1 address type when explicitly set
[33mb2748bb42b[m Revert "crush: remove invalid upmap items"
[33m55cab2ef61[m mgr/dashboard: Cross sign button not working for some modals
[33mbab89482cc[m mgr/dashboard: grafana charts match time picker selection.
[33m6bf16bb6bb[m Merge pull request #31985 from liewegas/wip-fix-telemetry-log-typo-nautilus
[33me9bf010cbf[m mgr/telemetry: fix log typo
[33mbf24a5efb2[m Merge pull request #31974 from liewegas/wip-fix-crash-sorting-nautilus
[33mc64620c92b[m Merge pull request #31894 from smithfarm/wip-43030-nautilus
[33m11a10582f4[m mgr/dashboard,grafana: remove shortcut menu
[33m332d6aab44[m mgr/crash: fix 'crash ls[-new]' sorting
[33mb3a65ea53d[m mgr/dashboard: Trim IQN on iSCSI target form
[33mdef67fc52b[m mgr/dashboard: unable to set bool values to false when default is true
[33ma25793a2ae[m mgr/dashboard: remove traceback/version assertions
[33m10e674d59b[m doc: mention --namespace option in rados manpage
[33m6515fbbc64[m remove the restriction of address type in init_with_hosts when build initial monmap
[33mf9ec42b29d[m rgw: crypt: permit RGW-AUTO/default with SSE-S3 headers
[33mb0c6871103[m Merge pull request #31833 from dillaman/wip-42891-nautilus
[33m6856f696d0[m common/options: remove unused ms_msgr2_{sign,encrypt}_messages
[33ma1f6060952[m qa: kernel.sh: update for read-only changes
[33mc0ffcc721c[m Merge pull request #31822 from neha-ojha/wip-31657-nautilus
[33m57379c81a2[m cls/rbd: sanitize the mirror image status peer address after reading from disk
[33mdfd3c3f5e4[m Merge pull request #31812 from jan--f/wip-42965-nautilus
[33m9ae6f62efe[m osd: release backoffs during merge
[33m3d09f796f7[m ceph-volume: add mock dependency to tox.ini
[33m85517d3769[m  ceph-volume: python2 raises OSError on Popen with missing binary.
[33m2180097377[m ceph-volume: py2 compatibility for selinux test
[33m5523db2a60[m ceph-volume: don't assume SELinux
[33m61d113cf11[m ceph-volume: fix test test_selinuxenabled_doesnt_exist
[33md49e8f09ff[m mgr/dashboard: test_mgr_module QA test has not been adapted to latest controller changes
[33m0e8e9a872b[m mgr/dashboard: fix restored RBD image naming issue
[33m8e14bf4224[m mgr/dashboard: open files with UTF-8 encoding in Grafana checking script
[33m804990b3de[m mgr/dashboard: check embedded Grafana dashboard references
[33mda45b4e7ca[m mds: release free heap pages after trim
[33ma83e839b21[m mgr/dashboard: Dashboard can't handle self-signed cert on Grafana API
[33mbee5aa8d65[m mgr/dashboard: sort monitors by open sessions correctly.
[33m9c6866bccf[m mgr/dashboard: refactor TableKeyValueComponent
[33m2d1f9a524a[m mgr/dashboard: key-value-table doesn't render booleans
[33m8718b4668e[m mgr/dashboard: Update translation
[33ma5a88763ad[m mgr/dashboard: Remove compression mode unset in pool from
[33m1a9b150ad6[m mgr/dashboard: Handle always-on Ceph Manager modules correctly
[33m2556604306[m mgr/dashboard: show Rename in modal header & button when renaming RBD snapshot
[33m9989c20373[m Merge PR #30521 into nautilus
[33m5d6c2f8ae5[m Merge pull request #31733 from epuertat/wip-42677-nautilus
[33mfbad25bd86[m Merge pull request #30229 from vumrao/wip-vumrao-bluefs-shared-alloc-with-log-level-change-nautilus
[33m76cb218d11[m Merge pull request #31704 from smithfarm/wip-42858-nautilus
[33m4bca7d355a[m Merge pull request #31676 from tchaikov/nautilus-42832
[33mfd07f68943[m Merge pull request #31526 from ricardoasmarques/wip-42730-nautilus
[33m2f62e838b9[m Merge pull request #30910 from rjfd/wip-42283-nautilus
[33mb6e05a910f[m Merge pull request #30685 from ifed01/wip-ifed-fast-fsck-nau
[33m5995bc10dd[m test/rbd_mirror: fix mock warnings
[33maeb0a12bd0[m Merge pull request #31742 from smithfarm/wip-42836-nautilus
[33m9d69c14c6f[m rgw: drop getting list-type when get_data is false
[33me66bcf6412[m rgw: Silence warning: control reaches end of non-void function
[33mc60c9b4ada[m qa/tasks: uid, gid for subvolume create and subvolumegroup create commands
[33m1bb0fbb482[m cephfs: chown function
[33mee853e26b0[m mgr/volumes: uid, gid for subvolume create and subvolumegroup create commands
[33m693d6fe278[m Merge pull request #31522 from ricardoasmarques/wip-42729-nautilus
[33m37288bb458[m Merge pull request #31516 from rhcs-dashboard/42694-filter-non-pool-fields
[33mf1cf19e612[m Merge pull request #31413 from smithfarm/wip-41980-nautilus
[33m1ced1f5830[m Merge pull request #31375 from tspmelo/wip-42150-nautilus
[33m77040ae242[m mon/OSDMonitor : Fix pool set taget_size_bytes
[33m41339c094b[m Merge pull request #31349 from rhcs-dashboard/42589-edit-image-after-data-received
[33mf7e3f75aaf[m Merge pull request #31263 from ricardoasmarques/wip-42295-nautilus
[33m5addafca24[m Merge pull request #31160 from Exotelis/wip-42482-nautilus
[33m92c425c401[m os/bluestore/BlueFS: Move bluefs alloc size initialization log message to log level 1 Fixes: https://tracker.ceph.com/issues/41399
[33m17e6bbe0e7[m os/bluestore/BlueFS: apply shared_alloc_size to shared device
[33mcf9c1dc50b[m os/bluestore/BlueFS: fix device_migrate_to_* to handle varying alloc sizes
[33m626d2cbd3a[m os/bluestore: whitespace
[33m1ff043ed30[m os/bluestore: cleanup around allocator calls
[33m309726cc58[m os/bluestore/BlueFS: add bluefs_shared_alloc_size
[33mdf7631815f[m os/bluestore/BlueStore.cc: start should be >= _get_ondisk_reserved()
[33m02c8b069f9[m mgr/dashboard: Remove title from sparkline tooltips
[33m8a33561b54[m src/msg/async/net_handler.cc: Fix compilation
[33maa035cfcf6[m mgr/devicehealth: ensure we don't store empty objects
[33mc3d7cc9e5a[m mgr/dashboard: fix grafana dashboards
[33m3f36f355de[m nautilus: mgr/dashboard: Fix data point alignment in MDS count… (#31535)
[33meaaa82a39c[m Merge pull request #31658 from smithfarm/wip-42731-nautilus
[33m568a5dc03e[m Merge pull request #31604 from smithfarm/wip-42782-nautilus
[33m358e8dc2f3[m Merge pull request #31444 from dzafman/wip-41785
[33ma4a958bf0e[m Merge pull request #31261 from ceph/wip-nautilus-restful-node-items
[33mfb1656f038[m Merge pull request #31125 from smithfarm/wip-42439-nautilus
[33m8d3a6c409e[m Merge pull request #31011 from smithfarm/wip-42401-nautilus
[33mcbaa135ce4[m Merge pull request #30740 from smithfarm/wip-41463-nautilus
[33m6bf44037cb[m Merge pull request #31682 from dzafman/wip-42432-nautilus
[33mb6877ff2df[m Merge pull request #31077 from smithfarm/wip-42141-nautilus
[33mb077ca8cb6[m Merge pull request #30900 from smithfarm/wip-42136-nautilus
[33ma99b2a4940[m Merge pull request #31136 from wjwithagen/wip-fixup-cephfs-nautilus
[33ma2a063ee14[m Merge pull request #30951 from sidharthanup/mds-evict-duplicate-nautilus
[33mcc7bcd90b2[m Merge pull request #30043 from smithfarm/wip-41488-nautilus
[33mfa483118e7[m Merge pull request #29832 from pdvian/wip-41095-nautilus
[33mbcb1e27f2e[m Merge pull request #29811 from pdvian/wip-41093-nautilus
[33m2332b7b408[m Merge pull request #29750 from pdvian/wip-41087-nautilus
[33m49ae3bf577[m Merge pull request #31650 from uweigand/nautilus-backport-28013
[33mfc63e54c08[m Merge pull request #31079 from smithfarm/wip-42155-nautilus
[33macf5aa6dd9[m ceph.in: normalize BOOL values found by get_cmake_variables()
[33mdbc82da353[m os/bluestore: test conversion from global statfs
[33m810df58e91[m os/bluestore: simplify per-pool-stat config options
[33ma8b867c33d[m os/bluestore: fix invalid stray shared blob detection in fsck.
[33md5d6baf520[m os/bluestore: fix warning counting in fsck
[33md8d79b6e12[m os/bluestore: remove duplicate logging
[33mc3a8493729[m os/bluestore: introduce 'fix_on_mount' mode for no per pool tolerance.
[33mc1178c4ffe[m os/bluestore: parallelize quick-fix running.
[33mcef4e41bf2[m common/work_queue: make ThreadPool/WorkQueue more reusable.
[33mf8711f30a0[m os/bluestore: pass containers by ptr in FSCK_ObjectCtx
[33m7a2ebe7f7f[m os/bluestore: another split out for fsck logic blocks.
[33m40068127a9[m os/bluestore: reorder blocks within fsck_check_objects.
[33m7c99874a5f[m os/bluestore: split out object checking from _fsck.
[33mfefcabb910[m os/bluestore: fsck: int64_t for error count
[33m18872f6752[m os/bluestore: introduce shallow (quick-fix) mode for bluestore fsck/repair.
[33m59b4c68384[m os/bluestore: do not collect onodes when doing fsck.
[33mb242bdc70c[m mon/PGMap: fix incorrect pg_pool_sum when delete pool
[33m901c37b58b[m pybind/mgr: Cancel output color control
[33m95bbbc09d6[m Merge pull request #30144 from ifed01/wip-ifed-gc-blobs-nau
[33m43f3f3295c[m Merge pull request #31397 from jan--f/wip-41460-nautilus
[33mec2eba70e9[m Merge pull request #30774 from smithfarm/wip-42144-nautilus
[33mf59b3b8ee0[m Merge pull request #30624 from tchaikov/wip-nautilus-41983
[33mabc9feb72a[m Merge pull request #30622 from smithfarm/wip-42083-nautilus
[33m554fc28498[m Merge pull request #30390 from pdvian/wip-41804-nautilus
[33m23b273ba23[m Merge pull request #30370 from pdvian/wip-41766-nautilus
[33mc9e69a1dd7[m Merge pull request #30360 from yuvalif/allow_gcc9_compilation
[33mcd0c1def15[m Merge pull request #30261 from smithfarm/wip-41724-nautilus
[33m4b306d7a80[m Merge pull request #30215 from sebastian-philipp/nautilus-k8sevents
[33md364b2f1bb[m Merge pull request #30016 from smithfarm/wip-41509-nautilus
[33m0392ece3bf[m Merge pull request #30006 from smithfarm/wip-40043-nautilus
[33mf1bba2e270[m Merge pull request #29949 from smithfarm/wip-41282-nautilus
[33m82232800ff[m Merge pull request #30046 from smithfarm/wip-40270-nautilus
[33mc338d6c82b[m Merge pull request #30089 from tchaikov/wip-nautilus-gcc-8
[33m2650e87907[m Merge PR #30008 into nautilus
[33m997ee60198[m Merge PR #30851 into nautilus
[33m4ec27e6bf5[m Merge PR #30849 into nautilus
[33md61e57f377[m Merge pull request #31605 from rhcs-dashboard/42795-fixtypo
[33mc8a0c9c30d[m Merge pull request #31334 from LongDuncan/nautilus
[33m37e9634f1a[m Merge pull request #31304 from smithfarm/wip-42545-nautilus
[33md6656467e3[m Merge pull request #31040 from smithfarm/wip-41495-nautilus
[33mb9c78bc5bf[m Merge pull request #31019 from ifed01/wip-42041-nautilus
[33mb8128b55b3[m Merge pull request #30983 from tchaikov/wip-nautilus-42363
[33mf66d4793c2[m Merge pull request #31097 from smithfarm/wip-42395-nautilus
[33m0f2aea1d1b[m Merge pull request #29592 from penglaiyxy/wip-simple-messenger
[33m78ebf08526[m RGW: fix an endless loop error when to show usage
[33ma06039de57[m rgw/rgw_reshard: Don't dump RGWBucketReshard JSON in process_single_logshard
[33m91974d297a[m rgw: when resharding store progress json only when both verbose and out are specified.
[33m1813f25907[m mgr/dashboard: fix mgr module API tests
[33m4905fe035c[m qa/tasks/mgr/dashboard/test_mgr_module: remove enable/disable test from MgrModuleTelemetryTest
[33m18a4a76d44[m qa/tasks/mgr/dashboard/test_mgr_module: sync w/ telemetry
[33mf7b90a1d4f[m mgr/dashboard/qa: add more fields to report
[33m9c79d877fe[m mgr: Improve balancer module status
[33mb5f2a12bbe[m test: Test balancer module commands
[33maa9f4e7893[m mgr: Release GIL before calling OSDMap::calc_pg_upmaps()
[33m15f360dd17[m qa/mgr/balancer: Add cram based test for altering target_max_misplaced_ratio setting
[33mfe63922030[m rgw: use explicit to_string() overload for boost::string_ref
[33mb028ce38af[m ceph.in: do not preload asan even if not needed
[33m652ffb14dd[m ceph.in: do not preload libasan if it is not found
[33m19b5ed7013[m ceph.in: disable ASAN if libasan is not found
[33m12409f8e8a[m ceph.in: only preload asan library if it is enabled
[33m74cc775490[m ceph.in: use get_cmake_variables(*args)
[33mc6e859a24c[m doc/mgr/crash: document missing commands, options
[33ma9844787a1[m qa/suites/rados/singleton/all/test-crash: whitelist RECENT_CRASH
[33meab609ccf1[m qa/suites/rados/mgr/tasks/insights: whitelist RECENT_CRASH
[33m4e5c7659b3[m qa/tasks/mgr/test_insights: crash module now rejects bad crash reports
[33md1b97f7949[m mgr/crash: don't make these methods static
[33m40562f31f0[m mgr/BaseMgrModule: handle unicode health detail strings
[33mf1d3c7454e[m mgr/crash: verify timestamp is valid
[33m6cf17d6709[m qa/suites/mgr: whitelist RECENT_CRASH
[33m17beda1356[m mgr/crash: remove unused var
[33m616594fa67[m mgr/crash: remove unused import 'six'
[33m2c45e742bb[m qa/workunits/rados/test_crash: health check
[33mc3134abb14[m mgr/crash: improve validation on post
[33m3a9d1949c3[m mgr/crash: automatically prune old crashes after a year
[33m416b987b9f[m mgr/crash: raise RECENT_CRASH warning for recent (new) crashes
[33mf6559fbb03[m mgr/crash: add 'crash ls-new'
[33m41662fce4b[m mgr/crash: add option and serve infra
[33m3d35ac5c19[m mgr/crash: keep copy of crashes in memory
[33m5543836fd1[m mgr/pg_autoscaler: adjust style to match built-in tables
[33m935292b70d[m mgr/crash: make 'crash ls' a nice table with a NEW column
[33m5968d40167[m mgr/crash: nicely format 'crash info' output
[33m2a2b3c2a8a[m mgr/crash: add 'crash archive <id>', 'crash archive-all' commands
[33m364f39c567[m Merge branch 'nautilus' into wip-device-telemetry-nautilus
[33m527b1366b0[m Merge PR #30755 into nautilus
[33ma71ae690fe[m Merge PR #30773 into nautilus
[33m59f1d66daa[m Merge PR #31099 into nautilus
[33mf52915caf4[m Merge PR #31100 into nautilus
[33m712e8d19c4[m Merge PR #31411 into nautilus
[33maadd0087ec[m Merge PR #31446 into nautilus
[33m23fa90fafc[m Merge PR #31612 into nautilus
[33m5d7d812d5c[m Merge PR #31644 into nautilus
[33mc2d7480a39[m ceph-objectstore-tool: return 0 if incmap is sane
[33maffb632cf1[m Merge pull request #31565 from tspmelo/wip-42747-nautilus
[33m77a93fa3ca[m qa/tasks: fs subvolume resize inf command
[33md5a0ddb86f[m mgr/volumes: fs subvolume resize inf/infinite command
[33m7d9694cd22[m qa/tasks: tests for resize subvolume
[33m400e4462a5[m mgr/volumes: fs subvolume resize command
[33m9bd65a88ae[m mon/MonMap: encode (more) valid compat monmap when we have v2-only addrs
[33md7af6c7069[m global: disable THP for Ceph daemons
[33m8541e60f59[m Merge PR #30827 into nautilus
[33me21d92e9f6[m Merge PR #31076 into nautilus
[33m63bf1da0fd[m Merge PR #30768 into nautilus
[33m883d9169d5[m Merge pull request #31641 from liewegas/fix-42783-nautilus
[33m8758697a95[m Revert "rocksdb: enable rocksdb_rmrange=true by default"
[33m117c8b5ca0[m os/bluestore: consolidate extents from the same device only
[33m31ab1a3de0[m qa: have kclient tests use new mount.ceph functionality
[33mb3de34a308[m doc: document that the kcephfs mount helper will search keyring files for secrets
[33m9060cf94fc[m mount.ceph: fork a child to get info from local configuration
[33m35a1d4e75d[m mount.ceph: track mon string and path inside ceph_mount_info
[33m867c2dfea9[m mount.ceph: add name and secret to ceph_mount_info
[33m4fdf75ee91[m mount.ceph: add ceph_mount_info structure
[33mcdfd11f879[m mount.ceph: clean up debugging output and error messages
[33mfe7da6139b[m mount.ceph: clean up return codes
[33mb5a1f2b138[m mount.ceph: add comment explaining why we need to modprobe
[33me153b2a5e5[m mount.ceph: use bools for flags
[33m7815d9014b[m common: have read_secret_from_file return negative error codes
[33me2d6413c62[m qa/tasks/ceph.conf.template: increase mon tell retries
[33mf47ded38b5[m cmake: Allow cephfs and ceph-mds to be build when building on FreeBSD
[33md1b69a3726[m Merge pull request #31248 from smithfarm/wip-42562-nautilus
[33m146a84058c[m qa: enable dashboard tests to be run with "--suite rados/dashboard"
[33m3ab1b98c2d[m Merge pull request #31628 from yuvalif/nautilus-backport-of-42497
[33m266b424bf7[m rgw/amqp: remove a test that requires syncronization betwen the amqp manager internal thread and the test itself amqp reconnect should be tested by system test instead
[33m16f68715e3[m osd: Use lock_guard for sched_scrub_lock like in master
[33mfe478d4362[m osd: Replace active/pending scrub tracking for local/remote
[33ma8438a4b52[m osd: Add dump_scrub_reservations to show scrub reserve tracking
[33m0941b50edf[m osd: Rename dump_reservations to dump_recovery_reservations
[33mc9fc97a042[m Merge pull request #30899 from smithfarm/wip-42126-nautilus
[33m0129f80d5f[m Merge pull request #31482 from callithea/wip-42682-nautilus
[33mefbe6e89e1[m Merge pull request #30696 from uweigand/nautilus-z-build
[33m6f53243011[m Merge pull request #30419 from ceph/wip-41238-nautilus
[33ma5f8f417e1[m Merge pull request #29961 from smithfarm/wip-41130-nautilus
[33m091e8a7f64[m Merge pull request #29960 from smithfarm/wip-41125-nautilus
[33m8963748735[m Merge pull request #29959 from smithfarm/wip-41119-nautilus
[33ma2ef1f1456[m Merge pull request #29956 from smithfarm/wip-41109-nautilus
[33m4fddeb5044[m Merge pull request #30941 from vumrao/wip-vumrao-42326
[33m1cf4fde13a[m Merge pull request #30923 from vumrao/wip-vumrao-42242
[33m1b3cad47c4[m Merge pull request #29748 from dzafman/wip-40840
[33mfd6f15f5ed[m Merge pull request #29649 from pdvian/wip-40944-nautilus
[33m8e3e2a1f38[m Merge pull request #29550 from pdvian/wip-40878-nautilus
[33m7e6cb62365[m Merge pull request #30546 from pdvian/wip-41917-nautilus
[33m0cb661a071[m Merge pull request #30528 from smithfarm/wip-42014-nautilus
[33m58226068cb[m Merge pull request #30524 from liewegas/bugfix-40716-nautilus
[33maa13fb3371[m Merge pull request #30486 from sobelek/wip-41921-nautilus
[33mf0697a9af5[m Merge pull request #30480 from pdvian/wip-41862-nautilus
[33m918fe79765[m Merge pull request #30371 from pdvian/wip-41712-nautilus
[33m718802b03a[m Merge pull request #30280 from smithfarm/wip-41640-nautilus
[33m9692aa0e1d[m Merge pull request #31027 from smithfarm/wip-42182-nautilus
[33md5947d6cd9[m Merge pull request #31026 from smithfarm/wip-41981-nautilus
[33m123a795f62[m Merge pull request #30869 from smithfarm/wip-41323-nautilus
[33m47b10db4df[m Merge pull request #31073 from smithfarm/wip-42281-nautilus
[33m514d820b28[m Merge pull request #30999 from cbodley/wip-40630
[33m59c0b7aa67[m Merge pull request #30746 from IlsooByun/rgw_race_condition
[33m425aa32455[m Merge pull request #29785 from pdvian/wip-41090-nautilus
[33me6a3b23913[m Merge pull request #29784 from pdvian/wip-41091-nautilus
[33mb869b741d6[m Merge pull request #29772 from smithfarm/wip-41350-nautilus
[33md6916aa0b3[m ceph-volume: assume msgrV1 for all branches containing mimic
[33md32a2ecf13[m common: fix typo in rgw_user_max_buckets option long description.
[33mf35e5f1252[m Merge pull request #31555 from jan--f/wip-42755-nautilus
[33m661a008fe5[m tests: test_librados_build.sh: grab from nautilus branch in nautilus
[33m337172633c[m Merge pull request #31576 from ricardoasmarques/wip-42745-nautilus
[33m62afd012cb[m Merge pull request #30195 from dzafman/wip-network-nautilus
[33mef7e78227a[m Merge pull request #30904 from smithfarm/wip-42152-nautilus
[33m5b6554edc7[m mgr/dashboard: Provide the name of the object being deleted
[33m4e2edd59ce[m mgr/dashboard: Wait for breadcrumb text is present in e2e tests
[33m7454edd2c8[m mgr/dashboard: cheroot moved into a separate project
[33mdba5920365[m mgr/dashboard: A block-manager can not access the pool page
[33m18ccf7b067[m Merge pull request #29905 from croit/backport-26538
[33mff84110161[m mgr/dashboard: Use serial RGW Admin OPS API calls
[33m1cc98f5514[m mgr/dashboard: Disable event propagation in the helper icon
[33m4ab987c913[m mgr/dashboard: Improve position of MDS chart tooltip
[33me60e7335bd[m mgr/dashboard: Fix data point alignment in MDS counters chart
[33ma8a48b97d9[m nautilus: mgr/dashboard: Fix calculation of PG Status percenta… (#30394)
[33mecd9cf638d[m Merge pull request #30516 from tspmelo/wip-41773-nautilus
[33m2085ea133f[m nautilus: mgr/dashboard: NFS list should display the "Pseudo P… (#30529)
[33m1d053e111c[m Merge pull request #30691 from sobelek/wip-42163-nautilus
[33m6287fddf66[m ceph-volume: add proper size attribute to partitions
[33m99271c148d[m mgr/prometheus: initializing osd_dev_node = None
[33m311723023f[m mgr/prometheus: assign a value to osd_dev_node when obj_store is not filestore or bluestore
[33md87ae4e2c7[m ceph-volume tests validate restorecon skip calls
[33m9befc77b87[m ceph-volume util.system allow skipping restorecon calls
[33m51b0d7b3d0[m ceph-volume: reject disks smaller then 5GB in inventory
[33m04d8b21d0b[m ceph-volume: use fsync for dd command
[33mcb8144184d[m Merge pull request #29954 from smithfarm/wip-40597-nautilus
[33m958d1b8946[m Merge pull request #31075 from smithfarm/wip-42427-nautilus
[33m2df6575e9d[m Merge pull request #29955 from smithfarm/wip-40849-nautilus
[33m66d81cae4d[m Merge pull request #29898 from smithfarm/wip-41264-nautilus
[33me51a769453[m Merge pull request #29849 from theanalyst/nautilus-listv2
[33ma556d829e1[m Merge pull request #29803 from ivancich/nautilus-rgw-housekeeping-reset-stats
[33m5a071f3d2e[m Merge pull request #29777 from ivancich/nautilus-rgw-bucket-list-max-entries
[33m3a67a06c0e[m qa/workunits/rbd: test mirrored snap trash removal
[33mc0dc9345d4[m rbd-mirror: allow proxied trash snap remove for local image
[33md6840dc3ce[m librbd: introduce a way to allow proxied trash snap remove
[33mf3eea7032e[m librbd: do not try to open parent journal when removing trash snapshot
[33m5fe4a90b29[m mgr/dashboard: tasks: only unblock controller thread after TaskManager thread
[33m9f634d8532[m mgr/dashboard: RBD tests must use pools with power-of-two pg_num
[33ma3114e97c9[m nautilus: mgr/dashboard: Validate iSCSI controls min/max value… (#30545)
[33m73de9cffb5[m rbd-mirror: mirrored clone should be same format
[33m42f7af560e[m librbd: allow to override format in clone request
[33md9215112ac[m mgr/dashboard: do not show non-pool data in pool details
[33m8b2a50bf07[m mgr/prometheus: return FQDN for default server_addr
[33md9ba311d31[m mgr/dashboard: return FQDN for default server_addr
[33mdd7922d936[m Merge pull request #31408 from smithfarm/wip-42532-nautilus
[33m684deed088[m Merge pull request #31405 from smithfarm/wip-42535-nautilus
[33m591ee56688[m Merge pull request #30643 from pdvian/wip-41958-nautilus
[33m8a13789f45[m Merge pull request #30648 from trociny/wip-42095-nautilus
[33mfb45e63898[m Merge pull request #30783 from smithfarm/wip-41920-nautilus
[33m61655c2921[m Merge pull request #30116 from smithfarm/wip-41440-nautilus
[33m4f7cc6195c[m Merge pull request #29978 from smithfarm/wip-41441-nautilus
[33m114456d5fa[m Merge pull request #29871 from trociny/wip-41422-nautilus
[33mc8471d866b[m Merge pull request #29870 from trociny/wip-41286-nautilus
[33m968c23606d[m Merge pull request #29869 from trociny/wip-41420-nautilus
[33mbe96295573[m Merge pull request #31470 from dillaman/wip-41629-nautilus
[33m963b2939e1[m Merge pull request #30821 from dillaman/wip-41972-nautilus
[33m3d4bbd8b76[m Merge pull request #30818 from dillaman/wip-rbd-parent-namespace-nautilus
[33md82241ce17[m librbd: workaround an ICE of GCC
[33mb95081caf5[m qa/workunits/rbd: add 'remove mirroring pool' test
[33m7c564605d0[m librbd: behave more gracefully when data pool removed
[33mfdcd3e21f8[m librados: add IoCtx::is_valid method to test if IoCtx was initialized
[33me5faeec3da[m Merge pull request #30822 from dillaman/wip-41968-nautilus
[33m7a0f18d3bc[m Merge pull request #31468 from ceph/revert-30824-wip-41629-nautilus
[33md6b3025a27[m crush: remove invalid upmap items
[33m3326a59440[m Revert "nautilus: librbd: behave more gracefully when data pool removed"
[33mc80974a5b4[m mgr/dashboard: Fix error on ceph-iscsi version pre controls_limits
[33m910b84007d[m mgr/dashboard: Set iSCSI disk WWN and LUN number from the UI
[33m0f7b514051[m Merge pull request #30844 from liewegas/wip-crash-nautilus
[33me6c1cb3518[m mgr/dashboard: Set RO as the default access_type for RGW NFS exports
[33m265e0970f7[m Merge pull request #30823 from dillaman/wip-42204-nautilus
[33m231a0f49d1[m Merge pull request #30824 from dillaman/wip-41629-nautilus
[33m68d53c64a6[m Merge pull request #30825 from dillaman/wip-41883-nautilus
[33m2e04e7b2fa[m Merge pull request #30948 from dillaman/wip-42333-nautilus
[33m27e329a82f[m mgr/dashboard: Display WWN and LUN number in iSCSI target details
[33mbcbe499ca8[m nautilus: mgr/dashboard: Unify button/URL actions naming for i… (#29510)
[33m16d86fd877[m nautilus: mgr/dashboard: change bucket owner between owners fr… (#29485)
[33md592e56e74[m mgr/devicehealth: do not scrape mon devices
[33m4eaa54b37c[m Merge pull request #31012 from smithfarm/wip-42392-nautilus
[33md9e80bacee[m Merge pull request #31031 from smithfarm/wip-42200-nautilus
[33mc68a6bb8f1[m Merge pull request #31034 from smithfarm/wip-40504-nautilus
[33m54390e0912[m Merge pull request #31037 from smithfarm/wip-41548-nautilus
[33m666c5cefd9[m Merge pull request #31038 from smithfarm/wip-41705-nautilus
[33m6dde4ef50c[m Merge pull request #31039 from smithfarm/wip-42125-nautilus
[33me5e7b9ffd8[m Merge pull request #31111 from tchaikov/nautilus/42455
[33m516bfd8b89[m nautilus: mgr/dashboard: qa: whitelist client eviction warning (#31114)
[33mcfdd389102[m PendingReleaseNotes: fix typo
[33m134a14f32c[m PendingReleaseNotes: remove kludge
[33ma06a97aad1[m mgr/telemetry: add stats about crush map
[33mde0c224bc9[m mgr/telemetry: add rgw metadata
[33m02c0509b0a[m mgr/telemetry: include fs size (files, bytes, snaps)
[33me3523928c8[m mds: report r{files,bytes,snaps} via perfcounters
[33m0886ff9875[m mgr/telemetry: mds cache stats
[33m04a4a926fd[m mgr/telemetry: add some rbd metadata
[33m970bcdf239[m mgr/telemetry: note whether osd cluster_network is in use
[33m55006a6ef3[m mgr/telemetry: add host counts
[33mcfc56ffc50[m mgr/telemetry: add more pool metadata
[33m31ff782e56[m mgr/telemetry: remove crush rule name
[33m7325b1600f[m mgr/telemetry: include min_mon_release and msgr v1 vs v2 addr count
[33m4dff975047[m mgr/telemetry: add CephFS metadata
[33ma59f514662[m mgr/telemetry: include balancer info (active=true/false, mode)
[33meb0a438258[m mgr/telemetry: include per-pool pg_autoscale info
[33m8b79ecaad6[m mgr/telemetry: dict.pop() errs on nonexistent key
[33mf23290a32c[m mgr/telemetry: send device telemetry via per-host POST to device endpoint
[33m639bd48c1e[m mgr/telemetry: fix remote into crash do_ls()
[33mb51c5e3ddb[m mgr/telemetry: clear the event after being awaken by it
[33ma3a31569f2[m mgr/telemetry: bump content revision and add a release note
[33ma0c188ca6a[m telemetry/server: add device report endpoint
[33me7245099ce[m mgr/telemetry: include device telemetry
[33m9cf71a2039[m mgr/telemetry: salt osd ids too
[33m8e7e3b8e37[m mgr/telemetry: obscure entity_name with a salt
[33m6da5bc78c9[m mgr/telemetry: force re-opt-in if the report contents change
[33m57649b30fe[m mgr/telemetry: less noise in the log
[33mf6cdb23175[m mgr/telemetry: wake up serve on config change
[33m12ab0fa91d[m mgr/telemetry: track telemetry report revisions
[33mc3f52b54d2[m qa/tasks/mgr/dashboard/test_mgr_module: adjust expected schema
[33m3855a1793a[m mgr/telemetry: separate out cluster config vs running daemons
[33mfc7dcc38e0[m mgr/telemetry: include any config options that are customized
[33m856f030461[m mgr/telemetry: specify license when opting in
[33m7b3af7eca0[m doc/mgr/telemetry: update
[33md3fdba45c6[m mgr/telemetry: move contact info to an 'ident' channel
[33m373eb73058[m mgr/telemetry: accept channel list to 'telemetry show'
[33m89fa71952b[m mgr/telemetry: always generate new report for 'telemetry show'
[33mbd284663bc[m mgr/telemetry: add 'device' channel and call out to devicehealth module
[33mcb0b5c5096[m mgr/telemetry: add telemetry channel 'device'
[33m756e793605[m mgr/telemetry: add separate channels
[33mce8567aa54[m mgr/devicehealth: pull out MAX_SAMPLES
[33m2973a22d22[m Merge pull request #30889 from yuvalif/nautilus-backport-42042
[33m212750b05d[m mgr/dashboard: do not log tokens
[33mb630bb9505[m mgr/ActivePyModules: behave if a module queries a devid that does not exist
[33m409b6d751a[m Merge pull request #30259 from LenzGr/wip-41604-nautilus
[33m94a932df7b[m nautilus: mgr/dashboard: Allow the decrease of pg's of an exis… (#30376)
[33m348c8d7eb4[m nautilus: mgr/dashboard: Allow disabling redirection on standb… (#30382)
[33m079331bd36[m ceph-volume: make get_pv() accepts list of PVs
[33m42d027ddb4[m ceph-volume: make get_vg() accepts list of VGs
[33md32b3455ee[m ceph-volume: rearrange api/lvm.py
[33m3e9b316bea[m ceph-volume: mokeypatch calls to lvm related binaries
[33m8da24dfc74[m nautilus: mgr/dashboard: Automatically use correct chromedrive… (#31371)
[33m207806abaa[m os/bluestore/KernelDevice: fix RW_IO_MAX constant
[33m1efb1c68e8[m os/bluestore/KernelDevice: print aio error extent in hex
[33mbd29e44a65[m osd: Make encode/decode of osd_stat_t compatible with version 14
[33mff262be213[m test: Ignore OSD_SLOW_PING_TIME* if injecting socket failures
[33maf2f825ec3[m test: Allow fractional milliseconds to make test possible
[33m9163e1b5fe[m doc: Document network performance monitoring
[33mb8e150ed37[m osd doc mon mgr: To milliseconds for config value, user input and threshold out
[33m882b1e987b[m osd mon mgr: Convert all network ping time output to milliseconds
[33mb2a8c3522d[m common: Add support routines to generate strings for fixed point
[33m56ddfed202[m test: Add basic test for network ping tracking
[33m38773a3409[m osd: Add debug_heartbeat_testing_span to allow quicker testing
[33mdc1e297cfb[m osd: Add debug_disable_randomized_ping config for use in testing
[33me442eb1d15[m osd mgr: Add osd_mon_heartbeat_stat_stale option to time out ping info after 1 hour
[33m79ba2b8ad7[m mon: Indicate when an osd with slow ping time is down
[33ma62ee09fc2[m osd mon: Add last_update to osd_stat_t heartbeat info
[33m9622a75762[m osd: After first interval populate vectors so 5min/15min values aren't 0
[33m8f8dfee0a1[m osd mgr: Store last pingtime for possible graphing
[33mec5e6ed04d[m osd mgr: Add minimum and maximum tracking to network ping time
[33m263ad05938[m doc: Add documentation and release notes
[33m9e48aa1144[m osd mgr mon: Add mon_warn_on_slow_ping_ratio config as 5% of osd_heartbeat_grace
[33m5198809158[m mgr: Add "dump_osd_network" mgr admin request to get a sorted report
[33ma95bb67cd1[m osd: Add "dump_osd_network" osd admin request to get a sorted report
[33mf0a8c65b0a[m osd mon: Track heartbeat ping times and report health warning
[33m021677e9ce[m Merge pull request #30532 from mikechristie/nautilus-rbd-nbd-netlink
[33mb7f7364798[m Merge pull request #30661 from smithfarm/wip-41771-nautilus
[33m833d7680de[m Merge pull request #30464 from smithfarm/wip-41915-nautilus
[33m081b174b48[m Merge pull request #30423 from smithfarm/wip-41545-nautilus
[33me4902c5b42[m Merge pull request #30354 from pdvian/wip-41764-nautilus
[33m9d8d5d968c[m Merge pull request #30120 from pdvian/wip-41620-nautilus
[33mbd8a493e66[m Merge pull request #30697 from uweigand/nautilus-endian-fixes
[33m42c1b4dadf[m Merge pull request #30048 from smithfarm/wip-41258-nautilus
[33m6704981916[m Merge pull request #30007 from smithfarm/wip-41279-nautilus
[33mddb29e2e96[m Merge pull request #30000 from smithfarm/wip-41503-nautilus
[33m6bd176adf0[m Merge pull request #29999 from smithfarm/wip-41501-nautilus
[33m1e9b4a18bc[m Merge pull request #30050 from smithfarm/wip-41443-nautilus
[33m610a4fd3cb[m Merge pull request #30051 from smithfarm/wip-41456-nautilus
[33m937066da44[m Merge pull request #30080 from pdvian/wip-41596-nautilus
[33m9c4a79456b[m mgr/dashboard: fix LazyUUID4 not serializable
[33m56fd8b6744[m mgr/dashboard: accept socket error 0
[33mbc5e7e472c[m mgr/dashboard: Configuring an URL prefix does not work as expected
[33m4fa2265984[m mgr/dashboard: Automatically use correct chromedriver version
[33m098187c86e[m ceph.in: check ceph-conf returncode
[33mdabb9ea145[m mgr/dashboard: accept exceptions from builtin SSL
[33m5ab81fa4a3[m mgr/dashboard: extract monkey patches for cherrypy out
[33me1ab77034e[m osd: set affinity for *all* threads
[33mb58df29c9a[m mgr: restful requests api adds support multiple commands
[33m3cf9dcab1a[m mgr/dashboard: edit/clone/copy rbd image after its data is received
[33ma0e157401d[m Merge pull request #31228 from jan--f/wip-42540-nautilus
[33mf83be16495[m Merge pull request #29994 from smithfarm/wip-41448-nautilus
[33mc28c1a0d40[m Merge pull request #29992 from smithfarm/wip-40084-nautilus
[33m1311dc1edb[m Merge pull request #29991 from smithfarm/wip-39700-nautilus
[33m29ef5d0210[m Merge pull request #29988 from smithfarm/wip-39682-nautilus
[33m06cca80190[m Merge pull request #29979 from smithfarm/wip-41341-nautilus
[33m746c8a849a[m Merge pull request #29997 from smithfarm/wip-41453-nautilus
[33m257e110051[m Merge pull request #29998 from smithfarm/wip-41491-nautilus
[33m8764da9391[m Merge pull request #30805 from jan--f/wip-42236-nautilus
[33m8d721fd00f[m Merge pull request #30807 from jan--f/wip-42234-nautilus
[33m3bd5b49552[m Merge pull request #31290 from idryomov/wip-krbd-unmap-msgr1-nautilus
[33mbee7ad84a8[m qa/tasks/cbt: run stop-all.sh when finishing up
[33m87b8b27127[m mds: split the dir if the op makes it oversized
[33mb1d4498710[m auth/Crypto: assert(len <= 256) before calling getentropy()
[33m958e43a1ab[m auth/Crypto: fallback to /dev/urandom if getentropy() fails
[33mf225bc2a1c[m doc, qa:remove invalid option mon_pg_warn_max_per_osd
[33m7a08825ad4[m rgw: prevent bucket reshard scheduling if bucket is resharding
[33m611df8166c[m ceph-volume-zfs: add the inventory command
[33m315b7ff13c[m qa/suites/krbd: run unmap subsuite with msgr1 only
[33m3bba7c3d41[m rgw: add executor type for basic_waitable_timers
[33m57a191863b[m rgw: beast handle_connection() takes io_context
[33md2a56a4309[m Merge pull request #31210 from sebastian-philipp/nautilus-ceph-volume-device_id
[33m7d8aeba5e3[m Merge pull request #31259 from jan--f/wip-41290-nautilus
[33m1a5ff1300f[m[33m ([m[1;31morigin/wip-nautilus-restful-node-items[m[33m)[m restful: Use node_id for _gather_leaf_ids
[33ma4602189e0[m restful: Query nodes_by_id for items
[33m352ed0ccae[m doc: update bluestore cache settings and clarify data fraction
[33mee46b124ef[m api/lvm: rewrite a condition
[33m2ad3e0f2e3[m qa: add script to stress udev_enumerate_scan_devices()
[33m3a6fa88e61[m krbd: retry on an empty list from udev_enumerate_scan_devices()
[33m8e15daef1e[m krbd: retry on transient errors from udev_enumerate_scan_devices()
[33mc7968c30a4[m qa: add script to test udev event reaping
[33m29ee92a151[m krbd: increase udev netlink socket receive buffer to 2M
[33m924e18f468[m krbd: avoid udev netlink socket overrun
[33m3817245256[m krbd: reap all available events before polling again
[33m7e66b7e33e[m krbd: separate event reaping from event processing
[33m3012b35364[m krbd: get rid of poll() timeout
[33m98cb958fc1[m common/thread: Fix race condition in make_named_thread
[33mbf3f852da4[m ceph-volume: add Ceph's device id to inventory
[33m989db05aab[m mgr/dashboard: add Debug plugin
[33m7b578b6c95[m mgr/dashboard: add new plugin hooks
[33mae69803b2a[m mgr/dashboard: doc plugin infra changes
[33m7979e20b0a[m mgr/dashboard: add SimplePlugin helper
[33mdcb5d6bfe9[m mgr/dashboard: add new interfaces to plugin
[33m7ecadf9be8[m mgr/dashboard: add Mixins and @final to plugins
[33m1298e7b56f[m mgr: add new Command helper
[33m84829db3f5[m rgw: add num_shards to radosgw-admin bucket stats
[33md16976a685[m mgr/dashboard: Update i18n.config for nautilus
[33m37e33b392e[m test/libcephfs: introduce (uint64_t)ceph_pthread_self()
[33m6407b73cbf[m mgr/dashboard: Add transifex-i18ntool
[33m0be0fdfdd1[m test/libcephfs: Only use sys/xattr.h on Linux
[33md51a5bb140[m cephfs: Create a separate dirent{} for FreeBSD
[33mecf7dddfd4[m test/fs: Only use features.h on Linux
[33m049988990c[m rpm: make librados2, libcephfs2 own (create) /etc/ceph
[33ma3a7d9ef8b[m doc/cephfs: improve add/remove MDS section
[33m43e96c346e[m mgr/dashboard: qa: whitelist client eviction warning
[33m67d907efc8[m mon/MonCommands: "smart" only needs read permission
[33m0c6f41a1b8[m mgr/pg_autoscaler: use 'stored' for pool_logical_used
[33m28f8f72311[m auth/cephx/CephxClientHandler: handle decode errors
[33mfe0c425c68[m auth/cephx/CephxProtocol: handle decode errors in CephXTicketHandler::verify_service_ticket_reply
[33m312e44020a[m auth/cephx/CephxServiceHandler: handle decode errors
[33mc913fb2a48[m common/ceph_context: avoid unnecessary wait during service thread shutdown
[33m05479e8a5a[m Merge pull request #31074 from smithfarm/wip-42417-nautilus
[33mb061da67cb[m add bucket permission verify when copy obj
[33mfe8c6f7d44[m qa: whitelist "Error recovering journal" for cephfs-data-scan
[33m6558448e3f[m qa: allow client mount to reset fully
[33m4d4c47830a[m qa: tolerate ECONNRESET errcode during logrotate
[33m885bcf9ac4[m client/MetaRequest: Add age to MetaRequest dump
[33me88db9752d[m osdc/Objecter: Add age to the ops
[33m88761e9a19[m common/ceph_time: Use fixed floating-point notation for mono_clock
[33ma0eee0251a[m mds: Revert "properly setup client_need_snapflush for snap inode"
[33m7ebc5c2da9[m mds: cleanup dirty snap caps tracking
[33m8f645d0205[m osd/PGLog: persist num_objects_missing for replicas when peering is done
[33mf25371e1ae[m mgr/volumes: fix incorrect snapshot path creation
[33m4363856d29[m doc/rbd: s/guess/xml/ for codeblock lexer
[33m7578719991[m rgw: lifecycle days may be 0
[33m732bbe291b[m rgw: sync with elastic search v7
[33m18586232eb[m osd: accident of rollforward may need to mark pglog dirty
[33md2c8f86819[m OSD: rollforward may need to mark pglog dirty
[33md590704778[m qa: fix broken ceph.restart marking of OSDs down
[33meb891806b5[m qa: add debugging failed osd-release setting
[33m1c7b4727f5[m qa: use mimic-O upgrade process
[33m8a619ba771[m mgr: fix weird health-alert daemon key
[33m3036a92bb2[m mon/Monitor.cc: fix condition that checks for unrecognized auth mode
[33m8993bd0e25[m qa/tasks/ceph: retry several times to tell mons ot stop logging health
[33mb560f03dcd[m mon/MonClient: allow retries to be adjusted
[33mad3c03f381[m mon/MonClient: give up targetted mon_commands after one attempt
[33mc0b32516cb[m mon/MonClient: debug show start_mon_command variant
[33me7d8f9e15e[m mon/MonClient: tolerate null onfinish during shutdown
[33mcecb5328bc[m common/safe_io: pass mode to safe_io; use 0600, not 0644
[33m49685cdaef[m kv/RocksDBStore: tell rocksdb to set mode to 0600, not 0644
[33m10c4f3071e[m osd/PrimaryLogPG: skip obcs that don't exist during backfill scan_range
[33mada78d0e37[m Merge pull request #31009 from SUSE/wip-doc-telemetry-default-interval-nautilus
[33m0bd837a47a[m rgw: disable compression/encryption on Appendable objects
[33m9e8c3994a7[m rgw: fix default storage class for get_compression_type
[33md004126bd1[m os/bluestore: fix objectstore_blackhole read-after-write
[33mbd779ac88d[m mgr/balancer: python3 compatibility issue
[33m484b167ed3[m doc/mgr/telemetry: update default interval
[33m8ebaa53382[m rgw: RGWSyncLogTrimCR wont set last_trim=max_marker
[33m685e262f78[m rgw: data/bilogs are trimmed when no peers are reading them
[33m4c46245fc5[m rgw: data/bilog trimming tracks min markers as strings
[33m8527188e83[m rgw: bilog trim uses bucket_info.num_shards to size marker array
[33m2ccdbd3f2c[m rgw: fix end condition in AsyncMetadataList for bilog trim
[33m71dd44575b[m mds: Fix duplicate client entries in list to avoid multiple client evictions
[33m1b15b75b3a[m ceph.spec.in: provide python2-<modname>
[33m8eb0fac3c2[m ceph.spec.in: use python_provide macro
[33m340aa9246b[m mgr/dashboard: home controller unit test
[33m1a4de6df3d[m mgr/dashboard: home: fix fallback to default language
[33m6c222b78bb[m mgr/dashboard: home: fix python2 failure in regex processing
[33macfbedea12[m mds: drive cap recall while dropping cache
[33mf20081b84c[m qa: update json format from session listing
[33m68e2be7ea6[m mds: recall caps from quiescent sessions
[33m937b9b2e91[m mds: use Session::dump method uniformly
[33m6b99509be0[m mds: fix dump routine for session_info_t.used_inos
[33m5049372c87[m mds: fix comment over MDRequestImpl
[33m3698c9427a[m mds: fix some misleading log messages
[33m34160786ed[m client: comment fix in _lookup
[33me291c33334[m mds: use stdbool.h instead of hand-rolling our own bool type
[33m0fd3e5b79f[m mds: use auto to deduce iterator type
[33m857d6d9cb1[m mds: simplify method definition
[33m7a0790f745[m mds: remove useless debug message
[33mbe014c4c25[m mds: use const get_request_count
[33md24342ae70[m mds: use session_info_t socket inst for addr
[33m4ccdb7ddfc[m mds: refactor session lookup
[33m511f44fd53[m mds: add explicit trim flag
[33m593ab54bdd[m common/TrackedOp: make settings atomic
[33m7384f650b3[m mds: alphabetize tracked config keys
[33mc4ff344f99[m mds: apply configuration changes through MDSRank
[33m97cd26a87f[m common: provide method to get half-life
[33md8e5cfa367[m common: correct variable name
[33mf0492ebd67[m test/cls_rbd: removed mirror peer pool test cases
[33meee12ad94a[m qa/workunits/mon: test for crush rule size in pool set size command
[33mab092cffda[m mon/OSDMonitor: add check for crush rule size in pool set size command Fixes: https://tracker.ceph.com/issues/42111
[33m2ba99dc3ba[m mgr/k8sevents: Add support for remote kubernetes
[33mf9c2a69c09[m pybind/mgr: Remove code duplication
[33m9c35026310[m libcephfs: Add test for lazyio via libcephfs
[33mffc7207d50[m libcephfs: Add lazyio_propogate and lazyio_synchronize methods to libcephfs
[33med48f07366[m mgr/dashboard: Add translation file for en-US
[33mda9e8c65ff[m mgr/dashboard: Allow I18N when using "ng serve"
[33m40852085f6[m mgr/dashboard: frontend: default language option in environment
[33m9e8f894772[m mgr/dashboard: Only show available languages
[33mcfd30d8093[m mgr/dashboard: endpoint to list available languages
[33m42f75a8627[m mgr/dashboard: Store selected language in cookies
[33meb2eb58824[m mgr/dashboard: Provide TRANSLATIONS directly in app.module.ts
[33mbef0152ca9[m osd/PG: Add PG to large omap log message
[33m03ac4ee7c3[m mgr/dashboard: Remove missing translation warnings during build
[33m81b29f5463[m mgr/dashboard: Remove call to registerLocalData
[33m024e5ac716[m mgr/dashboard: Remove I18N from main.ts
[33m131372ccc4[m make-dist: build dashboard frontend for each language
[33ma82e50550d[m dashboard: detect language and serve correct frontend app
[33m8f8e76444e[m cmake: dashboard: support locale-dependent frontend builds
[33md415b4c7ba[m mon/OSDMonitor: trim no-longer-exist failure reporters
[33m5bd554bb4a[m osd: Remove unused osdmap flags full, nearfull from output
[33m9dff01ddb6[m osd/OSDMap: do not trust partially simplified pg_upmap_item
[33m27ddb3f11a[m mgr/dashboard: frontend: add npm-run-all dependency
[33mdc6662dafa[m mgr/dashboard: package.json: add build scripts for each language
[33ma544dfe456[m rgw/amqp: fix race condition in AMQP unit test
[33m69ca01df21[m osd: Rename backfill reservation reject names to reflect too full use
[33md34dba7cb6[m osd: Rename MBackfillReserve::TOOFULL to what it does in particular (revoke)
[33m91c2854edc[m osd: Don't set backfill_toofull in RemoteReservationRevoked path
[33m48fffc3c30[m radosgw-admin: 'mdlog trim' loops until done
[33m061ddf8c1c[m radosgw-admin: 'datalog trim' takes shard-id and loops until done
[33m375f1a5b59[m rgw: protect AioResultList by AioThrottle::mutex to avoid race condition
[33m3101b14bb7[m ceph-crash: try to post as either client.crash[.$hostname] or client.admin
[33m9012fa854a[m ceph-crash: use open(..,'rb') to read bytes for Python3
[33m008f0f599e[m mon/MonCap: add 'crash' profile
[33mef96928425[m qa/tasks: tests for ls
[33m9f8883b0f6[m mgr/volumes: list FS subvolumes, subvolume groups and their snapshots
[33mb1a9115fcf[m rbd-mirror: prevent restored trash images from being deleted after delay
[33m1cadc397be[m rbd-mirror: renamed RemoveRequest state machine to TrashRemoveRequest
[33mab9e9f3ada[m rbd-mirror: set image as primary when moving to trash
[33m0d1fcf0975[m librbd: allow mirroring trash images to be restored
[33m92843bfba5[m librbd: reuse async trash remove state machine
[33m7c0f9afb8d[m librbd: async trash remove state machine
[33mc046c8fd57[m qa/workunits/rbd: add 'remove mirroring pool' test
[33meb8adcbe50[m librbd: behave more gracefully when data pool removed
[33macb870be0a[m librados: add IoCtx::is_valid method to test if IoCtx was initialized
[33mfe68a8da9e[m librbd: v1 clones are restricted to the same namespace
[33m3140054ba2[m cls/rbd: sanitize entity instance messenger version type
[33mf6078d02c2[m qa/suites/rbd: test case for rbd-mirror bootstrap
[33ma7c6d3f402[m doc/rbd: document new rbd mirror bootstrap commands
[33m6ac576f4e8[m doc/rbd: clarify cluster naming by using site-a/b
[33m77d9ddbc95[m rbd: new 'mirror pool peer bootstrap create/import' commands
[33m26c0b79e84[m librbd: new RBD mirroring peer bootstrap API methods
[33m2f91ed7e74[m mon/MonCap: new 'rbd-mirror-peer' profile
[33m85b17b994e[m rbd: display and configure local mirroring site name
[33ma953d5a79b[m mgr/alerts: raise health alert if smtplib has a problem
[33m155465512b[m mgr/alerts: simple module to send health alerts
[33me53d46487b[m librbd: initial support for friendly mirror site names
[33m1be3429e8b[m pybind/rbd: deprecate `parent_info`
[33m1a89ccdc72[m ceph-volume: update tests since VolumeGroups.filter returns a list
[33m17ea5b444f[m ceph-volume: VolumeGroups.filter shouldn't purge itself
[33m011b2f55ff[m ceph-volume: allow creating empty VolumeGroup objects
[33md1146c3a83[m ceph-volume: update tests since PVolumes.filter returns a list
[33mcccd89d039[m ceph-volume: PVolumes.filter shouldn't purge itself
[33m26e8c88924[m ceph-volume: allow creating empty PVolumes objects
[33m6bd64f37f6[m Merge pull request #30676 from sobelek/wip-42050-nautilus
[33mb8114803cc[m Merge pull request #29965 from smithfarm/wip-41272-nautilus
[33m562b7e2195[m Merge pull request #29969 from smithfarm/wip-41446-nautilus
[33m36a9b46187[m Merge pull request #29970 from smithfarm/wip-41459-nautilus
[33m2dfb738ed4[m Merge pull request #29972 from smithfarm/wip-41482-nautilus
[33mb811db87c2[m Merge pull request #29974 from smithfarm/wip-41493-nautilus
[33m6af018cef9[m Merge pull request #29963 from smithfarm/wip-41267-nautilus
[33m503a17e766[m Merge pull request #29971 from smithfarm/wip-41479-nautilus
[33m5b91b85fd2[m Merge pull request #30037 from smithfarm/wip-41588-nautilus
[33mc284bb4554[m Merge pull request #30068 from smithfarm/wip-41485-nautilus
[33m930d59beed[m Merge pull request #30160 from pdvian/wip-41624-nautilus
[33m4125ec29ad[m Merge pull request #30748 from smithfarm/wip-42194
[33mf8ed28a812[m os/bluestore: fix improper setting of STATE_KV_SUBMITTED.
[33m73a74e09dc[m osd/PG: scrub error when objects are larger than osd_max_object_size
[33m3e2237a76c[m os/bluestore: refuse to mkfs or mount if osd_max_object_size >= MAX_OBJECT_SIZE
[33m27275cbc01[m mgr/prometheus: Fix KeyError in get_mgr_status
[33mca767c4c31[m Merge pull request #29591 from pdvian/wip-40894-nautilus
[33m8a7de33564[m Merge pull request #29878 from pdvian/wip-41096-nautilus
[33m74e8794d9b[m Merge pull request #29879 from pdvian/wip-41099-nautilus
[33m2ab8fda326[m Merge pull request #29938 from pdvian/wip-41107-nautilus
[33mf40a7c3f9c[m Merge pull request #29983 from pdvian/wip-41128-nautilus
[33m39eb1a0a9e[m Merge pull request #30026 from smithfarm/wip-40895-nautilus
[33mcbd6cc682b[m Merge pull request #30030 from smithfarm/wip-40887-nautilus
[33m19c1b5dbba[m Merge pull request #30031 from smithfarm/wip-40900-nautilus
[33m6db49b4622[m Merge pull request #30032 from smithfarm/wip-41113-nautilus
[33m5fa49988f7[m Merge pull request #30038 from smithfarm/wip-41276-nautilus
[33m68022dba6f[m Merge pull request #30039 from smithfarm/wip-41465-nautilus
[33me92295ea24[m Merge pull request #30040 from smithfarm/wip-41467-nautilus
[33m93efb53b58[m Merge pull request #30041 from smithfarm/wip-41477-nautilus
[33m2e0055abbe[m Merge pull request #30057 from varshar16/wip-nautilus-cephfs-shell-path-conversion
[33m711b67b956[m Merge pull request #30418 from pdvian/wip-41851-nautilus
[33m0f3afc2d4c[m Merge pull request #30442 from pdvian/wip-41855-nautilus
[33m13975a353d[m Merge pull request #30455 from smithfarm/wip-41889-nautilus
[33m8bf6e35b8c[m Merge pull request #30508 from ukernel/nautilus-41948
[33m365a93bd3f[m ceph-backport.sh: add deprecation warning
[33m5845dfca39[m mgr/BaseMgrStandbyModule: drop GIL in ceph_get_module_option()
[33maa71d538ba[m client: fix lazyio_synchronize() to update file size
[33madc506d3fc[m doc: protection for 'fs volume rm' command
[33m7b7c685b32[m doc: add ceph fs volumes and subvolumes documentation
[33m80e220964f[m qa/tasks: add/update tests for --yes-i-really-mean-it
[33m459b776514[m mgr/volumes: protection for 'fs volume rm' command
[33m5fdf9e9dd0[m mds: mds returns -5 error when the deleted file does not exist
[33m8f4f4f9a33[m client: don't ceph_abort on bad llseek whence value
[33mce1e0f9195[m client: remove Inode dir_contacts field
[33m963d5939dd[m doc/ceph-fuse: mention -k option in ceph-fuse man page
[33mea832c4fcb[m client: add procession of SEEK_HOLE and SEEK_DATA in lseek.
[33m0eae89bccb[m client: _readdir_cache_cb() may use the readdir_cache already clear
[33m07801c9a4f[m client:EINVAL may be returned when offset is 0 ,Loff_t pos = f->pos should be the best. Fixes:https://tracker.ceph.com/issues/41837 Signed-off-by: wenpengLi <liwenpeng@inspur.com>
[33m354413d663[m rgw: fix list versions starts with version_id=null
[33me8216d06d3[m rgw: make rollback refcount tag match
[33m2a0bcd39ee[m rgw: make sure object's idtag is updated when available
[33mb62e822775[m ceph-objectstore-tool: update-mon-db: do not fail if incmap is missing
[33m3fd05da32a[m rgw: gc remove tag after all sub io finish
[33m1bc9b1f077[m Merge pull request #30325 from theanalyst/wip-41498-nautilus
[33me9c90c043d[m Merge pull request #30437 from smithfarm/wip-41846-nautilus
[33m93f9b02d3f[m Merge pull request #30604 from vumrao/wip-vumrao-41976
[33m2306d9b0d7[m Merge pull request #30651 from pdvian/wip-41970-nautilus
[33m8366607121[m Merge pull request #30680 from pdvian/wip-41974-nautilus
[33m7cd1d51dd6[m Merge pull request #30247 from pdvian/wip-41631-nautilus
[33m76bc69e37b[m Merge pull request #30248 from pdvian/wip-41627-nautilus
[33m431f087b23[m Merge pull request #30252 from smithfarm/wip-41707-nautilus
[33m7191d43ff8[m Merge pull request #30509 from pdvian/wip-41898-nautilus
[33m99e9f5ac1a[m Merge pull request #30472 from pdvian/wip-41858-nautilus
[33m0bdec13280[m Merge pull request #29716 from xiexingguo/wip-build-push-segv-for-n
[33m7d44d9e2aa[m Merge pull request #29928 from smithfarm/wip-41534-nautilus
[33mc2aaf4e67d[m Merge pull request #29946 from smithfarm/wip-39412-nautilus
[33mea40929353[m Merge pull request #30278 from pdvian/wip-41703-nautilus
[33m3d47dba8ac[m Merge pull request #30605 from vumrao/wip-vumrao-41963
[33m046a88d45d[m Merge pull request #30607 from vumrao/wip-vumrao-41960
[33m3f5b51e719[m Merge pull request #30708 from smithfarm/wip-epel-release-snafu-nautilus
[33mbc81865563[m ceph.spec.in: fix Cython package dependency for Fedora
[33m3b15a62ffb[m ceph.spec.in: s/pkgversion/version_nodots/
[33mabc1185214[m install-deps: do not install if rpm already installed
[33m22b3daac54[m rgw: kill compile warnning in rgw_object_lock.h
[33mde6b287458[m rgw: DefaultRetention requires either Days or Years
[33m183e5054af[m rgw: fix MalformedXML errors in PutBucketObjectLock/PutObjRetention
[33m940648c159[m rgw: fix doc compile warning caused by objectops.rst
[33m4c562ecf21[m rgw: fix compile error in unittest:iam_policy
[33me3736e6aae[m rgw: lifecycle expiration should check object lock before removing objects indeed
[33m35597d5dbf[m rgw: modify iam_policy unit test to support object lock.
[33ma7bfa6f560[m rgw: remove namespace from header file.
[33me214067af1[m rgw: add  missing operation to iam_policy
[33m46a694a4e4[m rgw: fix some bugs in object lock feature
[33m21529fd3bf[m rgw: fix some errors in params
[33mf36ce567c3[m rgw: add object lock doc.
[33m385db6ea26[m rgw: add object lock feature.
[33m750cd9b689[m bloom-filter: Improve test cases
[33mf85f6c1276[m osd: Endian fix for PGTempMap handling
[33m587ab1093e[m librbd: Endian fix for handling old image format resize requests
[33m766e88ff86[m rgw,test: Add missing init_le calls
[33m71f3c90923[m librados,test: Fix incorrect use of __le16/32/64
[33m21bd5f0b51[m transaction: Fix incorrect use of __le16/32/64
[33m01f5c16ebe[m bluestore: Fix incorrect use of __le16/32/64
[33m4c26428f10[m async,crimson: Add missing init_le calls
[33m0fb4d226d9[m msg: Fix incorrect use of __le16/32/64
[33m8b79e422e4[m messages: Fix incorrect use of __le16/32/64
[33m8b16d62bdc[m mds: Fix incorrect use of __le16/32/64
[33m01a14db05c[m cephx: Fix incorrect use of __le16/32/64
[33mefa189c3a3[m checksum: Fix incorrect use of __le16/32/64
[33m3d006ddd31[m include: Fix new-style encoding routines on big-endian
[33m8cefc321cd[m include: Simplify usage of init_le16/32/64 routines
[33mdbf0947e2d[m include: Endian fix for shared kernel/user headers
[33md1f0008e0f[m msg: fix addr2 encoding for sockaddrs
[33mc3259fc246[m rgw: tests: Fix building with -DWITH_BOOST_CONTEXT=OFF
[33m910f0eaa89[m cmake: Test for 16-byte atomic support on IBM Z
[33m73246ad641[m osd: DynamicPerfStats cleanup
[33m43a6585cb8[m mgr/dashboard: Automatically refresh CephFS chart
[33mb0c4c155e0[m mgr/dashboard: Refactors CephFS chart component
[33m60b530a753[m mgr/dashboard: Add missing unit tests for CephFS chart
[33m84282f15e6[m mgr/dashboard: Fix CephFS chart
[33ma9429b988f[m Merge pull request #30579 from yuvalif/wip-yuval-backport-notif-nautilus
[33meb21d2551c[m Merge pull request #30601 from smithfarm/wip-42070-nautilus
[33m07dde23d36[m Merge branch 'nautilus' into wip-yuval-backport-notif-nautilus
[33mf5f6e8c9b9[m Merge pull request #30686 from smithfarm/wip-40131-nautilus-follow-on
[33m199a17d8e0[m install-deps.sh: add EPEL repo for non-x86_64 archs as well
[33m4e4b0cdd8f[m Merge pull request #30322 from theanalyst/wip-41700-nautilus
[33m78c66be017[m Merge pull request #30245 from smithfarm/wip-41711-nautilus
[33m3bf8facf40[m doc: fix urls in posix.rst
[33me56b6dc99b[m Merge pull request #30664 from smithfarm/wip-42105-nautilus
[33m7cac52300e[m Merge pull request #30004 from smithfarm/wip-41568-nautilus
[33madefec88fd[m Merge pull request #30003 from smithfarm/wip-41529-nautilus
[33ma1401847eb[m osd/PrimaryLogPG: Avoid accessing destroyed references in finish_degraded_object
[33md3271c68fe[m rgw: lc: check for valid placement target before processing transitions
[33m0ccac679e2[m rgw: add minssing admin property when sync user info.
[33m83c8f45907[m ceph-volume: avoid backported version of configparser
[33m193fd80d21[m ceph-volume: fix warnings raised by pytest
[33m542ec6d205[m cmake,run-make-check.sh,deb,rpm: disable SPDK by default
[33maae09112a5[m Merge pull request #29731 from pdvian/wip-41081-nautilus
[33mee9e350275[m admin/build-doc: use python3
[33mbcdec9fa8b[m common/config_proxy: hold lock while accessing mutable container
[33mf3d0921081[m Merge PR #30025 into nautilus
[33m8ece29c759[m rgw: ldap auth: S3 auth failure should return InvalidAccessKeyId
[33m95c11e6743[m Merge pull request #30649 from lordcirth/wip-doc-misplaced-ratio
[33mefc8f7d669[m doc: max_misplaced was renamed in Nautilus
[33mc343213ae3[m osd: don't check for primary when updating pg's dynamic perf stats queries
[33m7b802bf4ec[m Merge pull request #30554 from alfredodeza/nautilus-guits-41392
[33m36f266e55a[m osd/osd_types: fix {omap,hitset_bytes}_stats_invalid handling on split/merge
[33m60757c5f09[m mgr: set hostname in DeviceState::set_metadata()
[33m01c7498da6[m pybind/rados: put lens array in outer scope
[33m43cacbc660[m pybind/rados: fix set_omap() crash on py3
[33m5abb83ae5e[m tools/rados: add --pgid in help
[33m9997eabe6d[m tools/rados: call pool_lookup() after rados is connected This commit fixes a segmentation fault when using --pgid option in rados ls command in combination with --pool/-p option. The reason for the crash was that we can not use the rados object before connecting it with the cluster using rados.init_with_context().
[33me2db5a773b[m radosgw-admin: add --uid check in bucket list command Fixes: https://tracker.ceph.com/issues/41589
[33m9b8fd6f9dc[m mgr/dashboard: Controls UI inputs based on "type"
[33mdddec1899d[m mgr/dashboard: Support iSCSI target-level CHAP auth
[33m3dc47b8c46[m mgr/dashboard: Wait for iSCSI target put and delete
[33mdb82baaadb[m mgr/dashboard: wait for iscsi/target create/put
[33m35480e6e03[m mgr/dashboard: Gracefully handle client/target info not found
[33m22713ce0b0[m mgr/dashboard: Fix error editing iSCSI disk controls
[33m534c2f1469[m Merge pull request #30049 from smithfarm/wip-41333-nautilus
[33ma2d4d67f03[m build-doc: allow building docs on fedora 30
[33m91cb3928a9[m rgw/pubsub: backporting pubsub/notifications to nautilus
[33m47c982e8d7[m rgw/pubsub: add notification filtering
[33m9ac7c524c0[m rgw/pubsub: support deletion markers and multipart upload
[33m8a32257104[m rgw/pubsub: push notifications documentation
[33m0d095b64e9[m rgw/pubsub: push notifications from ops
[33m5aee038d5c[m rgw/pubsub: allow pubsub REST API on master
[33m3525f83958[m rgw/pubsub: service reordering issue
[33m8a8c80d4b7[m rgw/pubsub: fix amqp topic bug. add disabled end2end push tests
[33m131bac7ed3[m rgw/pubsub: fix duplicates due to multiple zone synching
[33ma1831cbc35[m rgw/pubsub: run pubsub tests even if multisite fails
[33mdc262262ff[m rgw/pubsub: make new PSZone parameters optional in test
[33m36b872c7db[m rgw/pubsub: add conf parameter for full/incremental sync
[33mba0eac0384[m rgw: add tenant as parameter to User in multisite tests
[33m49592c313c[m rgw/pubsub: revert the RGWSysObjectCtx change
[33m74ae4014b1[m rgw/pubsub: cleanup tests for multiple notifications
[33m8a2a767408[m rgw/pubsub: fix doc on updates. attempt to fix multi-notifications
[33me38926de95[m rgw/pubsub: fix more test issues with teuthology failures
[33m0340d2d8a2[m rgw/pubsub: fix test issue with 3 zones
[33mf823976d2a[m rgw/pubsub: add another zone for pubsub
[33mecb5b90ff8[m rgw/pubsub: make sure bucket is empty before deletion
[33md84754f05b[m rgw/pubsub: fix "no module named rgw_multi.zone_ps"
[33m0b1463404c[m rgw/pubsub: fix "no module named rgw_multi.tests"
[33m21f8138c05[m rgw/pubsub: actually adding the ps tests
[33m8b4feb6076[m rgw/pubsub: fix popping the wrong key
[33m1955e0315c[m rgw/pubsub: fix topic arn. tenant support to multisite tests
[33mff975da867[m rgw/pubsub: handle subscription conf errors better
[33m609283c347[m rgw/pubsub: more info on notification deletion compatibility
[33mce5768269b[m rgw/pubsub: fix comments from PR #27493
[33m40a7f0a783[m rgw/pubsub: clarify pubsub zone configuration
[33m9a05d946aa[m rgw/pubsub: wrong link in S3 doc
[33mb3ce4d852a[m rgw/pubsub: fix documentation link errors
[33m9e0c9a4715[m rgw/pubsub: fix comments from PR #27091
[33mb652d3284e[m rgw/pubsub: add more S3 compatibility documentation
[33md1b1f88d47[m rgw/pubsub: test and doc bucket deletion impact on notifications
[33m31e2fd493b[m rgw/pubsub: avoid static creation of amqp manager
[33me27a0b7a38[m rgw/pubsub: add s3-compatible API documentation
[33mf0f474e177[m rgw/pubsub: allow for endpoint definition via topics
[33me95bbdc6af[m rgw/pubsub: implement S3 compatible get/delete APIs
[33m36fe1eb65d[m rgw: pubsub support s3 records. refactor ARN
[33md75dca1fcb[m rbd-nbd: Fix spacing in UpdateWatchCtx
[33m725083a76e[m rbd-nbd: Add resize support
[33m2b37c56627[m rbd nbd: always try to load module on map
[33m061b05d433[m rbd nbd: add support for netlink interface
[33m207088a0db[m cmake and build: add netlink lib requirement detection
[33ma2dc404c4f[m cmake: Move WITH_RBD option earlier
[33mbb7b11a0e4[m rbd nbd: Add cond to signal nbd disconnection
[33mea8519a80b[m rbd nbd: just move signal handler
[33mb32b66f984[m rbd nbd: move nbd index parsing to function
[33md48df4d783[m rbd nbd: move server startup code to function
[33meb0d0747fb[m rbd nbd: move nbd map ioctls to new function
[33m6ad0b46822[m ceph-volume: do not fail when trying to remove crypt mapper
[33mf3c1a77e26[m rgw/pubsub: initial version of S3 compliant API
[33m2bf60f718c[m nautilus: cmake/BuildDPDK: ignore gcc8/9 warnings
[33mb1d621fb31[m mgr/dashboard: Validate iSCSI controls min/max value
[33mc3864278ed[m osd: add log information to record the cause of do_osd_ops failure
[33m7ebc553115[m qa/standalone/osd/divergent-priors: add reproducer for bug 41816
[33mde490a15ad[m test: Divergent testing of _merge_object_divergent_entries() cases
[33m280f4ff2f8[m test: Make most tests use default objectstore bluestore
[33mdaedb4877f[m test: Output elapse time for each script for information
[33m02c70e8414[m mgr/dashboard: NFS list should display the "Pseudo Path"
[33m817644a07d[m osd/PeeringState: recover_got - add special handler for empty log
[33ma2261944a4[m Merge pull request #30304 from alfredodeza/nautilus-ceph-volume-zap-fix
[33m2e1748eb2e[m Merge pull request #30520 from smithfarm/wip-41956-nautilus
[33m89ffece490[m msg/async/ProtocolV1: require CEPHX_V2 if cephx_service_require_version >= 2
[33m19fa1a63dd[m mon/MonClient: skip CEPHX_V2 challenge if client doesn't support it
[33m14b99bc557[m ceph-volume: systemd fix typo in log message
[33m7bce1ffb49[m Merge PR #29926 into nautilus
[33mb7186e39c7[m rgw: fix data sync start delay if remote haven't init data_log
[33m880cf3569f[m mds: wake up lock waiters after forcibly changing lock state
[33m700c59925f[m mgr/volumes: return string type to ceph-manager
[33m82f4633f72[m qa/tasks: test for prevent negative subvolume size
[33mcb05943888[m mgr/volumes: prevent negative subvolume size
[33m0f69ce217e[m mgr/volumes: drop unnecessary size
[33md894ac96ce[m mgr/volumes: cleanup FS subvolume or subvolume group path
[33m4dd80305fb[m mgr/volumes: give useful error message
[33mae84c278c8[m mon: show pool id in pool ls command
[33mf3ef3ded38[m mon: ensure prepare_failure() marks no_reply on op
[33mc696d872aa[m rgw: fix memory growth while deleteing objects with radosgw-admin bucket rm --bucket=$BIG_BUCKET --bypass-gc --purge-objects by freeing <rgw_obj, RGWObjState> map elements allocated at https://github.com/ceph/ceph/blob/master/src/rgw/rgw_rados.cc#L236   result = &objs_state[obj];
[33m784202f5dc[m qa: avoid page cache for krbd discard round off tests
[33mc113783b57[m Merge pull request #29487 from rhcs-dashboard/nautilus-run-dashboard-api-tests
[33m39d8e981bd[m mgr/volumes: cluster log when a purge thread bails out
[33m835fd10868[m mgr/volumes: retry purging trash entries on intermittent error
[33mf21247c9dd[m rgw: fix indentation for listobjectsv2
[33m5bc6a2ef21[m client: nfs-ganesha with cephfs client, removing dir reports not empty
[33mf3b72b6fdb[m rgw: increase beast parse buffer size to 64k
[33mf26722530e[m mgr/dashboard: change bucket owner between owners from same tenant
[33m462e659cea[m Merge tag 'v14.2.4' into nautilus
[33ma84f5c1a28[m mgr/dashboard: run-backend-api-tests.sh CI improvements
[33m72b918f6c1[m pybind/mgr: install setuptools >= 12
[33md1f064b278[m mgr/dashboard: set python binary for teuthology venv
[33m83fe3eafb8[m test/librbd: set nbd timeout due to newer kernels defaulting it on
[33mafe2a755a5[m mds: make MDSIOContextBase delete itself when shutting down
[33mcf78989688[m 14.2.4
[33m6bae9a4d38[m[33m ([m[1;31morigin/wip-41238-nautilus[m[33m)[m mon/OSDMonitor: Add standalone test for mon_memory_target
[33m7bf2cb9b5c[m mon/OSDMonitor: Implement config observer to handle changes to cache sizes
[33m10b83ebc13[m mon/OSDMonitor: Use generic priority cache tuner for mon caches
[33m1c5b2396a9[m cmake: link libkv against libheap_profiler
[33mbd6c7a5505[m cmake: link libkv against common_prioritycache_obj
[33ma347fbc7f2[m cmake: remove kv_objs target
[33mbe5436c8af[m common: make pri cache perf counters optional
[33m0db8662f60[m common: make extra memory allocation for pri cache optional
[33m432ac3e972[m common/PriorityCache: fix over-aggressive assert when mem limited
[33mec33b46180[m common/PriorityCache: Implement a Cache Manager
[33m84756e466b[m mgr/dashboard: Fix calculation of PG Status percentage
[33mc1e2fbf8b2[m mgr: do not reset reported if a new metric is not collected
[33m8bb56184b5[m SSL-enabled dashboard does not play nicely with a frontend HAproxy. To fix that issue there are two new configuration options:
[33m75f4de193b[m[33m ([m[1;33mtag: v14.2.4[m[33m)[m 14.2.4
[33m13ee376d48[m ceph-volume tests create a test file for checking unicode output
[33m14c347cfe3[m ceph-volume tests create a shell test for functional unicode
[33m60de430c9e[m ceph-volume tests verify new logging fallback and encodings in terminal
[33m566d74dacc[m ceph-volume create a logger for the terminal
[33m2e27e32a3c[m ceph-volume: instantiate the new terminal logger in main()
[33m6f186a2bad[m ceph-volume terminal remove unicode stream handler
[33mf507d1d627[m mgr/dashboard: frontend: move ellipsis to before progress in task execution description
[33mf59637571e[m mgr/dashboard: test_pool: fix pool unit tests
[33m571017d6d9[m mgr/dashboard: pool: make _get and _wait_for_pgs class methods
[33m2d4ea95729[m mgr/dashboard: Watch for pool pg's increase and decrease
[33mcafaa7221f[m mgr/dashboard: Allow the decrease of pg's of an existing pool
[33m63443793ba[m rgw: fix dns name comparison for virtual hosting
[33m42db65d34d[m osd: prime splits/merges for any potential fabricated split/merge participant
[33mf5613fea92[m ceph.spec.in: reserve 2500MB per build job
[33meb0f88e93d[m rgw: fix minimum of unordered bucket listing
[33ma6e846c84b[m Merge pull request #29769 from alfredodeza/nautilus-bz-1738379
[33m0353ddfbf5[m cephfs-shell: Convert paths type from string to bytes
[33mac4d3ab99c[m Merge pull request #30294 from alfredodeza/nautilus-rm41378-2
[33mc94580ad3b[m Merge pull request #30307 from alfredodeza/nautilus-rm40664
[33m48c084a9a5[m test/cls_rbd/test_cls_rbd: update TestClsRbd.sparsify
[33m9681ad4c95[m Merge pull request #30300 from alfredodeza/nautilus-rm41660
[33m74d05baf0d[m rgw: fix checking index_doc_suffix when getting effective key
[33m5112357f96[m rgw: fix the bug of rgw not doing necessary checking to website configuration
[33m6b9fbbb2c5[m install-deps.sh: install `python*-devel` for python*rpm-macros
[33maedfe410b2[m ceph-volume tests: verify that wipefs tries several times
[33m7d5807d7cf[m ceph-volume lvm.zap: retry wipefs several times to prevent race condition failures
[33mf1cc8438fb[m ceph-volume lvm.zap fix cleanup for db partitions
[33m4183d29c22[m ceph-volume tests create a test file for checking unicode output
[33m1c2894dc7c[m ceph-volume tests create a shell test for functional unicode
[33mcd82ab7019[m ceph-volume tests verify new logging fallback and encodings in terminal
[33md1615c0da2[m ceph-volume terminal remove unicode stream handler
[33m26b97ecbc4[m ceph-volume: instantiate the new terminal logger in main()
[33mbdf6a8b57e[m ceph-volume create a logger for the terminal
[33m7c636503bb[m ceph-volume create a new tox.ini for shell-based tests
[33m716ed2dcc5[m qa/standalone/ceph-helpers: resurrect all OSD before waiting for health
[33m2c6d654ecc[m ceph-volume tests pre-instrall python-apt to prevent auto-install failing later
[33m1c7761bbdb[m Merge pull request #30283 from tchaikov/wip-nautilus-c++17
[33me9bf719530[m cmake: enforce C++17 instead of relying on cmake-compile-features
[33mcdefc53d82[m osd/PeeringState: fix wrong history of merge target
[33m432d1369c7[m osd/PrimaryLogPG: update oi.size on write op implicitly truncating object up
[33m6db5d8a201[m build/ops: fix build fail related to PYTHON_EXECUTABLE variable
[33mada0bcb0aa[m mgr/dashboard: access_control: add grafana scope read access to *-manager roles
[33m7e37204d7f[m rgw: fix cls_bucket_list_unordered() partial results returnied after -ENOENT dirent is encountered
[33m43cb012b4e[m doc/man/ceph-kvstore-tool: fix typo
[33m885a67eeb9[m doc/ceph-kvsore-tool: add description for 'stats' command
[33m97e1b1c578[m mgr/k8sevents: Initial ceph -> k8s events integration
[33mf851659984[m librbd: always try to acquire exclusive lock when removing image
[33mc9dda31274[m rgw: fixed "unrecognized arg" error when using "radosgw-admin zone rm".
[33m63c9576f33[m rgw: RGWCoroutine::call(nullptr) sets retcode=0
[33ma2c46bd9bc[m Merge pull request #30114 from tchaikov/wip-nautilus-CXX_FLAGS
[33m35c188b05a[m rgw/rgw_op: Remove get_val from hotpath via legacy options
[33m4bcce55d15[m os/bluestore: do garbage collection if blob count is too high.
[33m821869646e[m common/perf_conters: make dump_formatted_xxx funcs as const.
[33m3d11011305[m os/bluestore: store extents for GC within WriteContext.
[33ma8f550aeda[m os/bluestore: GC class, make some members local.
[33m699d725c4c[m os/bluestore: vector -> interval set in GC to track extents to collect.
[33m1977f08d74[m tests/store_test: many-many spanning blobs test case
[33m44ef6cbd44[m Merge branch 'nautilus' of github.com:ceph/ceph into nautilus
[33mc464b67518[m tools/rbd-ggate: close log before running postfork
[33m6fa46c0ec3[m rbd-mirror: ignore errors related to parsing the config file
[33m44529730d5[m global: update HOME environment variable when dropping privileges
[33ma3687f09d5[m dmclock: pick up change to use specified C++ settings if any
[33m0fe1ca97c5[m seastar: pick up change in fmt submodule
[33m4cfff799d8[m Merge pull request #30093 from jan--f/wip-41614-nautilus
[33m0f776cf838[m[33m ([m[1;33mtag: v14.2.3[m[33m)[m 14.2.3
[33m04b15cef88[m rgw: fix list bucket with delimiter wrongly skip some special keys
[33m9f197206f2[m ceph-volume: test number of times LVs list was created
[33m75b3c035fc[m ceph-volume: reuse list of LVs
[33m2d0463ff5a[m install-deps.sh: use gcc-8 on xenial and trusty
[33mc82fbc24c4[m rpm: s/devtoolset-7/devtoolset-8/
[33m92a0ea56e9[m install-dep,rpm: use devtools-8 on amd64
[33m29212ec98f[m test: ceph-objectstore-tool add remove --force with bad snapset test
[33mc70da8780d[m ceph-objectstore-tool: Ignore snapset error if just removing head with --force
[33m05b95d7832[m osd: merge replica log on primary need according to replica log's crt
[33me0ce90a092[m osd: clear PG_STATE_CLEAN when repair object
[33m69a97c4f3d[m rpm: always build ceph-test package
[33m9ebd43960d[m os/bluestore: Don't forget sub kv_submitted_waiters.
[33m864e50961b[m build/ops: make "patch" build dependency explicit
[33m3ec7fdd1d8[m client: return -eio when sync file which unsafe reqs has been dropped         Fixes:http://tracker.ceph.com/issues/40877
[33m76f8e28f36[m mds: fix InoTable::force_consume_to()
[33m128010d791[m mds: trim cache on regular schedule
[33m9dc80f59af[m mds: move some MDCache member init to header
[33m2e1881e9cb[m mount.ceph: properly handle -o strictatime
[33m467307916b[m qa: fix malformed suite config
[33m7a2b4ae18d[m rgw: fix a bug thart lifecycle expiraton generates delete marker continuously.
[33mafa4057492[m client: more precise CEPH_CLIENT_CAPS_PENDING_CAPSNAP
[33m01b2fc4c6e[m client: unify kicking cap flushes and kicking snapcap flushes
[33mf7b8e099f2[m client: define helper function that sends flushsnap message
[33m2af3674c59[m client: cleanup tracking of early kicked flushing caps
[33m390c6e923e[m qa/tests: test if unresponsive MDS client with no caps is evicted directly
[33m20f84f2527[m qa/tests: add a method to signal a MDS client
[33m04dc8c190a[m qa/cephfs: memoize FUSE client pid
[33m6d64424055[m test_volume_client: add positive test for ceph_volume_client method
[33med7eada14a[m test_volume_client: rename test_put_object_versioned()
[33mae19feb30f[m test_volume_client: rewrite test_put_object_versioned
[33mdfb17fbf11[m test_volume_client: use sudo_write_file() form teuthology
[33mdd7ffa1112[m test_volume_client: make test_object_versioned py3 compatible
[33m055e46905f[m test_volume_client: declare only one default for python version
[33m44970166ff[m test_volume_client: don't shadow class variable
[33mc662ca9f7d[m ceph_volume_client: don't convert None to str object
[33mb203d6d0ce[m ceph_volume_client: convert string to bytes object
[33md43d5880c2[m ceph_volume_client: make UTF-8 encoding explicit
[33m31befd4a0e[m pybind: Fixes print of path as byte object in error message
[33m7791f5d211[m pybind: Print standard error messages
[33m24f8b21240[m doc: cephfs: add section on fsync error reporting to posix.rst
[33m10ec940c24[m mgr/zabbix: encode string for Python 3 compatibility
[33me1f6710905[m Merge pull request #29945 from trociny/wip-41475-nautilus
[33ma777ae5494[m Merge pull request #29975 from theanalyst/nautilus-beast-endpoint-fix
[33maf6c343efe[m Added validation of zabbix_host to support hostnames, IPv4 and IPv6. Changed exception logging. Doc typo fixed.
[33m943108a51c[m mgr/zabbix: Documentation added.
[33md2f25f937a[m mgr/zabbix: Adds possibility to send data to multiple zabbix servers.
[33md7ae5f242f[m mgr/pg_autoscaler: fix race with pool deletion
[33mc17b63375b[m mgr/prometheus: Cast collect_timeout (scrape_interval) to float
[33m66d1cdc4ad[m mgr/zabbix: Fix typo in key name for PGs in backfill_wait state
[33m6e64716f27[m doc: Address further comments on choosing pg_num
[33m738c1fe326[m doc: adjust examples to use 2^n pg_num
[33m5fd1702363[m doc: pg_num should always be a power of two
[33ma1d0846751[m doc: default values for mon_health_to_clog_* were flipped
[33m67e20d86e8[m osd/PeeringState: do not complain about past_intervals constrained by oldest epoch
[33m5dca6979d5[m mon: Improve health status for backfill_toofull and recovery_toofull
[33mfea6390358[m osd/OSDCap: Check for empty namespace
[33m09a9f1d27e[m mon: add a process that handle Callback Arguments in C_AckMarkedDown,we add a process that handle _finish(int) Callback Arguments
[33m677397b80c[m qa/tasks/ceph.conf: do not warn on TOO_FEW_OSDS
[33m4363f78485[m mon/PGMap: enable/disable TOO_FEW_OSDS warning with an option
[33m99420c1a22[m qa/standalone: remove osd_pool_default_size in test_wait_for_health_ok
[33mec3e6b2900[m osd: Better error message when OSD count is less than osd_pool_default_size
[33m8f0c3a347c[m test: add tests for per-pool scrub status
[33m0802abe2c1[m mon: show no[deep-]scrub flags per pool in the status
[33m63b4d84371[m filestore: assure sufficient leaves in pre-split
[33m0549a63d87[m qa: wait for kernel client death
[33mb99aec42c7[m qa: use hard_reset to reboot kclient
[33m0ef23aa662[m qa/workunits/rados/test_envlibrados_for_rocksdb: install newer cmake
[33m30edd8b12a[m pybind/mgr/rbd_support: fix missing variable in error path
[33m9d911b0abd[m Merge pull request #29977 from yuriw/wip-yuriw-41513-nautilus
[33m4205964a15[m qa/tests: adding mgr.x into the restart/upgrade sequence before monitors
[33m29753dd3ca[m rgw: asio: check the remote endpoint before processing requests
[33m1518909853[m radosgw-admin: bucket sync status not 'caught up' during full sync
[33m63ecdfa5dd[m rgw: fix potential realm watch lost
[33mddf65d374c[m rgw: make dns hostnames matching case insensitive
[33m728352c558[m rgw: url decode PutUserPolicy params
[33m57a9ac8c6d[m rgw_file: readdir: do not construct markers w/leading '/'
[33md6d8298e54[m rgw:Fix rgw decompression log-print
[33m24cb211563[m rgw: don't throw when accept errors are happening on frontend
[33m9cc43d75d1[m Fix bucket versioning vs. swift metadata bug.
[33m1cb71585aa[m rgw: swift: fix: https://tracker.ceph.com/issues/37765
[33m9b5c5e9eb5[m rgw: permit rgw-admin to populate user info by access-key
[33m753f2b2fce[m rgw: fix drain handles error when deleting bucket with bypass-gc option Fixes: https://tracker.ceph.com/issues/40587
[33m62139b60fa[m rgw_file: introduce fast S3 Unix stats (immutable)
[33mba7bef0dd8[m doc/rados/operations/health-checks: document BlueStore fragmentation and BlueFS space available features
[33m3da06609dd[m test/objectstore: Allocator_test. Add test for dumping free regions and fragmentation_score.
[33m3603e443e3[m BlueStore/allocator: Add command to inspect how much BlueStore's block can go to BlueFS.
[33m6112dd0909[m tools/ceph-bluestore-tool: add commands free-dump and free-score
[33m430c4ccb6e[m common/admin_socket: Add 'execute_command' that allows for self-reflection.
[33m5fc227dd07[m common/admin_socket: Adapted old protocol to use new protocol, simplifies handle path.
[33m594f4d6e62[m BlueStore/allocator: Add ability to dump free allocations via admin socket interface.
[33maf3237708d[m BlueStore/allocator: Give allocator names, so they can be distinguished.
[33m295d7258b3[m BlueStore/allocator: Improved (but slower) method of calculating fragmentation.
[33m1c8ee19a44[m osd/osd_types: pool_stat_t::dump - fix 'num_store_stats' field
[33m9116d9af67[m pybind/mgr/rbd_support: ignore missing OSD support for RBD namespaces
[33mf41652a818[m pybind/rbd: new OperationNotSupported exception
[33m639756516f[m mds: delay exporting directory whose pin value exceeds max rank id
[33m8024fa1c6b[m Merge pull request #29918 from badone/wip-nautilus-tracker-41518-grafana-server
[33m59ce4db7bd[m Merge pull request #29899 from alfredodeza/nautilus-rm41378
[33mdc887dcb3f[m qa/valgrind.supp: fix the name for aes-128-gcm whiterules.
[33m299e33b264[m qa/valgrind.supp: generalize the whiterule for aes-128-gcm.
[33mdbbe68b18e[m nautilus: qa/ceph-ansible: Disable dashboard
[33mdec3489ab3[m ceph-volume tests set the noninteractive flag for Debian, to avoid prompts in apt
[33m7660d4292e[m rgw: Move upload_info declaration out of conditional
[33m91e0ea71c0[m cephfs: fix a memory leak use a smart pointer instead of using 'new' to resolve a memory leak .
[33mc4ba976525[m cephfs: avoid map been inserted by mistake
[33m29aeaf50e4[m qa/workunits/rbd: stress test `rbd mirror pool status --verbose`
[33m29eb502f3a[m rbd-mirror: don't overwrite status error returned by replay
[33m47c59ace6a[m rgw: continuationToken or startAfter shouldn't be returned if not specified.
[33mfdf6991553[m rgw:listobjectsv2
[33mfe45e31e0e[m qa: wait for MDS to come back after removing it
[33m49a6c9aaf2[m Merge pull request #29805 from yuriw/wip-yuriw-41384-nautilus
[33m13c5661544[m qa: ignore expected MDS_CLIENT_LATE_RELEASE warning
[33m18af54ab80[m qa/tests: changed running rbd tests test_librbd_python.sh from tag: v14.2.2
[33me157074510[m Merge pull request #29801 from smithfarm/wip-41263-nautilus
[33m8d4a8e860e[m rgw_file: dont deadlock in advance_mtime()
[33m3a6de8909a[m Merge pull request #28862 from liewegas/wip-bluefs-extents-nautilus
[33md618281836[m Merge pull request #29191 from mynaramana/patch-1
[33ma20ba26721[m doc/rados: Correcting some typos in the clay code documentation Signed-off-by: Myna <mynaramana@gmail.com>
[33m3b2c4f8e37[m rpm: fdupes in SUSE builds to conform with packaging guidelines
[33mf47609cbd4[m rpm: put librgw lttng SOs in the librgw-devel package
[33me226904c91[m Merge pull request #29551 from pdvian/wip-40882-nautilus
[33m1ad4ad9614[m Merge pull request #29722 from dillaman/wip-39499-nautilus
[33m67a5d1fadd[m Merge pull request #29723 from dillaman/wip-40511-nautilus
[33m8e3bf64082[m Merge pull request #29725 from dillaman/wip-41078-nautilus
[33mb7ab3dfaf5[m rgw: clean-up error handling with rgw-admin --reset-stats
[33med6ef38291[m rgw: clean-up error handling with rgw-admin --reset-stats
[33m56ec5fb8f9[m ceph-volume devices.lvm zap use the identifier to report success
[33m8041a508e0[m ceph-volume tests check success message when zapping
[33mc95758b97e[m rgw: mitigate bucket list with max-entries excessively high
[33m4b7bd008dc[m Merge pull request #29745 from liewegas/wip-bluestore-no-cgroup-nautilus
[33m096033b9d9[m os/bluestore/bluefs_types: consolidate contiguous extents
[33md2153f99c9[m qa: sleep briefly after resetting kclient
[33mc8ee3ca953[m osd: support osd_repair_during_recovery
[33m6fbf23ecf1[m os/bluestore: do not set osd_memory_target default from cgroup limit
[33mb28c939e4a[m Merge pull request #29650 from pdvian/wip-40945-nautilus
[33m27ff851953[m Merge pull request #29678 from pdvian/wip-40948-nautilus
[33m5286e37857[m librbd: tweaks to improve throughput for journaled IO
[33m5b96bc0a49[m librbd: new rbd_journal_object_writethrough_until_flush option
[33m68ac2cc019[m journal: support dynamically updating recorder flush options
[33m62436afa3f[m journal: fix broken append batching implementation
[33m8302f3271c[m journal: improve logging on recorder append path
[33m4759953e7d[m journal: wait for in flight advance sets on stopping recorder
[33m152dac5ac6[m journal: optimize object overflow detection
[33mcff1897e10[m Merge pull request #27684 from liewegas/wip-rgw-pgs-nautilus
[33me4b759c9b8[m Merge pull request #29478 from xiaoxichen/wip-41002
[33mce1ff927e8[m Merge pull request #29724 from dillaman/wip-40888-nautilus
[33m6be1ab1128[m rgw: pass mostly_omap flag when opening/creating pools
[33m50c07745e8[m rgw/rgw_rados: pass mostly_omap flag when opening/creating pools
[33m49bbc189fc[m rgw: move rgw_init_ioctx() to rgw_tools.cc
[33m57b072ccae[m rgw: Get rid of num_rados_handles infrastructure in RGWSI_RADOS
[33m398c1271eb[m rgw: Get rid of num_rados_handles infrastructure in RGWRados
[33m4619a82eb9[m rgw: Remove rgw_num_rados_handles option
[33me2c5c8da87[m doc: Fix rbd namespace documentation
[33me84c4474d2[m mon/mgr: add 'rbd_support' to list of always-on mgr modules
[33m3ce69405cc[m pybind/mgr/rbd_support: use image ids to detect duplicate tasks
[33m9ccc2e91a1[m pybind/rbd: fix call to unregister_osd_perf_queries
[33m8e8d63575f[m pybind/mgr: don't log exception when cannot find RBD task by id
[33mdb0f409459[m pybind/mgr: handle duplicate rbd task commands
[33m1fd7794dfe[m qa: test case for new rbd background tasks
[33m5c36533728[m pybind/mgr: rbd tasks now provide a dict to the progress events
[33m3c671a4ed1[m pybind/mgr: mark progress events as failed if an error occurs
[33m1399b01447[m pybind/mgr: support marking progress events as failed
[33m4a4e5be872[m pybind/mgr: new 'rbd task' background task management
[33m285676df7a[m pybind/rbd: new OperationCanceled exception
[33m61a065d140[m pybind/rbd: flatten, remove, trash_remove, migration progress callback
[33m05eb65adc9[m librbd: abort an image removal if block objects cannot be removed
[33m5dfa2a2e77[m librbd: allow ProgressCtx::update_progress to cancel maintenance ops
[33m1656b3ae2f[m doc/rbd: initial live-migration documentation
[33m12248c7a69[m librbd: the first post-migration snapshot isn't always dirty
[33m38c0216d83[m librbd: don't update snapshot object maps if copyup data is all zeros
[33m144e2f9c8b[m librbd: avoid repeatedly invoking is_zero() in CopyupRequest
[33m0c5ddc5fe3[m Merge pull request #29436 from wjwithagen/wip-41038-nautilus
[33ma61ca37cb6[m Merge pull request #29454 from pdvian/wip-40734-nautilus
[33md47090ef32[m Merge pull request #29352 from Devp00l/wip-40658-nautilus
[33m2f03cb2eca[m Merge pull request #29354 from Devp00l/wip-40982-nautilus
[33m375a2cbeae[m Merge pull request #29524 from pdvian/wip-40846-nautilus
[33m83b25331af[m Merge pull request #29566 from pdvian/wip-40885-nautilus
[33m831ab10fe7[m Merge pull request #29671 from neha-ojha/wip-40322-nautilus
[33mdcf2f578c4[m Merge pull request #29682 from b-ranto/wip-push-dash
[33mb45bcc62d9[m Merge pull request #29308 from smithfarm/wip-40518-nautilus
[33mfb230f01d9[m Merge pull request #29326 from smithfarm/wip-40540-nautilus
[33mc40045cb7e[m Merge pull request #29328 from smithfarm/wip-40237-nautilus
[33mf03286ff39[m Merge pull request #29329 from smithfarm/wip-40501-nautilus
[33m4003b6e658[m Merge pull request #29409 from pdvian/wip-40600-nautilus
[33m019861751c[m Merge pull request #29410 from pdvian/wip-40627-nautilus
[33m0ab208a500[m Merge pull request #29499 from pdvian/wip-40848-nautilus
[33m4b072a3777[m msg/simple: reset in_seq_acked to zero when session is reset
[33m86a445627c[m osd/ReplicatedBackend: check against empty data_included before enabling crc
[33md1170fc855[m Merge pull request #29697 from pdvian/wip-41084-nautilus
[33md83f5978da[m Merge pull request #29660 from jan--f/wip-41248-nautilus
[33mdcf69208e9[m ceph-volume: don't try to test lvm zap on simple tests
[33mb07494e770[m Merge pull request #29702 from jan--f/wip-41308-nautilus
[33mcbe62a457b[m Merge pull request #29689 from jan--f/wip-41299-nautilus
[33mde61a2ed39[m mgr/dashboard: Unify the labels on all table actions objects
[33m8cb3f07a44[m mgr/dashboard: Unify action naming for NFS
[33m0484d857ec[m Merge pull request #29455 from pdvian/wip-40672-nautilus
[33ma9522f2318[m ceph-volume: fix batch functional tests, idempotent test must check stderr
[33ma86e07fabd[m ceph-volume: don't keep device lists as sets
[33mb06ac13e04[m Merge pull request #29690 from jan--f/wip-41082-nautilus
[33m4fb5afd472[m common/options.cc: common/options.cc: change default value of bluestore_fsck_on_umount_deep to false
[33me1c14da2bb[m common/options.cc: change default value of bluestore_fsck_on_mount_deep to false
[33m086145121c[m Merge pull request #29645 from yuriw/wip-yuriw-upgrade-rbd-nautilus
[33me036ab9c34[m Merge pull request #29065 from s0nea/wip-40786-nautilus
[33m99be48e050[m Merge pull request #29372 from pdvian/wip-40537-nautilus
[33m3e318be356[m Merge pull request #29373 from pdvian/wip-40542-nautilus
[33mbc18d44317[m Merge pull request #29562 from liewegas/wip-41037-nautilus
[33m9e5a071c28[m Merge pull request #29694 from jan--f/c-v-no-dashboard-test-nautilus
[33m0ef425451c[m Merge pull request #29484 from pdvian/wip-40737-nautilus
[33m27f49c35b9[m Merge pull request #29444 from ricardoasmarques/fix-ceph-iscsi-required-version-nautilus
[33ma95f731a5f[m ceph-volume: when testing disable the dashboard
[33m50044330dc[m Merge pull request #29306 from smithfarm/wip-40498-nautilus
[33mca05747acc[m ceph-volume: batch ensure device lists are disjoint
[33m3fa99198a3[m rpm: Require ceph-grafana-dashboards
[33m73c64fb1ce[m common/options.cc, doc: osd_snap_trim_sleep overrides other variants
[33m2bf5b0caef[m doc/rados/configuration/osd-config-ref.rst: document snap trim sleep
[33m987beb9698[m osd: add hdd, ssd and hybrid variants for osd_snap_trim_sleep
[33m24698dd732[m mon/OSDMonitor.cc: use CEPH_RELEASE_LUMINOUS instead of ceph_release_t::nautilus
[33mba55405704[m Merge pull request #29442 from pdvian/wip-40659-nautilus
[33mf2caca5dca[m mon/OSDMonitor: require nautilus to set pg_autoscale_mode
[33m01f629258f[m mon/OSDMonitor: allow pg_num to increase when require_osd_release < N
[33m10354ce3b1[m Merge pull request #29617 from pdvian/wip-40942-nautilus
[33m057d4496b5[m mgr/dashboard: RGW User quota validation is not working correctly
[33md31c4934e8[m qa/suites/rados/mgr/tasks/module_selftest: whitelist mgr client getting backlisted
[33m1f8f16e716[m qa/tests: added rbd_fsx tests to the tests mix
[33m87618ccd20[m mgr/dashboard: Pool list shows current r/w byte usage in graph
[33ma274ec1489[m Merge pull request #29600 from jan--f/wip-41203-nautilus
[33mb0f40204c6[m mon/OSDMonitor.cc: better error message about min_size
[33m1fd0f608cf[m Merge pull request #29440 from neha-ojha/wip-40940-nautilus
[33m5f40d87cdd[m Merge pull request #29439 from neha-ojha/wip-40969-nautilus
[33maf48315668[m Merge pull request #28833 from s0nea/wip-40616-nautilus
[33m30996324aa[m Merge pull request #28889 from bk201/wip-40661-nautilus
[33m3e5953043c[m Merge pull request #28912 from bk201/wip-40685-nautilus
[33mfb33f1f81f[m Merge pull request #28938 from Exotelis/i18n-nautilus
[33m305aa9cec1[m Merge pull request #28968 from Devp00l/wip-40699-nautilus
[33m17254258c3[m Merge pull request #28974 from ricardoasmarques/wip-40723-nautilus
[33mdc767fa346[m Merge pull request #28992 from smithfarm/wip-40691-nautilus
[33m6c49fa6cf3[m Merge pull request #29044 from s0nea/wip-40656-nautilus
[33mccf3c78df2[m Merge pull request #29045 from s0nea/wip-40657-nautilus
[33m57d659ef75[m Merge pull request #29061 from s0nea/wip-40279-nautilus
[33m033d7ddf66[m ceph-volume: never log to stdout, use stderr instead
[33m8b09f2bcdb[m ceph-volume: terminal: encode unicode when writing to stdout
[33m7674af824c[m mds: cleanup truncating inodes when standby replay mds trim log segments
[33m4d9848f739[m Merge pull request #29491 from tspmelo/wip-41075-nautilus
[33m413f5c1012[m mon/MgrMonitor: fix null deref when invalid formatter is specified
[33m39d5fccd54[m common/config: respect POD_MEMORY_REQUEST *and* POD_MEMORY_LIMIT env vars
[33me69f38475b[m common: add comment about pod memory requests/limits
[33mfed261446c[m common/config: let diff show non-build defaults
[33mdba8ff221e[m common/config: do no include multiple 'default' values
[33ma0034bafad[m cls/journal: reduce verbosity of debug logs for non-errors
[33m4e387c20af[m cls/rbd: reduce verbosity of debug logs for non-errors
[33mfa69483f59[m mgr/mgr_module: Allow resetting module options
[33m127e6386f3[m doc/orchestrator: Disable the orchestrator
[33mf05a301b92[m Merge PR #29490 into nautilus
[33m836b1d4e1e[m Merge PR #29513 into nautilus
[33m0bdfdc6b54[m Merge pull request #29506 from jan--f/wip-41137-nautilus
[33m8bd7359829[m mgr/dashboard: controllers/grafana is not Python3 compatible
[33m23a116f5d6[m qa/tasks/ceph_deploy: assume systemd and simplify shutdown wonkiness
[33m67e8a68fb1[m qa/tasks/ceph_deploy: do not rely on ceph-create-keys
[33m9199743046[m ceph-volume: print most logging messages to stderr
[33m6f82bc122c[m Merge pull request #29416 from jan--f/wip-41021-nautilus
[33m8605595e00[m rgw: Don't crash on copy when metadata directive not supplied
[33mdeee443013[m Merge pull request #28583 from ukernel/nautilus-40326
[33mf3643e3c19[m Merge pull request #28609 from batrick/i40324
[33mb89072d0ce[m Merge pull request #29156 from smithfarm/wip-40839-nautilus
[33mc8dfee38d7[m Merge pull request #29157 from smithfarm/wip-40842-nautilus
[33m9f3309921a[m Merge pull request #29158 from smithfarm/wip-40843-nautilus
[33m4b765355a9[m Merge pull request #29186 from xiaoxichen/wip-40874-nautilus
[33m91085885e1[m Merge pull request #29209 from smithfarm/wip-40316-nautilus
[33m8fd9134dd2[m Merge pull request #29231 from smithfarm/wip-40438-nautilus
[33m2e3efa36ab[m Merge pull request #29233 from smithfarm/wip-40440-nautilus
[33m460f504605[m Merge pull request #29275 from liewegas/wip-cephfs-meta-pool-priority-nautilus
[33m13cd9d5d4f[m Merge pull request #29343 from pdvian/wip-40443-nautilus
[33m30b129b87f[m Merge pull request #29344 from pdvian/wip-40445-nautilus
[33mb025e7e6cc[m mgr/dashboard: Fix e2e failures caused by webdriver version
[33mec446972ec[m mgr/volumes: set uid/gid of FS client's mount as 0/0
[33m142fe072c2[m mgr/volumes: add `ceph fs subvolumegroup getpath` command
[33meca9c0f2ec[m doc/rgw: document use of 'realm pull' instead of 'period pull'
[33md885f3c87d[m Client: unlink dentry for inode with llref=0
[33mf3a61452f9[m Merge pull request #29060 from sebastian-philipp/nautilus-orchestrator_cache
[33mb0a6e3d797[m Merge pull request #28187 from pdvian/wip-39516-nautilus
[33m94e8a40c55[m Merge pull request #28573 from smithfarm/wip-40281-nautilus
[33m2b16453c04[m Merge pull request #29007 from smithfarm/wip-40750-nautilus
[33m53e2584741[m Merge pull request #29102 from tspmelo/wip-40733-nautilus
[33mac0bc3e604[m Merge pull request #29162 from smithfarm/wip-40837-nautilus
[33mf27f6898d4[m Merge pull request #29204 from smithfarm/wip-40319-nautilus
[33m8ba30d2224[m Merge pull request #29391 from pdvian/wip-40625-nautilus
[33mf0ea9fa7f8[m Merge pull request #29464 from jan--f/wip-41058-nautilus
[33m4cdcc208a2[m ceph-volume: fall back to PARTTYPE if PARTLABEL is empty
[33m833ca7c60a[m ceph-volume: adjust tests for empty PARTLABEL fields
[33meac6767e90[m ceph-volume: refactor ceph-disk_member unittests
[33m59177f780c[m Merge pull request #29453 from neha-ojha/wip-41052-nautilus
[33m57350df0b1[m mgr/diskprediction_cloud: Service unavailable
[33m932e1d5713[m docs: fix rgw ldap username token
[33m044d9a6454[m qa/tasks/cbt.py: change port to work with client_endpoints
[33mbb8eaa8b7b[m Merge pull request #28740 from smithfarm/wip-40546-nautilus
[33m0caf2e8d52[m Merge pull request #28810 from ifed01/wip-ifed-db-stats-nau
[33m396c57051a[m Merge pull request #29252 from smithfarm/wip-40180-nautilus
[33m1db19cdfc7[m Merge pull request #29309 from smithfarm/wip-40216-nautilus
[33mc292b52f53[m doc: Update 'ceph-iscsi' min version
[33md2ab1e6d17[m doc/mgr/dashboard: update SSL configuration instructions
[33mee526b1827[m mgr/dashboard: added CLI commands to set SSL certificate and key
[33m8ba912f1bd[m mgr/dashboard: Add backwards compatibility to interlock of `fast-diff` and `object-map`
[33m9664cdd7ff[m mgr/dashboard: Interlock `fast-diff` and `object-map`
[33m2256298654[m rocksdb: Updated to v6.1.2
[33m29bafe5750[m rocksdb: enable rocksdb_rmrange=true by default
[33mcc3e208cad[m kv: make delete range optional on number of keys
[33m734b5199dc[m Merge pull request #29254 from smithfarm/wip-40465-nautilus
[33mf6ef7a321c[m Merge pull request #28528 from pdvian/wip-39743-nautilus
[33mdf1f1b1b9f[m Merge pull request #28575 from smithfarm/wip-40381-nautilus
[33m1bc3cc4aa2[m Merge pull request #28576 from smithfarm/wip-40382-nautilus
[33m80ba690d3f[m Merge pull request #28870 from pdvian/wip-40235-nautilus
[33m4a64a6deca[m Merge pull request #29193 from pdvian/wip-40272-nautilus
[33md62a92907a[m Merge pull request #29244 from pdvian/wip-40274-nautilus
[33m78afca1202[m Merge pull request #29246 from pdvian/wip-40276-nautilus
[33mff04418e6f[m Merge pull request #29315 from pdvian/wip-40293-nautilus
[33m614b026d22[m[33m ([m[1;31morigin/wip-41038-nautilus[m[33m)[m cmake: update FindBoost.cmake
[33m50e40ff5b7[m mgr/orchestrator: add --refresh to `ceph orchestrator service ls`
[33m5fc495f520[m mgr/orchestrator: allow listing iscsi services
[33m4c29da3ff3[m mgr/deepsea: return ganesha and iscsi endpoint URLs
[33mc3e3307bbf[m ceph-volume tests: ensure that better heuristics exist for objectstore detection
[33ma45caa00cf[m ceph-volume simple.activate better detect bluestore/filestore when type is not found
[33maa9d616ae0[m Merge pull request #28962 from ifed01/wip-ifed-kv-prefetch-nau
[33mec19164df8[m Merge pull request #28639 from xiexingguo/wip-pr-28404-for-n
[33m7d8738aade[m rgw_file: advance_mtime() should consider namespace expiration
[33mb9ce591d62[m rgw_file: fix readdir eof() calc--caller stop implies !eof
[33m3779219431[m Merge pull request #28891 from ifed01/wip-ifed-fix-alloc-dump-nau
[33m15ceb87f83[m Merge pull request #28892 from ifed01/wip-ifed-fix-no-compress-nau
[33mcfa5e4b5c1[m Merge pull request #28893 from ifed01/wip-ifed-remove-assert-bs-tool-nau
[33mf37ebcdf0c[m Merge pull request #28963 from ifed01/wip-ifed-add-omap-tail-nau
[33m840477f002[m Merge pull request #28966 from liewegas/wip-set-aio-write-max-nautilus
[33mcccacfba7e[m Merge pull request #29023 from ifed01/wip-ifed-fix-stuplid-alloc-len0-nau
[33m655ba2c5e8[m Merge pull request #29147 from pdvian/wip-40267-nautilus
[33m5a60c426c9[m Merge pull request #29168 from b-ranto/wip-n-mgr-metadata
[33m4a594e4152[m Merge pull request #29173 from neha-ojha/wip-40583-nautilus
[33m4e2a5e785a[m Merge pull request #29194 from pdvian/wip-40273-nautilus
[33m7ad4a3a42e[m Merge pull request #29207 from smithfarm/wip-40904-nautilus
[33m859dfb6697[m Merge pull request #29227 from ifed01/wip-ifed-slow-ops2-nau
[33mb09a987bea[m Merge pull request #28756 from smithfarm/wip-40231-nautilus
[33ma739847ffe[m Merge pull request #28869 from dzafman/wip-backport-40073
[33medf4541fea[m Merge pull request #28993 from smithfarm/wip-40730-nautilus
[33mbdea51ac8a[m Merge pull request #29115 from liewegas/wip-39693-nautilus
[33ma2c9186339[m Merge pull request #29140 from liewegas/wip-40441-nautilus
[33mfe28705e93[m Merge pull request #29141 from liewegas/wip-msg-async-1-nautilus
[33m83c99177e2[m Merge pull request #29142 from liewegas/wip-msg-noneed-set-connection-nautilus
[33m120c32dd57[m Merge pull request #29143 from liewegas/wip-secure-nautilus
[33m0cc85d043d[m Merge pull request #29159 from smithfarm/wip-40845-nautilus
[33mf24c9076bf[m Merge pull request #29188 from badone/wip-nautilus-lazy-omap-stats-backport-tracker-40744
[33mb15077d1a5[m mgr/dashboard: Fix the table mouseenter event handling test
[33m53209a9129[m Merge pull request #28550 from smithfarm/wip-40007-nautilus
[33m67686635e4[m Merge pull request #28648 from pdvian/wip-40107-nautilus
[33m7761be6df8[m Merge pull request #28712 from pdvian/wip-40134-nautilus
[33m5a86a4f67a[m Merge pull request #28713 from smithfarm/wip-40505-nautilus
[33m2214ed016a[m Merge pull request #28714 from smithfarm/wip-40515-nautilus
[33m202d31e419[m Merge pull request #28715 from smithfarm/wip-40508-nautilus
[33m6308bb61a8[m Merge pull request #28728 from joke-lee/nautilus
[33m67d34e8c4a[m Merge pull request #28729 from joke-lee/nautilus-backport-28172
[33mad25b7aaae[m Merge pull request #28735 from smithfarm/wip-40125-nautilus
[33m4c7d3f9260[m Merge pull request #28736 from smithfarm/wip-40129-nautilus
[33mf138e4ecf8[m Merge pull request #28737 from smithfarm/wip-40137-nautilus
[33m795e7b9f3d[m Merge pull request #28739 from smithfarm/wip-40142-nautilus
[33m38d20d54e3[m Merge pull request #28751 from pdvian/wip-40150-nautilus
[33m0c4bacc089[m Merge pull request #28854 from pdvian/wip-40226-nautilus
[33m3329ba2671[m Merge pull request #28886 from pdvian/wip-40263-nautilus
[33m3daecf0a18[m Merge pull request #29154 from smithfarm/wip-40591-nautilus
[33ma17b0a8949[m Merge pull request #29163 from smithfarm/wip-40851-nautilus
[33md085c1e20d[m Merge pull request #29205 from smithfarm/wip-40760-nautilus
[33m5e125c61ee[m Merge pull request #29265 from smithfarm/wip-40124-nautilus
[33m03c6ae8bbb[m Merge pull request #29286 from smithfarm/wip-40355-nautilus
[33mf6bf1a3d2b[m Merge pull request #29287 from smithfarm/wip-40358-nautilus
[33m2534e4a045[m Merge pull request #29310 from smithfarm/wip-40349-nautilus
[33m0c525c0f7e[m Merge pull request #29311 from smithfarm/wip-40352-nautilus
[33m790e8b2a45[m Merge pull request #29313 from smithfarm/wip-40512-nautilus
[33m2991d8c3e0[m Merge pull request #29325 from smithfarm/wip-40450-nautilus
[33mb918a6a550[m Merge pull request #28549 from smithfarm/wip-39749-nautilus
[33m0ba7113f13[m osd/OSD: auto mark heartbeat sessions as stale and tear them down
[33md1b8eaf6ea[m Merge pull request #29040 from dcasier/wip-40100-nautilus
[33m0d96a02333[m mon: take the mon lock in handle_conf_change
[33m7f2aa85af5[m osd/PG: do not queue scrub if PG is not active when unblock
[33m12f8b813b0[m mds: cleanup unneeded client_snap_caps when splitting snap inode
[33m0d3db9427c[m client: set snapdir's link count to 1
[33mc62118bdac[m rbd: use the ordered throttle for the export action
[33mea39d3ad44[m osd/OSDCap: rbd profile permits use of rbd.metadata_list cls method
[33m4c4def2fc3[m cls/rgw: keep issuing bilog trim ops after reset
[33mde16cf2a26[m qa/rgw: update default port in perl workunits
[33m33b23b50f0[m qa/rgw: extra s3tests tasks use rgw endpoint configuration
[33mee6490fee8[m rgw_file: pretty-print fh_key
[33mba98fada3f[m rgw_lc: use a new bl while encoding RGW_ATTR_LC
[33m1086b881e5[m rgw/multisite:RGWListBucketIndexesCR for data full sync pagination
[33mc40ab6bcdb[m rgw/OutputDataSocket: actually discard data on full buffer
[33me30b2577d9[m rgw_file: permit lookup_handle to lookup root_fh
[33mc3bb6e0b53[m rgw: perfcounters: add gc retire counter
[33m85480c8d8e[m doc: fixed --read-only argument value in multisite doc
[33m8767765002[m mgr/dashboard: Time diff service
[33m27d2c748bc[m mgr/dashboard: Handle unset settings
[33m6db62fe675[m mgr/dashboard: Silence Alertmanager alerts
[33mfc1c90659b[m mgr/dashboard: Allow test selection overwrite
[33m0b805a39a5[m mgr/dashboard: Add succeeded action labels
[33m0d1523c7ad[m rbd-nbd: sscanf return 0 mean not-match.
[33m85fa207048[m Merge pull request #29260 from jan--f/wip-40921-nautilus
[33md0de0a3d31[m rgw: set null version object acl issues
[33mc1cd4066e9[m rgw: provide admin friendly reshard status output
[33mb6dd70f46a[m rgw: update the "radosgw-admin reshard status" command documentation with expected output examples
[33mfd7f0a610e[m mon/FSCommand: set pg_num_min via 'fs new', not in mgr/volumes
[33m4e5f998fab[m mon/FSCommands: set pg_autoscale_factor on 'fs new', not via mgr/volumes
[33ma4dd46c3f2[m mon: set recovery_priority=5 on fs metadata pool
[33ma525832da5[m qa/rgw: add dnsmasq back to s3a-hadoop
[33m74ac48eb8b[m qa/rgw: remove ceph-ansible from s3a-hadoop suite
[33m094fb88efa[m qa/rgw: use default ports (80 or 443) unless overridden
[33md26f5e70e7[m qa/rgw: rgw task can override --rgw-dns-name on the command line
[33m71d264d6e1[m qa/rgw: allow rgw client config to override port
[33mee902e2fe3[m ceph-volume:util: Use proper param substition
[33mbf68d8e54e[m osd: copy (don't move) pg list when sending beacon
[33m420e227357[m test: Make sure that extra scheduled scrubs don't confuse test
[33me3611502db[m Merge pull request #28958 from smithfarm/wip-40710-nautilus
[33mba7decb510[m Merge pull request #28919 from LenzGr/nautilus-documentation
[33m0c3c61f654[m Merge pull request #28738 from smithfarm/wip-40140-nautilus
[33m9ce492f799[m librbd: do not unblock IO prior to growing object map during resize
[33m85ac3a1f32[m librados: move buffer free functions to inline namespace
[33m622ff61524[m mds: trim cache during standby replay Fixes:http://tracker.ceph.com/issues/40213 Signed-off-by: simon gao <simon29rock@gmail.com>
[33m789786c659[m mds: don't mark cap NEEDSNAPFLUSH if client has no pending capsnap
[33m7a922c8287[m os/bluestore: add slow op detector for collection listing
[33mc65a4e4a90[m os/bluestore: parametrize latency threshold for log_latency funcs..
[33mf22e5d8d17[m os/bluestore: cleanup around slow op logging.
[33m2f01d2d6f4[m os/bluestore: fix origin reference in logging slow ops.
[33mc7684e459c[m qa/tasks/cephfs/test_volume_client: print py2 or py3 which the test case runs
[33m445bfffa10[m mgr/influx: try to call close()
[33md63544c8d5[m rgw: Save an unnecessary copy of RGWEnv
[33mca9bf6e56f[m qa/workunits/rados/test_envlibrados_for_rocksdb: support SUSE distros
[33m4e76baa233[m qa/workunits/rados/test_librados_build.sh: install build deps
[33m35e13fdc77[m qa/workunits/rados/test_envlibrados_for_rocksdb: use helper script
[33meeaa59ea2d[m qa/workunits/ceph-helpers-root: use /etc/os-release instead
[33m2169c36504[m os/bluestore: proper locking for BlueFS prefetching
[33m0cc014f0e0[m osd: Modify lazy omap stats to only show one copy
[33m565ac08c37[m tests: Add test for lazy omap stat collection
[33mf102f4bdfd[m Client: bump ll_ref from int32 to uint64_t
[33m0e9a0ff18f[m Merge pull request #29137 from yuriw/wip-yuriw-40832-nautilus
[33m3fdf6d0b81[m mgr: use ipv4 default when ipv6 was disabled
[33mdcaf976db0[m rbd-mirror: link against the specified alloc library
[33mba2eb4689f[m Merge pull request #28768 from dzafman/wip-40265
[33me53e4d66c8[m Merge pull request #29050 from rhcs-dashboard/fix-40768-nautilus
[33m48ede0000c[m qa/tests: added 14.2.2 to the mix
[33m53a549ed35[m Merge pull request #28230 from ivancich/nautilus-wip-rgw-admin-unordered
[33m76e0387811[m Merge pull request #28769 from dillaman/wip-40572-nautilus
[33mc1cd350e09[m Merge pull request #28816 from trociny/wip-40462-nautilus
[33mdd52f38586[m Merge pull request #28817 from trociny/wip-40594-nautilus
[33m661a6d87be[m Merge pull request #28937 from smithfarm/wip-systemd-dep-suse-nautilus
[33md8180c57ac[m common/options.cc: Lower the default value of osd_deep_scrub_large_omap_object_key_threshold
[33m5b6e4f5ddd[m Add mgr metdata to prometheus exporter module
[33ma877a89e0d[m Added single check to avoid duplication. Included few more commands.
[33m7e353df85a[m rgw: Fail radosgw-admin commands on non-master zone that modify metadata but with an option to override, allowing changes only on the local zone.
[33m37136b4ca5[m common/options: Set concurrent bluestore rocksdb compactions to 2
[33ma27fd26066[m mon/MDSMonitor: use stringstream instead of dout for mds repaired
[33m2d322fca24[m cephfs-shell: Remove undefined variable files in do_rm()
[33m14f3979fed[m client: support the fallocate() when fuse version >= 2.9
[33mdd504d05a1[m cephfs-shell: Fix TypeError in poutput()
[33mb364390b5d[m rgw: minor code clean-up
[33m61d115dbc0[m rgw: allow multipart upload abort to proceed
[33m43e3920a7b[m common: OutputDataSocket retakes mutex on error path
[33md370937ce2[m common/options: allow (but to not prefer or require) secure mode
[33m2d3c8e3063[m common/options: make clients prefer to connect to mons via secure mode
[33mfd0f46d1a5[m common/options: allow connections to mons in secure mode
[33m54a10d2555[m common/options: prefer secure mode between monitors
[33m2c39190fab[m auth/AuthRegistry: remove experimental flag for 'secure' mode
[33m87efe7f90f[m msg/async: no-need set connection for Message.
[33maba609e725[m msg/async: avoid unnecessary costly wakeups for outbound messages
[33m34727cbc45[m qa/valgrind.supp: be slightly less specific on suppression
[33m7226568219[m msg/async, v2: make the reset_recv_state() unconditional.
[33mc0deeb6c4c[m mgr/dashboard: Add, update and remove translations
[33ma7a380a44f[m Merge PR #29079 into nautilus
[33md5c0ec5de9[m os/bluestore: be verbose about objects that existing on rmcoll
[33m9c3931e27c[m osd/PrimaryLogPG: disallow ops on objects with an empty name
[33mf895e0c50e[m osd/PG: fix cleanup of pgmeta-like objects on PG deletion
[33ma70e5ecb51[m mgr/dashboard: Fix aditional npm vulnerabilities
[33mee8e21c678[m mgr/dashboard: Fix npm vulnerabilities
[33m4f8fa0a002[m[33m ([m[1;33mtag: v14.2.2[m[33m)[m 14.2.2
[33maad32e02a9[m mgr/dashboard: Switch ng2-toastr for ngx-toastr
[33m2bc3a8bac9[m client: do not return EEXIST for mkdirs
[33mbd4a74d799[m pybind/mgr/volumes: print exceptions in purge thread
[33m6c8e81a0a1[m pybind/mgr/volumes: refactor trash readdir
[33m5002ac0a87[m pybind/mgr/volumes: use existing client provided recursive mkdir
[33m0ecebbad97[m pybind/mgr/volumes: cleanup fs removal
[33m1f22288559[m pybind/mgr/subvolumes: use bytes for paths
[33m6f81ef87f0[m pybind/mgr/volumes: remove unused property
[33md2b3051b9a[m test: cleanup removing all subvolumes before removing subvolume group
[33mb17eee937d[m mgr / volumes: wrap rmtree() call within try..except block
[33m3aee03a0ff[m mgr / volumes: use negative error codes everywhere
[33m2004ab53ae[m test: add basic purge queue validation test
[33m09fb1f7311[m mgr / volumes: schedule purge job for volumes on init
[33m6de9dbbe17[m mgr / volumes: purge queue for async subvolume delete
[33m64b5c0aa8e[m mgr / volumes: maintain connection pool for fs volumes
[33mbb9048a7b0[m mgr/volumes: do not import unused module
[33m64f035c0ee[m Merge PR #28589 into nautilus
[33mb4980226d0[m Merge PR #28646 into nautilus
[33m35e52c0aca[m Merge PR #29032 into nautilus
[33m25b33d3280[m Merge pull request #29028 from ceph/backport-nautilus-28060
[33m1c38e44b93[m mgr/dashboard: s/portal_ip_address/portal_ip_addresses/
[33mc191c27059[m mgr/dashboard: Optimize portal IPs calculation
[33m83ef771312[m mgr/dashboard: Optimize target edition when a portal is removed
[33mf4577b62ff[m mgr/deepsea: gracefully handle nonexistent nodes in {service,device} ls
[33m4a862be702[m mgr/deepsea: rejig service cache
[33m542fa9fa25[m mgr/orchestrator: fix some minor typos/kinks in inventory/service cache
[33maf445ed75e[m mgr/orchestrator: Impove type hint for describe_service
[33m9ab890f907[m mgr/orchestrator: Introduce OutdatableDictMixin
[33m4ddf834f0a[m mgr/orchestrator: Add cache for Inventory and Services
[33m57a769c941[m mgr/dashboard: Display iSCSI "logged in" info
[33m21aff48d33[m mgr/dashboard: RGW rest client instances cache eviction
[33m9bfd3ab0de[m ceph-volume: lvm.activate: Return an error if WAL/DB devices absent
[33madfcc5e572[m qa/tasks/mgr/dashboard/test_health: fix test_full_health test
[33maacfa8f08c[m mon: use per-pool stats only when all OSDs are reporting
[33m841710dfdf[m osd: report whether we have per-pool stats
[33m151915bc0d[m osd/osd_types: osd_stat_t: include num_per_pool_osds member
[33m248994d8bb[m Merge pull request #29022 from smithfarm/wip-40762-nautilus
[33m41db5388c2[m[33m ([m[1;31morigin/backport-nautilus-28060[m[33m)[m ceph-volume: skip missing interpreters when running tox tests
[33maa6da416cb[m ceph-volume: use the Device.rotational property instead of sys_api
[33m1d8bd0680a[m os/bluestore: avoid length overflow in extents returned by Stupid Allocator.
[33mb9c652a326[m rgw: always generate after delimiter char to skip directory
[33me9b13b0a2e[m rgw: fix list bucket with start maker and delimiter '/' will miss next object with char '0'
[33m2e2a4e3bb1[m packaging: remove SuSEfirewall2 support
[33md6f58697c7[m mon/AuthMonitor: clear_secrets() in create_initial()
[33mc02295a07a[m auth/cephx/CephxKeyServer: make clear_secrets() clear rotating secrets too
[33m92e0e0befb[m debian/control: add python-routes dependency
[33m44dd1f2382[m mds: check last laggy before marking unresponsive client stale
[33mf2e19bbea0[m mds: remove the code that skip evicting the only client
[33m2e1506022c[m qa/cephfs: update tests for stale session handling
[33m081932f63a[m mds: change how mds revoke stale caps
[33ma9dfbadb15[m mds: don't mark unresponsive sessions holding no caps stale
[33m671c719a3d[m mgr/dashboard: Rename iSCSI gateways name to FQDN
[33mda610ce1ed[m mgr/dashboard: Upgrade ceph-iscsi config to version 10
[33m838616ae5d[m ceph_test_objectstore: add very_large_write test
[33m935ea30cfa[m os/bluestore: fix aio pwritev lost data problem.
[33m0c4bc84077[m os/bluestore: create the tail when first set FLAG_OMAP
[33m8da82b04df[m os/bluestore: protect BlueFS::FileReader::buf
[33mcb4b329105[m os/bluestore: support RocksDB prefetching at BlueFS
[33m5f36fc0be6[m os/bluestore: call BlueFS::_init_logger earlier.
[33m15e1a9549f[m os/bluestore: log 'buffered' in KernelDevice::read_random
[33mbd80b158e4[m os/bluestore: optimize mapping offset to extent in BlueFS.
[33ma347d29e9a[m Merge pull request #27723 from ceph/backport-nautilus-26957
[33m5d2441c5d8[m doc: cover more cache modes in rados/operations/cache-tiering.rst
[33md53aafeb94[m[33m ([m[1;31morigin/backport-nautilus-26957[m[33m)[m ceph-volume: look for rotational data in lsblk
[33mcb6cc8d6f0[m Packaging: Drop systemd BuildRequires in case of building for SUSE
[33m6e9f39bad4[m Merge pull request #28923 from ceph/backport-nautilus-28294
[33ma2fbaea6a9[m Merge pull request #28925 from ceph/backport-nautilus-28866
[33md2c53596c9[m Merge pull request #28924 from ceph/backport-nautilus-28836
[33mb38a0ce96a[m ceph-volume api.lvm catch IndexError when parsing dmmapper output
[33mc12a9eace2[m ceph-volume tests update to use error.value instead of str(error)
[33m7c29205647[m[33m ([m[1;31morigin/backport-nautilus-28836[m[33m)[m ceph-volume tests add a sleep in tox for slow OSDs after booting
[33m305527f38f[m tests: pass --ssh-config to pytest to resolve hosts when connecting
[33me4801eb284[m Merge pull request #28922 from cbodley/wip-qa-rgw-swift-server-nautilus
[33m3fbd25774d[m qa/rgw: clean up arguments for swift task
[33m07640bd46e[m qa/rgw: swift task filters out config for skipped clients
[33m84763779a9[m qa/rgw: swift task looks for rgw_server endpoint
[33m18cbb45030[m doc: Improved dashboard feature overview
[33ma3f5c73c57[m mgr/dashboard: use mds_mem.dn for fs dentries
[33m3222c7b67a[m Merge pull request #28872 from badone/wip-40670-nautilus
[33md09d6f0798[m Merge pull request #28871 from badone/wip-40669-nautilus
[33m4552354580[m os/bluestore/bluestore-tool: omit device from both source and taget lists when migrating.
[33m758336160e[m os/bluestore/bluestore-tool: fix error output
[33m750b774208[m os/bluestore/bluestore-tool: do not assert when migrate command fails.
[33mc8f37bcbd2[m os/bluestore: load OSD all compression settings unconditionally.
[33m5e42c6b99c[m os/bluestore: more smart allocator dump when lacking space for bluefs.
[33m407cecb0cc[m mgr/dashboard: fix MDS charts are stacked in Filesystems page
[33maf54923a80[m rgw_file: all directories are virtual with respect to contents
[33m88f0d5724d[m qa/ceph-ansible: Move to ansible 2.8
