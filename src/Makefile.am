AUTOMAKE_OPTIONS = gnu
SUBDIRS = ocf java
DIST_SUBDIRS = gtest ocf libs3 java

EXTRA_DIST = \
	libs3/COPYING \
	libs3/ChangeLog \
	libs3/GNUmakefile \
	libs3/GNUmakefile.mingw \
	libs3/GNUmakefile.osx \
	libs3/INSTALL \
	libs3/LICENSE \
	libs3/README \
	libs3/TODO \
	libs3/archlinux \
	libs3/debian \
	libs3/doxyfile \
	libs3/inc \
	libs3/libs3.spec \
	libs3/mswin \
	libs3/src \
	libs3/test \
	unittest_bufferlist.sh

CLEANFILES =
bin_PROGRAMS =
# like bin_PROGRAMS, but these targets are only built for debug builds
bin_DEBUGPROGRAMS =
sbin_PROGRAMS =
# like sbin_SCRIPTS but can be used to install to e.g. /usr/sbin
ceph_sbindir = $(prefix)$(sbindir)
ceph_sbin_SCRIPTS = \
	ceph-disk \
	ceph-disk-prepare \
	ceph-disk-activate \
	ceph-disk-udev \
	ceph-create-keys

sbin_SCRIPTS = \
	mount.fuse.ceph

bin_SCRIPTS = ceph-run $(srcdir)/ceph-clsinfo ceph-debugpack ceph-rbdnamer
dist_bin_SCRIPTS =
# C/C++ tests to build will be appended to this
check_PROGRAMS =
# tests to actually run on "make check"; if you need extra, non-test,
# executables built, you need to replace this with manual assignments
# target by target
TESTS = $(check_PROGRAMS) unittest_bufferlist.sh

check-local:
	$(srcdir)/test/encoding/check-generated.sh
	$(srcdir)/test/encoding/readable.sh ../ceph-object-corpus

EXTRALIBS = -luuid
if FREEBSD
EXTRALIBS += -lexecinfo
endif
if LINUX
EXTRALIBS += -lrt
endif
if WITH_PROFILER
EXTRALIBS += -lprofiler
endif

LIBGLOBAL_LDA = libglobal.la $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)

LIBOS_LDA = libos.a

if WITH_LIBAIO
LIBOS_LDA += -laio
endif

# use system leveldb
LIBOS_LDA += -lleveldb -lsnappy

# monitor
ceph_mon_SOURCES = ceph_mon.cc common/TextTable.cc
ceph_mon_LDFLAGS = $(AM_LDFLAGS)
ceph_mon_LDADD = libmon.a $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_mon_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
bin_PROGRAMS += ceph-mon

ceph_mon_store_converter_SOURCES =  mon_store_converter.cc \
			       mon/MonitorStore.cc
ceph_mon_store_converter_LDFLAGS = ${AM_LDFLAGS}
ceph_mon_store_converter_LDADD =  $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_mon_store_converter_CXXFLAGS = ${AM_CXXFLAGS}
bin_PROGRAMS += ceph_mon_store_converter


# osd
ceph_osd_SOURCES = ceph_osd.cc objclass/class_debug.cc \
	       objclass/class_api.cc
ceph_osd_LDADD = libosd.a $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_osd_CXXFLAGS = ${AM_CXXFLAGS}
bin_PROGRAMS += ceph-osd

if LINUX
ceph_osd_LDADD += -ldl
endif

# mds
ceph_mds_SOURCES = ceph_mds.cc
ceph_mds_LDADD = libmds.a libosdc.la $(LIBGLOBAL_LDA)
bin_PROGRAMS += ceph-mds
ceph_mds_CXXFLAGS = ${AM_CXXFLAGS}

# admin tools
ceph_SOURCES = \
	tools/ceph.cc \
	tools/common.cc \
	mon/PGMap.cc
ceph_LDADD = $(LIBGLOBAL_LDA) $(LIBEDIT_LIBS)
ceph_CXXFLAGS = ${AM_CXXFLAGS}

ceph_conf_SOURCES = ceph_conf.cc
ceph_conf_LDADD = $(LIBGLOBAL_LDA)
ceph_conf_CXXFLAGS = ${AM_CXXFLAGS}
ceph_authtool_SOURCES = ceph_authtool.cc
ceph_authtool_LDADD = $(LIBGLOBAL_LDA)
ceph_filestore_dump_SOURCES = tools/ceph-filestore-dump.cc objclass/class_debug.cc \
	       objclass/class_api.cc
ceph_filestore_dump_SOURCES += perfglue/disabled_heap_profiler.cc
ceph_filestore_dump_LDADD = libosd.a $(LIBOS_LDA) $(LIBGLOBAL_LDA) -lboost_program_options
if LINUX
ceph_filestore_dump_LDADD += -ldl
endif
bin_PROGRAMS += ceph ceph-conf ceph-authtool ceph_filestore_dump

ceph_osdomap_tool_SOURCES = tools/ceph-osdomap-tool.cc \
			  os/LevelDBStore.cc
ceph_osdomap_tool_LDFLAGS = ${AM_LDFLAGS}
ceph_osdomap_tool_LDADD =  $(LIBOS_LDA) $(LIBGLOBAL_LDA) -lboost_program_options
ceph_osdomap_tool_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph-osdomap-tool

ceph_monstore_tool_SOURCES = tools/ceph-monstore-tool.cc \
			  os/LevelDBStore.cc
ceph_monstore_tool_LDFLAGS = ${AM_LDFLAGS}
ceph_monstore_tool_LDADD =  $(LIBOS_LDA) $(LIBGLOBAL_LDA) -lboost_program_options
ceph_monstore_tool_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph-monstore-tool

monmaptool_SOURCES = monmaptool.cc
monmaptool_LDADD = $(LIBGLOBAL_LDA)
crushtool_SOURCES = crushtool.cc
crushtool_LDADD = $(LIBGLOBAL_LDA)
osdmaptool_SOURCES = osdmaptool.cc
osdmaptool_LDADD = $(LIBGLOBAL_LDA)
bin_PROGRAMS += monmaptool crushtool osdmaptool

rgw_dencoder_src = rgw/rgw_dencoder.cc \
                   rgw/rgw_acl.cc \
                   rgw/rgw_common.cc \
                   rgw/rgw_env.cc \
                   rgw/rgw_json_enc.cc

ceph_dencoder_SOURCES = test/encoding/ceph_dencoder.cc ${rgw_dencoder_src} perfglue/disabled_heap_profiler.cc
ceph_dencoder_CXXFLAGS = ${AM_CXXFLAGS}
ceph_dencoder_LDADD = $(LIBGLOBAL_LDA) libcls_lock_client.a \
			  libcls_rgw_client.a libcls_replica_log_client.a \
			  libosd.a libmds.a libosdc.la $(LIBOS_LDA) libmon.a
bin_PROGRAMS += ceph-dencoder

mount_ceph_SOURCES = mount/mount.ceph.c common/armor.c common/safe_io.c common/secret.c include/addr_parsing.c
mount_ceph_LDADD = -lkeyutils
if LINUX
sbin_PROGRAMS += mount.ceph
endif

# user tools
cephfs_SOURCES = cephfs.cc
cephfs_LDADD = libcommon.la
bin_PROGRAMS += cephfs

librados_config_SOURCES = librados-config.cc
librados_config_LDADD = libglobal.la librados.la $(PTHREAD_LIBS) $(EXTRALIBS) $(CRYPTO_LIBS)
bin_PROGRAMS += librados-config

# synthetic client
ceph_syn_SOURCES = ceph_syn.cc client/SyntheticClient.cc
ceph_syn_LDADD = libclient.la $(LIBGLOBAL_LDA)
bin_PROGRAMS += ceph-syn

base: ceph-mon ceph-osd ceph-mds \
	ceph cephfs \
	ceph-syn \
	rados radosgw librados-config \
	ceph-conf monmaptool osdmaptool crushtool ceph-authtool \
	init-ceph mkcephfs mon_store_converter


# fuse targets?
if WITH_FUSE
ceph_fuse_SOURCES = ceph_fuse.cc client/fuse_ll.cc
ceph_fuse_LDADD = -lfuse libclient.la $(LIBGLOBAL_LDA)
ceph_fuse_CXXFLAGS = ${AM_CXXFLAGS}
bin_PROGRAMS += ceph-fuse

rbd_fuse_SOURCES = rbd_fuse/rbd-fuse.c
rbd_fuse_LDADD = -lfuse librados.la librbd.la $(LIBGLOBAL_LDA)
rbd_fuse_CXXFLAGS = ${AM_CXXFLAGS}
bin_PROGRAMS += rbd-fuse

endif

# tcmalloc?
if WITH_TCMALLOC
tcmalloc_safety_flags = -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free
ceph_osd_LDADD += -ltcmalloc
ceph_osd_CXXFLAGS += ${tcmalloc_safety_flags}
ceph_osd_SOURCES += perfglue/heap_profiler.cc
ceph_mds_LDADD += -ltcmalloc
ceph_mds_CXXFLAGS += ${tcmalloc_safety_flags}
ceph_mds_SOURCES += perfglue/heap_profiler.cc
ceph_mon_LDADD += -ltcmalloc
ceph_mon_CXXFLAGS += ${tcmalloc_safety_flags}
ceph_mon_SOURCES += perfglue/heap_profiler.cc
if WITH_FUSE
ceph_fuse_LDADD += -ltcmalloc
ceph_fuse_CXXFLAGS += ${tcmalloc_safety_flags}
endif #WITH_FUSE
else
ceph_osd_SOURCES += perfglue/disabled_heap_profiler.cc
ceph_mds_SOURCES += perfglue/disabled_heap_profiler.cc
ceph_mon_SOURCES += perfglue/disabled_heap_profiler.cc
endif # WITH_TCMALLOC

# debug targets
ceph_psim_SOURCES = psim.cc
ceph_psim_LDADD = $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_psim

ceph_test_mutate_SOURCES = test/test_mutate.cc
ceph_test_mutate_LDADD = libglobal.la librados.la $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_DEBUGPROGRAMS += ceph_test_mutate

ceph_test_rewrite_latency_SOURCES = test/test_rewrite_latency.cc
ceph_test_rewrite_latency_LDADD = libcommon.la $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_DEBUGPROGRAMS += ceph_test_rewrite_latency

ceph_test_msgr_SOURCES = testmsgr.cc
ceph_test_msgr_LDADD = $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_test_msgr

ceph_test_ioctls_SOURCES = client/test_ioctls.c
bin_DEBUGPROGRAMS += ceph_test_ioctls

ceph_dupstore_SOURCES = dupstore.cc
ceph_dupstore_CXXFLAGS= ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
ceph_dupstore_LDADD = $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_streamtest_SOURCES = streamtest.cc
ceph_streamtest_CXXFLAGS= ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
ceph_streamtest_LDADD = $(LIBOS_LDA) $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_dupstore ceph_streamtest

ceph_test_trans_SOURCES = test_trans.cc
ceph_test_trans_CXXFLAGS= ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
ceph_test_trans_LDADD = $(LIBOS_LDA) $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_test_trans

ceph_test_rados_SOURCES = test/osd/TestRados.cc test/osd/TestOpStat.cc test/osd/Object.cc  test/osd/RadosModel.cc
ceph_test_rados_LDADD = librados.la $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_test_rados

ceph_smalliobench_SOURCES = test/bench/small_io_bench.cc test/bench/rados_backend.cc test/bench/detailed_stat_collector.cc test/bench/bencher.cc
ceph_smalliobench_LDADD = librados.la -lboost_program_options $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_smalliobench

ceph_smalliobenchfs_SOURCES = test/bench/small_io_bench_fs.cc test/bench/filestore_backend.cc test/bench/detailed_stat_collector.cc test/bench/bencher.cc
ceph_smalliobenchfs_LDADD = librados.la -lboost_program_options $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_smalliobenchfs_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_smalliobenchfs

ceph_smalliobenchdumb_SOURCES = test/bench/small_io_bench_dumb.cc test/bench/dumb_backend.cc test/bench/detailed_stat_collector.cc test/bench/bencher.cc
ceph_smalliobenchdumb_LDADD = librados.la -lboost_program_options $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_smalliobenchdumb_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_smalliobenchdumb

ceph_smalliobenchrbd_SOURCES = test/bench/small_io_bench_rbd.cc test/bench/rbd_backend.cc test/bench/detailed_stat_collector.cc test/bench/bencher.cc
ceph_smalliobenchrbd_LDADD = librados.la librbd.la -lboost_program_options $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_smalliobenchrbd

ceph_tpbench_SOURCES = test/bench/tp_bench.cc test/bench/detailed_stat_collector.cc
ceph_tpbench_LDADD = librados.la -lboost_program_options $(LIBOS_LDA) $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_tpbench

ceph_omapbench_SOURCES = test/omap_bench.cc
ceph_omapbench_LDADD = librados.la $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_omapbench

ceph_kvstorebench_SOURCES = test/kv_store_bench.cc key_value_store/kv_flat_btree_async.cc
ceph_kvstorebench_LDADD = librados.la $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_kvstorebench

ceph_multi_stress_watch_SOURCES = test/multi_stress_watch.cc test/librados/test.cc
ceph_multi_stress_watch_LDADD = librados.la $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_multi_stress_watch 

if WITH_BUILD_TESTS
ceph_test_libcommon_build_SOURCES = test/test_libcommon_build.cc $(libcommon_files)
ceph_test_libcommon_build_LDADD = $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_DEBUGPROGRAMS += ceph_test_libcommon_build

ceph_test_librados_build_SOURCES = test/test_libcommon_build.cc $(libcommon_files) $(librados_SOURCES)
ceph_test_librados_build_LDADD = $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
ceph_test_librados_build_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_librados_build

ceph_test_librgw_build_SOURCES = test/test_libcommon_build.cc $(libcommon_files) \
			    $(librados_SOURCES) $(librgw_la_SOURCES)
ceph_test_librgw_build_LDADD = -lexpat $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
ceph_test_librgw_build_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_librgw_build

ceph_test_libcephfs_build_SOURCES = test/test_libcommon_build.cc $(libcommon_files) \
			    $(libosdc_la_SOURCES)
ceph_test_libcephfs_build_LDADD = libcephfs.la -lexpat $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
ceph_test_libcephfs_build_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_libcephfs_build
endif

if WITH_HADOOPCLIENT
ceph_test_libhadoopcephfs_build_SOURCES = test/test_libcommon_build.cc \
         $(libhadoopcephfs_la_SOURCES) \
	 $(libosdc_la_SOURCES) $(libcommon_files)
ceph_test_libhadoopcephfs_build_LDADD = libcephfs.la -lexpat $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
ceph_test_libhadoopcephfs_build_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_libhadoopcephfs_build
endif

##########
BUILT_SOURCES =
lib_LTLIBRARIES = 
noinst_LTLIBRARIES = 
noinst_LIBRARIES =

# libcephfs
libcephfs_la_SOURCES = \
	libcephfs.cc
libcephfs_la_CFLAGS= ${CRYPTO_CFLAGS} ${AM_CFLAGS}
libcephfs_la_CXXFLAGS= ${AM_CXXFLAGS}
libcephfs_la_LIBADD = libclient.la
libcephfs_la_LDFLAGS =  $(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS) \
            ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '^ceph_.*'
lib_LTLIBRARIES += libcephfs.la

ceph_test_timers_SOURCES = test/TestTimers.cc
ceph_test_timers_LDADD = $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_test_timers

ceph_test_signal_handlers_SOURCES = test/TestSignalHandlers.cc
ceph_test_signal_handlers_LDADD = $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_test_signal_handlers

# librados
librados_SOURCES = \
	librados/librados.cc \
	librados/RadosClient.cc \
	librados/IoCtxImpl.cc \
	osdc/Objecter.cc \
	osdc/Striper.cc
librados_la_SOURCES = ${librados_SOURCES}
librados_la_CFLAGS = ${CRYPTO_CFLAGS} ${AM_CFLAGS}
librados_la_CXXFLAGS = ${AM_CXXFLAGS}
librados_la_LIBADD = libcommon.la $(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS)
librados_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0 -export-symbols-regex '^rados_.*'
lib_LTLIBRARIES += librados.la

if WITH_RADOSGW

# rgw
librgw_a_SOURCES =  \
	rgw/librgw.cc \
	rgw/rgw_acl.cc \
	rgw/rgw_acl_s3.cc \
	rgw/rgw_acl_swift.cc \
	rgw/rgw_client_io.cc \
	rgw/rgw_fcgi.cc \
	rgw/rgw_xml.cc \
	rgw/rgw_usage.cc \
	rgw/rgw_json_enc.cc \
	rgw/rgw_user.cc \
	rgw/rgw_bucket.cc\
        rgw/rgw_tools.cc \
	rgw/rgw_rados.cc \
        rgw/rgw_http_client.cc \
        rgw/rgw_rest_client.cc \
	rgw/rgw_rest_conn.cc \
	rgw/rgw_op.cc \
	rgw/rgw_common.cc \
	rgw/rgw_cache.cc \
	rgw/rgw_formats.cc \
	rgw/rgw_log.cc \
	rgw/rgw_multi.cc \
	rgw/rgw_policy_s3.cc \
	rgw/rgw_gc.cc \
	rgw/rgw_multi_del.cc \
	rgw/rgw_env.cc \
	rgw/rgw_cors.cc \
	rgw/rgw_cors_s3.cc \
        rgw/rgw_auth_s3.cc \
	rgw/rgw_metadata.cc \
	rgw/rgw_replica_log.cc
librgw_a_CFLAGS = ${CRYPTO_CFLAGS} ${AM_CFLAGS}
librgw_a_CXXFLAGS = -Woverloaded-virtual ${AM_CXXFLAGS}
noinst_LIBRARIES += librgw.a

my_radosgw_ldadd = \
       libglobal.la librgw.a librados.la libcls_rgw_client.a libcls_log_client.a \
       libcls_statelog_client.a libcls_replica_log_client.a libcls_lock_client.a \
       libcls_refcount_client.a libcls_version_client.a -lcurl -lexpat \
       $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)

radosgw_SOURCES = \
	rgw/rgw_resolve.cc \
        rgw/rgw_rest.cc \
        rgw/rgw_rest_swift.cc \
        rgw/rgw_rest_s3.cc \
        rgw/rgw_rest_usage.cc \
        rgw/rgw_rest_user.cc \
        rgw/rgw_rest_bucket.cc \
        rgw/rgw_rest_metadata.cc \
        rgw/rgw_replica_log.cc \
        rgw/rgw_rest_log.cc \
        rgw/rgw_rest_opstate.cc \
        rgw/rgw_rest_replica_log.cc \
        rgw/rgw_rest_config.cc \
        rgw/rgw_http_client.cc \
        rgw/rgw_swift.cc \
	rgw/rgw_swift_auth.cc \
	rgw/rgw_main.cc
radosgw_LDADD = $(my_radosgw_ldadd) -lfcgi -lresolv
radosgw_CXXFLAGS = ${AM_CXXFLAGS}
bin_PROGRAMS += radosgw

radosgw_admin_SOURCES = rgw/rgw_admin.cc
radosgw_admin_CXXFLAGS = ${AM_CXXFLAGS}
radosgw_admin_LDADD = $(my_radosgw_ldadd)
bin_PROGRAMS += radosgw-admin

ceph_rgw_multiparser_SOURCES = rgw/rgw_multiparser.cc
ceph_rgw_multiparser_CXXFLAGS = ${AM_CXXFLAGS}
ceph_rgw_multiparser_LDADD = $(my_radosgw_ldadd)
bin_DEBUGPROGRAMS += ceph_rgw_multiparser

ceph_rgw_jsonparser_SOURCES = rgw/rgw_jsonparser.cc rgw/rgw_common.cc rgw/rgw_env.cc rgw/rgw_json_enc.cc
ceph_rgw_jsonparser_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
ceph_rgw_jsonparser_LDADD = $(my_radosgw_ldadd)
bin_DEBUGPROGRAMS += ceph_rgw_jsonparser

endif

# librbd
librbd_la_SOURCES = \
	librbd/librbd.cc \
	librbd/AioCompletion.cc \
	librbd/AioRequest.cc \
	cls/rbd/cls_rbd_client.cc \
	librbd/ImageCtx.cc \
	librbd/internal.cc \
	librbd/LibrbdWriteback.cc \
	librbd/WatchCtx.cc \
	osdc/ObjectCacher.cc \
	osdc/Striper.cc \
	librados/snap_set_diff.cc \
	cls/lock/cls_lock_client.cc \
	cls/lock/cls_lock_types.cc \
	cls/lock/cls_lock_ops.cc \
	common/util.cc
librbd_la_CFLAGS = ${AM_CFLAGS} ${CRYPTO_CFLAGS}
librbd_la_CXXFLAGS = ${AM_CXXFLAGS}
librbd_la_LIBADD = librados.la
librbd_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 \
	-export-symbols-regex '^rbd_.*' $(PTHREAD_LIBS) $(EXTRALIBS) 
lib_LTLIBRARIES += librbd.la

rados_SOURCES = rados.cc rados_import.cc rados_export.cc rados_sync.cc common/obj_bencher.cc
rados_LDADD = libglobal.la libcls_lock_client.a librados.la $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_PROGRAMS += rados

if WITH_REST_BENCH

rest_bench_SOURCES = tools/rest_bench.cc common/obj_bencher.cc
rest_bench_LDADD = libglobal.la $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
rest_bench_CXXFLAGS = ${AM_CXXFLAGS}
bin_PROGRAMS += rest-bench

if WITH_SYSTEM_LIBS3
rest_bench_LDADD += -ls3
else
rest_bench_LDADD += libs3/build/lib/libs3.a -lcurl -lxml2
rest_bench_CXXFLAGS += -I$(top_srcdir)/src/libs3/inc
SUBDIRS += libs3
endif

endif

ceph_scratchtool_SOURCES = scratchtool.c
ceph_scratchtool_LDADD = librados.la $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
ceph_scratchtoolpp_SOURCES = scratchtoolpp.cc
ceph_scratchtoolpp_LDADD = librados.la $(PTHREAD_LIBS) -lm
ceph_radosacl_SOURCES = radosacl.cc
ceph_radosacl_LDADD = librados.la $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_DEBUGPROGRAMS += ceph_scratchtool ceph_scratchtoolpp ceph_radosacl

rbd_SOURCES = rbd.cc common/fiemap.cc common/secret.c common/TextTable.cc common/util.cc
rbd_CXXFLAGS = ${AM_CXXFLAGS}
rbd_LDADD = libglobal.la librbd.la librados.la $(PTHREAD_LIBS) -lm -lkeyutils $(CRYPTO_LIBS) $(EXTRALIBS)
if LINUX
bin_PROGRAMS += rbd
endif


ceph_test_crypto_SOURCES = testcrypto.cc
ceph_test_crypto_LDADD =  $(LIBGLOBAL_LDA)
ceph_test_crypto_CXXFLAGS = ${AM_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_crypto

ceph_test_keys_SOURCES = testkeys.cc
ceph_test_keys_LDADD = libmon.a $(LIBGLOBAL_LDA) 
ceph_test_keys_CXXFLAGS = ${AM_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_keys

if WITH_TCMALLOC
ceph_test_keys_LDADD += -ltcmalloc
ceph_test_keys_CXXFLAGS += ${tcmalloc_safety_flags}
ceph_test_keys_SOURCES += perfglue/heap_profiler.cc
endif


## rados object classes

# rbd: rados block device class
libcls_rbd_la_SOURCES = cls/rbd/cls_rbd.cc
libcls_rbd_la_CFLAGS = ${AM_CFLAGS}
libcls_rbd_la_CXXFLAGS= ${AM_CXXFLAGS}
libcls_rbd_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_rbd_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__cls_.*'

radoslibdir = $(libdir)/rados-classes
radoslib_LTLIBRARIES = libcls_rbd.la


# lock class
libcls_lock_la_SOURCES = cls/lock/cls_lock.cc
libcls_lock_la_CFLAGS = ${AM_CFLAGS}
libcls_lock_la_CXXFLAGS= ${AM_CXXFLAGS}
libcls_lock_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_lock_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__cls_.*'

radoslib_LTLIBRARIES += libcls_lock.la

# refcount class
libcls_refcount_la_SOURCES = cls/refcount/cls_refcount.cc
libcls_refcount_la_CFLAGS = ${AM_CFLAGS}
libcls_refcount_la_CXXFLAGS= ${AM_CXXFLAGS}
libcls_refcount_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_refcount_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__cls_.*'

radoslib_LTLIBRARIES += libcls_refcount.la

# version class
libcls_version_la_SOURCES = cls/version/cls_version.cc
libcls_version_la_CFLAGS = ${AM_CFLAGS}
libcls_version_la_CXXFLAGS= ${AM_CXXFLAGS}
libcls_version_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_version_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__cls_.*'

radoslib_LTLIBRARIES += libcls_version.la

# log class
libcls_log_la_SOURCES = cls/log/cls_log.cc
libcls_log_la_CFLAGS = ${AM_CFLAGS}
libcls_log_la_CXXFLAGS= ${AM_CXXFLAGS}
libcls_log_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_log_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__cls_.*'

radoslib_LTLIBRARIES += libcls_log.la

libcls_statelog_la_SOURCES = cls/statelog/cls_statelog.cc
libcls_statelog_la_CFLAGS = ${AM_CFLAGS}
libcls_statelog_la_CXXFLAGS= ${AM_CXXFLAGS}
libcls_statelog_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_statelog_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__cls_.*'

radoslib_LTLIBRARIES += libcls_statelog.la

# replica log class
libcls_replica_log_la_SOURCES = cls/replica_log/cls_replica_log.cc
libcls_replica_log_la_CFLAGS = ${AM_CFLAGS}
libcls_replica_log_la_CXXFLAGS= ${AM_CXXFLAGS}
libcls_replica_log_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_replica_log_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__cls_.*'

radoslib_LTLIBRARIES += libcls_replica_log.la

if WITH_RADOSGW
# rgw: rados gateway
libcls_rgw_la_SOURCES = cls/rgw/cls_rgw.cc
libcls_rgw_la_CFLAGS = ${AM_CFLAGS}
libcls_rgw_la_CXXFLAGS= ${AM_CXXFLAGS}
libcls_rgw_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_rgw_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__cls_.*'

radoslib_LTLIBRARIES += libcls_rgw.la
endif

libcls_lock_client_a_SOURCES =  \
	cls/lock/cls_lock_client.cc  \
	cls/lock/cls_lock_types.cc \
	cls/lock/cls_lock_ops.cc
noinst_LIBRARIES += libcls_lock_client.a

libcls_refcount_client_a_SOURCES =  \
	cls/refcount/cls_refcount_client.cc
noinst_LIBRARIES += libcls_refcount_client.a

libcls_version_client_a_SOURCES =  \
	cls/version/cls_version_client.cc \
	cls/version/cls_version_types.cc
noinst_LIBRARIES += libcls_version_client.a

libcls_log_client_a_SOURCES =  \
	cls/log/cls_log_client.cc
noinst_LIBRARIES += libcls_log_client.a

libcls_statelog_client_a_SOURCES =  \
	cls/statelog/cls_statelog_client.cc
noinst_LIBRARIES += libcls_statelog_client.a

libcls_replica_log_client_a_SOURCES = \
	cls/replica_log/cls_replica_log_types.cc \
	cls/replica_log/cls_replica_log_ops.cc \
	cls/replica_log/cls_replica_log_client.cc
noinst_LIBRARIES += libcls_replica_log_client.a

libcls_rgw_client_a_SOURCES =  \
	cls/rgw/cls_rgw_client.cc \
	cls/rgw/cls_rgw_types.cc \
	cls/rgw/cls_rgw_ops.cc
noinst_LIBRARIES += libcls_rgw_client.a

## hadoop client
if WITH_HADOOPCLIENT
JAVA_BASE = /usr/lib/jvm/java-6-sun
libhadoopcephfs_la_SOURCES = client/hadoop/CephFSInterface.cc
libhadoopcephfs_la_LIBADD = libcephfs.la
libhadoopcephfs_la_CFLAGS = ${AM_CFLAGS}
libhadoopcephfs_la_CXXFLAGS = ${AM_CXXFLAGS}
libhadoopcephfs_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex 'hadoopcephfs_.*'
lib_LTLIBRARIES += libhadoopcephfs.la

endif

## CephFS Java Wrappers
##  - The JNI library is here
##  - The Java source Makefile.am is in src/java
if ENABLE_CEPHFS_JAVA
libcephfs_jni_la_SOURCES = \
	java/native/libcephfs_jni.cc \
	java/native/ScopedLocalRef.h \
	java/native/JniConstants.cpp \
	java/native/JniConstants.h
libcephfs_jni_la_LIBADD = libcephfs.la $(EXTRALIBS)
libcephfs_jni_la_CPPFLAGS = $(JDK_CPPFLAGS)
libcephfs_jni_la_CXXFLAGS = ${AM_CXXFLAGS}
libcephfs_jni_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
lib_LTLIBRARIES += libcephfs_jni.la
endif

## key_value_store classes

# key_value_store: key value store class
libcls_kvs_la_SOURCES = key_value_store/cls_kvs.cc
libcls_kvs_la_CFLAGS = ${AM_CFLAGS}
libcls_kvs_la_CXXFLAGS= ${AM_CXXFLAGS}
libcls_kvs_la_LIBADD = $(PTHREAD_LIBS) $(EXTRALIBS)
libcls_kvs_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__cls_.*'

radoslib_LTLIBRARIES += libcls_kvs.la

## System tests
libsystest_la_SOURCES = \
	test/system/cross_process_sem.cc \
	test/system/systest_runnable.cc \
	test/system/systest_settings.cc
libsystest_la_LIBADD = libglobal.la
noinst_LTLIBRARIES += libsystest.la

ceph_test_rados_list_parallel_SOURCES = \
	test/system/rados_list_parallel.cc \
	test/system/st_rados_create_pool.cc \
	test/system/st_rados_list_objects.cc
ceph_test_rados_list_parallel_LDADD = libsystest.la librados.la $(PTHREAD_LIBS)
bin_DEBUGPROGRAMS += ceph_test_rados_list_parallel

ceph_test_rados_open_pools_parallel_SOURCES = \
	test/system/rados_open_pools_parallel.cc \
	test/system/st_rados_create_pool.cc
ceph_test_rados_open_pools_parallel_LDADD = libsystest.la librados.la $(PTHREAD_LIBS)
bin_DEBUGPROGRAMS += ceph_test_rados_open_pools_parallel

ceph_test_rados_delete_pools_parallel_SOURCES = \
	test/system/rados_delete_pools_parallel.cc \
	test/system/st_rados_create_pool.cc \
	test/system/st_rados_delete_pool.cc \
	test/system/st_rados_list_objects.cc
ceph_test_rados_delete_pools_parallel_LDADD = libsystest.la librados.la $(PTHREAD_LIBS)
bin_DEBUGPROGRAMS += ceph_test_rados_delete_pools_parallel

ceph_test_rados_watch_notify_SOURCES = \
	test/system/rados_watch_notify.cc \
	test/system/st_rados_create_pool.cc \
	test/system/st_rados_delete_pool.cc \
	test/system/st_rados_delete_objs.cc \
	test/system/st_rados_watch.cc \
	test/system/st_rados_notify.cc
ceph_test_rados_watch_notify_LDADD = libsystest.la librados.la $(PTHREAD_LIBS)
bin_DEBUGPROGRAMS += ceph_test_rados_watch_notify

ceph_bench_log_SOURCES = \
	test/bench_log.cc
ceph_bench_log_LDADD = libcommon.la libglobal.la $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_DEBUGPROGRAMS += ceph_bench_log

## unit tests

# target to build but not run the unit tests
unittests:: $(check_PROGRAMS)

UNITTEST_CXXFLAGS = \
	-I$(top_srcdir)/src/gtest/include \
	-I$(top_builddir)/src/gtest/include
UNITTEST_STATIC_LDADD = \
	$(top_builddir)/src/gtest/lib/libgtest.a \
	$(top_builddir)/src/gtest/lib/libgtest_main.a \
	$(PTHREAD_LIBS)
UNITTEST_LDADD = ${UNITTEST_STATIC_LDADD}

unittest_encoding_SOURCES = test/encoding.cc
unittest_encoding_LDADD = libcephfs.la librados.la $(PTHREAD_LIBS) -lm \
			  ${UNITTEST_LDADD}
unittest_encoding_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS} \
		     -fno-strict-aliasing
check_PROGRAMS += unittest_encoding

unittest_addrs_SOURCES = test/test_addrs.cc
unittest_addrs_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
unittest_addrs_LDADD = libglobal.la $(PTHREAD_LIBS) -lm ${UNITTEST_LDADD} $(CRYPTO_LIBS) $(EXTRALIBS)
check_PROGRAMS += unittest_addrs

unittest_util_SOURCES = test/common/test_util.cc common/util.cc
unittest_util_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
unittest_util_LDADD = libcommon.la $(PTHREAD_LIBS) -lm ${UNITTEST_LDADD} $(CRYPTO_LIBS) $(EXTRALIBS)
check_PROGRAMS += unittest_util

unittest_workqueue_SOURCES = test/test_workqueue.cc
unittest_workqueue_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
unittest_workqueue_LDADD = libglobal.la $(PTHREAD_LIBS) -lm ${UNITTEST_LDADD} $(CRYPTO_LIBS) $(EXTRALIBS)
check_PROGRAMS += unittest_workqueue

unittest_striper_SOURCES = test/test_striper.cc 
unittest_striper_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
unittest_striper_LDADD = libglobal.la libosdc.la $(PTHREAD_LIBS) -lm ${UNITTEST_LDADD} $(CRYPTO_LIBS) $(EXTRALIBS)
check_PROGRAMS += unittest_striper

unittest_prebufferedstreambuf_SOURCES = test/test_prebufferedstreambuf.cc common/PrebufferedStreambuf.cc
unittest_prebufferedstreambuf_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
unittest_prebufferedstreambuf_LDADD = ${UNITTEST_LDADD} $(EXTRALIBS)
check_PROGRAMS += unittest_prebufferedstreambuf

unittest_str_list_SOURCES = test/test_str_list.cc
unittest_str_list_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
unittest_str_list_LDADD = libglobal.la $(PTHREAD_LIBS) -lm ${UNITTEST_LDADD} $(CRYPTO_LIBS) $(EXTRALIBS)
check_PROGRAMS += unittest_str_list

unittest_log_SOURCES = log/test.cc common/PrebufferedStreambuf.cc
unittest_log_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_log_LDADD = libcommon.la ${UNITTEST_LDADD}
unittest_log_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS} -O2
check_PROGRAMS += unittest_log

unittest_throttle_SOURCES = test/common/Throttle.cc
unittest_throttle_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_throttle_LDADD = libcommon.la ${LIBGLOBAL_LDA} ${UNITTEST_LDADD}
unittest_throttle_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS} -O2
check_PROGRAMS += unittest_throttle

unittest_base64_SOURCES = test/base64.cc
unittest_base64_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_base64_LDADD = libcephfs.la -lm ${UNITTEST_LDADD}
unittest_base64_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_base64

unittest_ceph_argparse_SOURCES = test/ceph_argparse.cc
unittest_ceph_argparse_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_ceph_argparse_LDADD = libglobal.la ${UNITTEST_LDADD}
unittest_ceph_argparse_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_ceph_argparse

unittest_osd_types_SOURCES = test/test_osd_types.cc
unittest_osd_types_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
unittest_osd_types_LDADD = libglobal.la libcommon.la $(PTHREAD_LIBS) -lm ${UNITTEST_LDADD} $(CRYPTO_LIBS) $(EXTRALIBS)
check_PROGRAMS += unittest_osd_types

unittest_gather_SOURCES = test/gather.cc
unittest_gather_LDADD = ${LIBGLOBAL_LDA} ${UNITTEST_LDADD}
unittest_gather_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_gather

unittest_run_cmd_SOURCES = test/run_cmd.cc
unittest_run_cmd_LDADD = libcephfs.la ${UNITTEST_LDADD}
unittest_run_cmd_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_run_cmd

unittest_signals_SOURCES = test/signals.cc
unittest_signals_LDADD = ${LIBGLOBAL_LDA} ${UNITTEST_LDADD}
unittest_signals_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_signals

unittest_simple_spin_SOURCES = test/simple_spin.cc
unittest_simple_spin_LDADD = libcephfs.la ${UNITTEST_LDADD}
unittest_simple_spin_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_simple_spin

unittest_librados_SOURCES = test/librados/librados.cc
unittest_librados_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_librados_LDADD = librados.la ${UNITTEST_LDADD}
unittest_librados_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_librados

unittest_bufferlist_SOURCES = test/bufferlist.cc
unittest_bufferlist_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA) 
unittest_bufferlist_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_bufferlist

unittest_crypto_SOURCES = test/crypto.cc
unittest_crypto_LDFLAGS = ${CRYPTO_LDFLAGS} ${AM_LDFLAGS}
unittest_crypto_LDADD =  ${LIBGLOBAL_LDA} ${UNITTEST_LDADD}
unittest_crypto_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_crypto

unittest_perf_counters_SOURCES = test/perf_counters.cc
unittest_perf_counters_LDFLAGS = ${AM_LDFLAGS}
unittest_perf_counters_LDADD =  ${LIBGLOBAL_LDA} ${UNITTEST_LDADD}
unittest_perf_counters_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_perf_counters

unittest_admin_socket_SOURCES = test/admin_socket.cc
unittest_admin_socket_LDFLAGS = ${AM_LDFLAGS}
unittest_admin_socket_LDADD =  ${LIBGLOBAL_LDA} ${UNITTEST_LDADD} libcommon.la
unittest_admin_socket_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_admin_socket

unittest_ceph_crypto_SOURCES = test/ceph_crypto.cc
unittest_ceph_crypto_LDFLAGS = ${CRYPTO_LDFLAGS} ${AM_LDFLAGS}
unittest_ceph_crypto_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_ceph_crypto_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_ceph_crypto

unittest_utf8_SOURCES = test/utf8.cc
unittest_utf8_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_utf8_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_utf8_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_utf8

unittest_mime_SOURCES = test/mime.cc
unittest_mime_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_mime_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_mime_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_mime

unittest_escape_SOURCES = test/escape.cc
unittest_escape_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_escape_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_escape_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_escape

unittest_chain_xattr_SOURCES = test/filestore/chain_xattr.cc
unittest_chain_xattr_LDFLAGS = ${AM_LDFLAGS}
unittest_chain_xattr_LDADD =  ${UNITTEST_STATIC_LDADD} $(LIBOS_LDA) $(LIBGLOBAL_LDA)
unittest_chain_xattr_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS} ${CRYPTO_CXXFLAGS}
check_PROGRAMS += unittest_chain_xattr

unittest_flatindex_SOURCES = test/os/TestFlatIndex.cc
unittest_flatindex_LDFLAGS = ${AM_LDFLAGS}
unittest_flatindex_LDADD =  ${UNITTEST_STATIC_LDADD} $(LIBOS_LDA) $(LIBGLOBAL_LDA)
unittest_flatindex_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS} ${CRYPTO_CXXFLAGS}
check_PROGRAMS += unittest_flatindex

unittest_strtol_SOURCES = test/strtol.cc
unittest_strtol_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_strtol_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_strtol_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_strtol

unittest_confutils_SOURCES = test/confutils.cc
unittest_confutils_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_confutils_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_confutils_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_confutils

unittest_heartbeatmap_SOURCES = test/heartbeat_map.cc common/HeartbeatMap.cc
unittest_heartbeatmap_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_heartbeatmap_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_heartbeatmap_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_heartbeatmap

unittest_formatter_SOURCES = test/formatter.cc rgw/rgw_formats.cc
unittest_formatter_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_formatter_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_formatter_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_formatter

unittest_libcephfs_config_SOURCES = test/libcephfs_config.cc
unittest_libcephfs_config_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_libcephfs_config_LDADD =  libcephfs.la ${UNITTEST_LDADD}
unittest_libcephfs_config_CXXFLAGS = ${CRYPTO_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_libcephfs_config

unittest_lfnindex_SOURCES = test/os/TestLFNIndex.cc
unittest_lfnindex_LDFLAGS = ${AM_LDFLAGS}
unittest_lfnindex_LDADD =  ${UNITTEST_STATIC_LDADD} $(LIBOS_LDA) $(LIBGLOBAL_LDA)
unittest_lfnindex_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS} ${CRYPTO_CXXFLAGS}
check_PROGRAMS += unittest_lfnindex

unittest_librados_config_SOURCES = test/librados/librados_config.cc
unittest_librados_config_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_librados_config_LDADD =  librados.la ${UNITTEST_LDADD}
unittest_librados_config_CXXFLAGS = ${CRYPTO_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_librados_config

#unittest_librgw_link_SOURCES = test/librgw_link.cc
#unittest_librgw_link_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
#unittest_librgw_link_LDADD =  librgw.la ${UNITTEST_LDADD}
#unittest_librgw_link_CXXFLAGS = ${CRYPTO_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
#check_PROGRAMS += unittest_librgw_link

unittest_daemon_config_SOURCES = test/daemon_config.cc
unittest_daemon_config_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_daemon_config_LDADD =  ${UNITTEST_LDADD} ${LIBGLOBAL_LDA}
unittest_daemon_config_CXXFLAGS = ${CRYPTO_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_daemon_config

unittest_osd_osdcap_SOURCES = test/osd/osdcap.cc osd/OSDCap.cc
unittest_osd_osdcap_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_osd_osdcap_LDADD =  ${UNITTEST_LDADD} ${LIBGLOBAL_LDA}
unittest_osd_osdcap_CXXFLAGS = ${CRYPTO_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_osd_osdcap

#if WITH_RADOSGW
#unittest_librgw_SOURCES = test/librgw.cc
#unittest_librgw_LDFLAGS = -lrt $(PTHREAD_CFLAGS) -lcurl ${AM_LDFLAGS}
#unittest_librgw_LDADD =  librgw.la librados.la ${UNITTEST_LDADD} -lexpat $(LIBGLOBAL_LDA)
#unittest_librgw_CXXFLAGS = ${CRYPTO_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
#check_PROGRAMS += unittest_librgw
#endif

unittest_ipaddr_SOURCES = test/test_ipaddr.cc
unittest_ipaddr_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_ipaddr_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_ipaddr

unittest_texttable_SOURCES = test/test_texttable.cc common/TextTable.cc 
unittest_texttable_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
unittest_texttable_LDADD = librados.la ${UNITTEST_LDADD}
unittest_texttable_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_texttable

if WITH_RADOSGW
ceph_test_cors_SOURCES = test/test_cors.cc
ceph_test_cors_LDFLAGS = libglobal.la
ceph_test_cors_LDADD = librados.la librgw.a ${UNITTEST_LDADD} ${UNITTEST_STATIC_LDADD} $(CRYPTO_LIBS) -lcurl -luuid -lexpat
ceph_test_cors_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_cors

unittest_rgw_meta_SOURCES = test/test_rgw_admin_meta.cc
unittest_rgw_meta_LDFLAGS = libglobal.la 
unittest_rgw_meta_LDADD = librgw.a ${UNITTEST_LDADD} ${UNITTEST_STATIC_LDADD} -lcryptopp -lcurl -luuid -lexpat  librados.la libcls_version_client.a \
 libcls_log_client.a libcls_statelog_client.a libcls_refcount_client.a libcls_rgw_client.a libcls_lock_client.a
unittest_rgw_meta_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += unittest_rgw_meta

unittest_rgw_log_SOURCES = test/test_rgw_admin_log.cc
unittest_rgw_log_LDFLAGS = libglobal.la 
unittest_rgw_log_LDADD = librgw.a ${UNITTEST_LDADD} ${UNITTEST_STATIC_LDADD} -lcryptopp -lcurl -luuid -lexpat  librados.la libcls_version_client.a \
 libcls_log_client.a libcls_statelog_client.a libcls_refcount_client.a libcls_rgw_client.a libcls_lock_client.a
unittest_rgw_log_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += unittest_rgw_log

unittest_rgw_opstate_SOURCES = test/test_rgw_admin_opstate.cc
unittest_rgw_opstate_LDFLAGS = libglobal.la 
unittest_rgw_opstate_LDADD = librgw.a ${UNITTEST_LDADD} ${UNITTEST_STATIC_LDADD} -lcryptopp -lcurl -luuid -lexpat  librados.la libcls_version_client.a \
 libcls_log_client.a libcls_statelog_client.a libcls_refcount_client.a libcls_rgw_client.a libcls_lock_client.a
unittest_rgw_opstate_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += unittest_rgw_opstate
endif

ceph_test_librbd_SOURCES = test/librbd/test_librbd.cc test/librados/test.cc
ceph_test_librbd_LDADD =  librbd.la librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_librbd_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_librbd

ceph_test_librbd_fsx_SOURCES = test/librbd/fsx.c
ceph_test_librbd_fsx_LDADD =  librbd.la librados.la -lm
ceph_test_librbd_fsx_CFLAGS = ${AM_CFLAGS} -Wno-format
bin_DEBUGPROGRAMS += ceph_test_librbd_fsx

ceph_test_cls_rbd_SOURCES = test/cls_rbd/test_cls_rbd.cc \
	test/librados/test.cc \
	cls/rbd/cls_rbd_client.cc \
	cls/lock/cls_lock_client.cc \
	cls/lock/cls_lock_types.cc \
	cls/lock/cls_lock_ops.cc
ceph_test_cls_rbd_LDADD = librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_cls_rbd_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_cls_rbd

ceph_test_cls_refcount_SOURCES = test/cls_refcount/test_cls_refcount.cc \
	test/librados/test.cc
ceph_test_cls_refcount_LDADD = librados.la libcls_refcount_client.a ${UNITTEST_STATIC_LDADD}
ceph_test_cls_refcount_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_cls_refcount

ceph_test_cls_version_SOURCES = test/cls_version/test_cls_version.cc \
	test/librados/test.cc
ceph_test_cls_version_LDADD = librados.la libcls_version_client.a ${UNITTEST_STATIC_LDADD}
ceph_test_cls_version_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_cls_version

ceph_test_cls_log_SOURCES = test/cls_log/test_cls_log.cc \
	test/librados/test.cc
ceph_test_cls_log_LDADD = libglobal.la librados.la libcls_log_client.a ${UNITTEST_STATIC_LDADD}
ceph_test_cls_log_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_cls_log

ceph_test_cls_statelog_SOURCES = test/cls_statelog/test_cls_statelog.cc \
	test/librados/test.cc
ceph_test_cls_statelog_LDADD = libglobal.la librados.la libcls_statelog_client.a ${UNITTEST_STATIC_LDADD}
ceph_test_cls_statelog_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_cls_statelog
ceph_test_cls_replica_log_SOURCES = \
	test/cls_replica_log/test_cls_replica_log.cc \
	test/librados/test.cc
ceph_test_cls_replica_log_LDADD = libglobal.la librados.la \
	libcls_replica_log_client.a ${UNITTEST_STATIC_LDADD}
ceph_test_cls_replica_log_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_cls_replica_log

ceph_test_cls_lock_SOURCES = test/cls_lock/test_cls_lock.cc test/librados/test.cc
ceph_test_cls_lock_LDFLAGS = ${AM_LDFLAGS}
ceph_test_cls_lock_LDADD =  libcls_lock_client.a librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_cls_lock_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_cls_lock

if WITH_RADOSGW

ceph_test_cls_rgw_SOURCES = test/cls_rgw/test_cls_rgw.cc \
	test/librados/test.cc
ceph_test_cls_rgw_LDADD = librados.la libcls_rgw_client.a ${UNITTEST_STATIC_LDADD}
ceph_test_cls_rgw_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_cls_rgw

endif

ceph_test_mon_workloadgen_SOURCES = \
     test/mon/test_mon_workloadgen.cc \
		 osdc/Objecter.cc \
		 osdc/Striper.cc
ceph_test_mon_workloadgen_LDADD = $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_test_mon_workloadgen_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_mon_workloadgen

ceph_test_rados_api_io_SOURCES = test/librados/io.cc test/librados/test.cc
ceph_test_rados_api_io_LDFLAGS = ${AM_LDFLAGS}
ceph_test_rados_api_io_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_rados_api_io_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_rados_api_io

ceph_test_rados_api_aio_SOURCES = test/librados/aio.cc test/librados/test.cc
ceph_test_rados_api_aio_LDFLAGS = ${AM_LDFLAGS}
ceph_test_rados_api_aio_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_rados_api_aio_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_rados_api_aio

ceph_test_rados_api_list_SOURCES = test/librados/list.cc test/librados/test.cc
ceph_test_rados_api_list_LDFLAGS = ${AM_LDFLAGS}
ceph_test_rados_api_list_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_rados_api_list_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_rados_api_list

ceph_test_rados_api_pool_SOURCES = test/librados/pool.cc test/librados/test.cc
ceph_test_rados_api_pool_LDFLAGS = ${AM_LDFLAGS}
ceph_test_rados_api_pool_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_rados_api_pool_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_rados_api_pool

ceph_test_rados_api_stat_SOURCES = test/librados/stat.cc test/librados/test.cc
ceph_test_rados_api_stat_LDFLAGS = ${AM_LDFLAGS}
ceph_test_rados_api_stat_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_rados_api_stat_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_rados_api_stat

ceph_test_rados_api_watch_notify_SOURCES = test/librados/watch_notify.cc test/librados/test.cc
ceph_test_rados_api_watch_notify_LDFLAGS = ${AM_LDFLAGS}
ceph_test_rados_api_watch_notify_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_rados_api_watch_notify_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_rados_api_watch_notify

ceph_test_rados_api_snapshots_SOURCES = test/librados/snapshots.cc test/librados/test.cc
ceph_test_rados_api_snapshots_LDFLAGS = ${AM_LDFLAGS}
ceph_test_rados_api_snapshots_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_rados_api_snapshots_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_rados_api_snapshots

ceph_test_rados_api_cls_SOURCES = test/librados/cls.cc test/librados/test.cc
ceph_test_rados_api_cls_LDFLAGS = ${AM_LDFLAGS}
ceph_test_rados_api_cls_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_rados_api_cls_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_rados_api_cls

ceph_test_rados_api_misc_SOURCES = test/librados/misc.cc test/librados/test.cc
ceph_test_rados_api_misc_LDFLAGS = ${AM_LDFLAGS}
ceph_test_rados_api_misc_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_rados_api_misc_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_rados_api_misc

ceph_test_libcephfs_SOURCES = test/libcephfs/test.cc test/libcephfs/readdir_r_cb.cc test/libcephfs/caps.cc
ceph_test_libcephfs_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
ceph_test_libcephfs_LDADD =  ${UNITTEST_STATIC_LDADD} libcephfs.la
ceph_test_libcephfs_CXXFLAGS = $(AM_CXXFLAGS) ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_libcephfs

ceph_test_filestore_SOURCES = test/filestore/store_test.cc
ceph_test_filestore_LDFLAGS = ${AM_LDFLAGS}
ceph_test_filestore_LDADD =  ${UNITTEST_STATIC_LDADD} $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_test_filestore_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS} ${CRYPTO_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_filestore

ceph_test_filestore_workloadgen_SOURCES = \
     test/filestore/workload_generator.cc \
     test/filestore/TestFileStoreState.cc
ceph_test_filestore_workloadgen_LDFLAGS = ${AM_LDFLAGS}
ceph_test_filestore_workloadgen_LDADD =  $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_test_filestore_workloadgen_CXXFLAGS =  ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_filestore_workloadgen

ceph_test_filestore_idempotent_SOURCES = test/filestore/test_idempotent.cc test/filestore/FileStoreTracker.cc test/common/ObjectContents.cc
ceph_test_filestore_idempotent_LDADD = $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_test_filestore_idempotent_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_filestore_idempotent

ceph_test_filestore_idempotent_sequence_SOURCES = \
     test/filestore/test_idempotent_sequence.cc \
     test/filestore/DeterministicOpSequence.cc \
     test/filestore/TestFileStoreState.cc \
     test/filestore/FileStoreDiff.cc
ceph_test_filestore_idempotent_sequence_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
ceph_test_filestore_idempotent_sequence_LDADD = $(LIBOS_LDA) $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += ceph_test_filestore_idempotent_sequence

ceph_xattr_bench_SOURCES = test/xattr_bench.cc
ceph_xattr_bench_LDFLAGS = ${AM_LDFLAGS}
ceph_xattr_bench_LDADD =  ${UNITTEST_STATIC_LDADD} $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_xattr_bench_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}  ${CRYPTO_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_xattr_bench

ceph_test_filejournal_SOURCES = test/test_filejournal.cc
ceph_test_filejournal_LDFLAGS = ${AM_LDFLAGS}
ceph_test_filejournal_LDADD =  ${UNITTEST_STATIC_LDADD} $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_test_filejournal_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_filejournal

ceph_test_stress_watch_SOURCES = test/test_stress_watch.cc test/librados/test.cc
ceph_test_stress_watch_LDFLAGS = ${AM_LDFLAGS}
ceph_test_stress_watch_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
ceph_test_stress_watch_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_stress_watch

ceph_test_objectcacher_stress_SOURCES = test/osdc/object_cacher_stress.cc test/osdc/FakeWriteback.cc osdc/ObjectCacher.cc
ceph_test_objectcacher_stress_LDFLAGS = ${AM_LDFLAGS}
ceph_test_objectcacher_stress_LDADD = $(LIBGLOBAL_LDA)
ceph_test_objectcacher_stress_CXXFLAGS = ${AM_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_objectcacher_stress

ceph_test_snap_mapper_SOURCES = test/test_snap_mapper.cc osd/SnapMapper.cc
ceph_test_snap_mapper_LDFLAGS = ${AM_LDFLAGS}
ceph_test_snap_mapper_LDADD =  ${UNITTEST_STATIC_LDADD} $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_test_snap_mapper_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS} $(LEVELDB_INCLUDE) ${CRYPTO_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_snap_mapper

ceph_test_object_map_SOURCES = test/ObjectMap/test_object_map.cc test/ObjectMap/KeyValueDBMemory.cc os/DBObjectMap.cc os/LevelDBStore.cc
ceph_test_object_map_LDFLAGS = ${AM_LDFLAGS}
ceph_test_object_map_LDADD =  ${UNITTEST_STATIC_LDADD} $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_test_object_map_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS} ${CRYPTO_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_object_map

ceph_test_keyvaluedb_atomicity_SOURCES = test/ObjectMap/test_keyvaluedb_atomicity.cc os/LevelDBStore.cc
ceph_test_keyvaluedb_atomicity_LDFLAGS = ${AM_LDFLAGS}
ceph_test_keyvaluedb_atomicity_LDADD =  ${UNITTEST_STATIC_LDADD} $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_test_keyvaluedb_atomicity_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS} ${CRYPTO_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_keyvaluedb_atomicity

ceph_test_keyvaluedb_iterators_SOURCES = test/ObjectMap/test_keyvaluedb_iterators.cc \
                                    test/ObjectMap/KeyValueDBMemory.cc \
                                    os/LevelDBStore.cc
ceph_test_keyvaluedb_iterators_LDFLAGS = ${AM_LDFLAGS}
ceph_test_keyvaluedb_iterators_LDADD =  ${UNITTEST_STATIC_LDADD} $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_test_keyvaluedb_iterators_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS} ${CRYPTO_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_keyvaluedb_iterators

ceph_test_store_tool_SOURCES = test/ObjectMap/test_store_tool/test_store_tool.cc \
			  os/LevelDBStore.cc
ceph_test_store_tool_LDFLAGS = ${AM_LDFLAGS}
ceph_test_store_tool_LDADD =  $(LIBOS_LDA) $(LIBGLOBAL_LDA)
ceph_test_store_tool_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_store_tool

ceph_test_cfuse_cache_invalidate_SOURCES = test/test_cfuse_cache_invalidate.cc
ceph_test_cfuse_cache_invalidate_LDFLAGS = ${AM_LDFLAGS}
ceph_test_cfuse_cache_invalidate_LDADD =
ceph_test_cfuse_cache_invalidate_CXXFLAGS = ${AM_CXXFLAGS}
bin_DEBUGPROGRAMS += ceph_test_cfuse_cache_invalidate

# shell scripts
editpaths = sed \
	-e 's|@bindir[@]|$(bindir)|g' \
	-e 's|@sbindir[@]|$(sbindir)|g' \
	-e 's|@libdir[@]|$(libdir)|g' \
	-e 's|@sysconfdir[@]|$(sysconfdir)|g' \
	-e 's|@datadir[@]|$(pkgdatadir)|g' \
	-e 's|@prefix[@]|$(prefix)|g' \
	-e 's|@@GCOV_PREFIX_STRIP[@][@]|$(GCOV_PREFIX_STRIP)|g'

shell_scripts = init-ceph mkcephfs ceph-debugpack ceph-coverage

$(shell_scripts): Makefile

$(shell_scripts): %: %.in
	rm -f $@ $@.tmp
	$(editpaths) '$(srcdir)/$@.in' >$@.tmp
	chmod +x $@.tmp
	chmod a-w $@.tmp
	mv $@.tmp $@

BUILT_SOURCES += init-ceph
sbin_SCRIPTS += mkcephfs

bin_SCRIPTS += ceph-coverage

CLEANFILES += $(shell_scripts)

CLEANFILES += ceph_ver.h sample.fetch_config

##

AM_COMMON_FLAGS = -Wall -D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT \
-D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE -rdynamic \
${WARN_TYPE_LIMITS} ${WARN_IGNORED_QUALIFIERS} -Winit-self -Wpointer-arith \
-fno-strict-aliasing

AM_CFLAGS = $(AM_COMMON_FLAGS)
AM_CXXFLAGS = \
	@AM_CXXFLAGS@ \
	$(AM_COMMON_FLAGS) \
	-DCEPH_LIBDIR=\"${libdir}\" \
	-Wnon-virtual-dtor \
	-Wno-invalid-offsetof \
	-Wstrict-null-sentinel
# note: this is position dependant, it affects the -l options that
# come after it on the command line. when you use ${AM_LDFLAGS} in
# later rules, take care where you place it. for more information, see
# http://blog.flameeyes.eu/2008/11/19/relationship-between-as-needed-and-no-undefined-part-1-what-do-they-do
# http://blog.flameeyes.eu/2008/11/20/misguided-link-and-as-needed
# http://www.gentoo.org/proj/en/qa/asneeded.xml
# http://gcc.gnu.org/ml/gcc-help/2010-12/msg00338.html
# http://sigquit.wordpress.com/2011/02/16/why-asneeded-doesnt-work-as-expected-for-your-libraries-on-your-autotools-project/
AM_LDFLAGS = -Wl,--as-needed

if USE_BOOST_SPIRIT_OLD_HDR
AM_CXXFLAGS += -DUSE_BOOST_SPIRIT_OLD_HDR
endif

if WITH_LIBATOMIC
AM_LDFLAGS += -latomic_ops
endif

if ENABLE_COVERAGE
AM_CFLAGS += -fprofile-arcs -ftest-coverage
AM_CXXFLAGS += -fprofile-arcs -ftest-coverage -O0
EXTRALIBS += -lgcov
endif

# extra bits
EXTRA_DIST += \
	$(srcdir)/verify-mds-journal.sh $(srcdir)/vstart.sh $(srcdir)/stop.sh \
	ceph-run $(srcdir)/ceph_common.sh \
	$(srcdir)/init-radosgw \
	$(srcdir)/init-radosgw.sysv \
	$(srcdir)/ceph-clsinfo $(srcdir)/make_version $(srcdir)/check_version \
	$(srcdir)/.git_version \
	$(srcdir)/ceph-rbdnamer \
	$(ceph_tool_gui_DATA) \
	$(srcdir)/test/encoding/readable.sh \
	$(srcdir)/test/encoding/check-generated.sh \
	$(srcdir)/upstart/ceph-all.conf \
	$(srcdir)/upstart/ceph-mon.conf \
	$(srcdir)/upstart/ceph-mon-all.conf \
	$(srcdir)/upstart/ceph-mon-all-starter.conf \
	$(srcdir)/upstart/ceph-create-keys.conf \
	$(srcdir)/upstart/ceph-osd.conf \
	$(srcdir)/upstart/ceph-osd-all.conf \
	$(srcdir)/upstart/ceph-osd-all-starter.conf \
	$(srcdir)/upstart/ceph-mds.conf \
	$(srcdir)/upstart/ceph-mds-all.conf \
	$(srcdir)/upstart/ceph-mds-all-starter.conf \
	$(srcdir)/upstart/radosgw.conf \
	$(srcdir)/upstart/radosgw-all.conf \
	$(srcdir)/upstart/radosgw-all-starter.conf \
	ceph-disk \
	ceph-disk-prepare \
	ceph-disk-activate \
	ceph-disk-udev \
	ceph-create-keys \
	mount.fuse.ceph


EXTRA_DIST += $(srcdir)/$(shell_scripts:%=%.in)

# work around old versions of automake that don't define $docdir
# NOTE: this won't work on suse, where docdir is /usr/share/doc/packages/$package.
docdir ?= ${datadir}/doc/ceph

doc_DATA = $(srcdir)/sample.ceph.conf sample.fetch_config

sample.fetch_config: fetch_config
	cp -f $(srcdir)/fetch_config ./sample.fetch_config

shell_commondir = $(libdir)/ceph
shell_common_SCRIPTS = ceph_common.sh

bash_completiondir = $(sysconfdir)/bash_completion.d
bash_completion_DATA = $(srcdir)/bash_completion/ceph \
               $(srcdir)/bash_completion/rados \
               $(srcdir)/bash_completion/rbd \
               $(srcdir)/bash_completion/radosgw-admin

libcephfs_includedir = $(includedir)/cephfs
libcephfs_include_DATA = $(srcdir)/include/cephfs/libcephfs.h

librbd_includedir = $(includedir)/rbd
librbd_include_DATA = \
	$(srcdir)/include/rbd/features.h \
	$(srcdir)/include/rbd/librbd.h \
	$(srcdir)/include/rbd/librbd.hpp

rados_includedir = $(includedir)/rados
rados_include_DATA = \
	$(srcdir)/include/rados/librados.h \
	$(srcdir)/include/rados/rados_types.h \
	$(srcdir)/include/rados/rados_types.hpp \
	$(srcdir)/include/rados/librados.hpp \
	$(srcdir)/include/buffer.h \
	$(srcdir)/include/page.h \
	$(srcdir)/include/crc32c.h

#crush_includedir = $(includedir)/crush
#crush_include_DATA = \
#	$(srcdir)/crush/hash.h \
#	$(srcdir)/crush/crush.h \
#	$(srcdir)/crush/mapper.h \
#	$(srcdir)/crush/types.h

FORCE:
.git_version: FORCE
	$(srcdir)/check_version $(srcdir)/.git_version
ceph_ver.h: .git_version
	$(srcdir)/make_version $(srcdir)/.git_version ./ceph_ver.h

ceph_ver.c: ./ceph_ver.h
common/version.cc: ./ceph_ver.h
test/encoding/ceph_dencoder.cc: ./ceph_ver.h

# cleaning
clean-local:
	-rm *.so *.gcno *.gcda

# libs

libcommon_la_SOURCES = $(libcommon_files)
libcommon_la_CFLAGS= ${CRYPTO_CFLAGS} ${AM_CFLAGS}
libcommon_la_CXXFLAGS= ${AM_CXXFLAGS}
libcommon_la_LDFLAGS = -lrt
noinst_LTLIBRARIES += libcommon.la

libglobal_la_SOURCES = \
	global/global_context.cc \
	global/global_init.cc \
	global/pidfile.cc \
	global/signal_handler.cc
libglobal_la_CFLAGS= ${CRYPTO_CFLAGS} ${AM_CFLAGS}
libglobal_la_CXXFLAGS= ${AM_CXXFLAGS}
libglobal_la_LIBADD= libcommon.la
noinst_LTLIBRARIES += libglobal.la

crush_files = \
	crush/builder.c \
	crush/mapper.c \
	crush/crush.c \
	crush/hash.c \
	crush/CrushWrapper.cc \
	crush/CrushCompiler.cc \
	crush/CrushTester.cc

# this list ommits the ceph_ver.c file
libcommon_files = \
	./ceph_ver.c \
	auth/AuthAuthorizeHandler.cc \
	auth/AuthClientHandler.cc \
	auth/AuthSessionHandler.cc \
	auth/AuthMethodList.cc \
	auth/cephx/CephxAuthorizeHandler.cc \
	auth/cephx/CephxClientHandler.cc \
	auth/cephx/CephxProtocol.cc \
	auth/cephx/CephxSessionHandler.cc \
	auth/none/AuthNoneAuthorizeHandler.cc \
	auth/unknown/AuthUnknownAuthorizeHandler.cc \
	auth/Crypto.cc \
	auth/KeyRing.cc \
	auth/RotatingKeyRing.cc \
	common/DecayCounter.cc \
	common/LogClient.cc \
	common/LogEntry.cc \
	common/PrebufferedStreambuf.cc \
	common/BackTrace.cc \
	common/perf_counters.cc \
	common/Mutex.cc \
	common/OutputDataSocket.cc \
	common/admin_socket.cc \
	common/admin_socket_client.cc \
	common/escape.c \
	common/Clock.cc \
	common/Throttle.cc \
	common/Timer.cc \
	common/Finisher.cc \
	common/environment.cc\
	common/sctp_crc32.c\
	common/assert.cc \
        common/run_cmd.cc \
	common/WorkQueue.cc \
	common/ConfUtils.cc \
	common/MemoryModel.cc \
	common/armor.c \
	common/fd.cc \
	common/xattr.c \
	common/safe_io.c \
	common/snap_types.cc \
	common/str_list.cc \
	common/errno.cc \
	json_spirit/json_spirit_reader.cpp \
	json_spirit/json_spirit_writer.cpp \
	json_spirit/json_spirit_value.cpp \
	log/Log.cc \
	log/SubsystemMap.cc \
	mon/MonCaps.cc \
	mon/MonClient.cc \
	mon/MonMap.cc \
	msg/Accepter.cc \
	msg/DispatchQueue.cc \
	msg/Message.cc \
	common/RefCountedObj.cc \
	msg/Messenger.cc \
	msg/Pipe.cc \
	msg/SimpleMessenger.cc \
	msg/msg_types.cc \
	os/hobject.cc \
	osd/OSDMap.cc \
	osd/osd_types.cc \
	mds/MDSMap.cc \
	mds/inode_backtrace.cc \
	mds/mdstypes.cc \
	common/blkdev.cc \
	common/common_init.cc \
	common/pipe.c \
	common/ceph_argparse.cc \
	common/ceph_context.cc \
	common/buffer.cc \
	common/code_environment.cc \
	common/dout.cc \
	common/signal.cc \
	common/simple_spin.cc \
	common/Thread.cc \
	common/Formatter.cc \
	common/HeartbeatMap.cc \
	include/ceph_fs.cc \
	include/ceph_hash.cc \
	include/ceph_strings.cc \
	include/ceph_frag.cc \
	common/config.cc \
	common/utf8.c \
	common/mime.c \
	common/strtol.cc \
	common/page.cc \
	common/lockdep.cc \
	common/version.cc \
	common/hex.cc \
	common/entity_name.cc \
	common/ceph_crypto.cc \
	common/ceph_crypto_cms.cc \
	common/ceph_json.cc \
	common/ipaddr.cc \
	common/pick_address.cc \
	include/addr_parsing.c \
	$(crush_files)

if WITH_PROFILER
libcommon_files += perfglue/cpu_profiler.cc
else
libcommon_files += perfglue/disabled_stubs.cc
endif



libmon_a_SOURCES = \
	auth/cephx/CephxKeyServer.cc \
	auth/cephx/CephxServiceHandler.cc \
	auth/cephx/CephxSessionHandler.cc \
	auth/AuthServiceHandler.cc \
	auth/AuthSessionHandler.cc \
	mon/Monitor.cc \
	mon/Paxos.cc \
	mon/PaxosService.cc \
	mon/OSDMonitor.cc \
	mon/MDSMonitor.cc \
	mon/MonmapMonitor.cc \
	mon/PGMonitor.cc \
	mon/PGMap.cc \
	mon/LogMonitor.cc \
	mon/AuthMonitor.cc \
	mon/Elector.cc \
	mon/MonitorStore.cc \
	os/LevelDBStore.cc \
	mon/HealthMonitor.cc \
	mon/DataHealthService.cc \
	mon/ConfigKeyService.cc \
	common/util.cc
libmon_a_CXXFLAGS= ${AM_CXXFLAGS}
noinst_LIBRARIES += libmon.a

libmds_a_SOURCES = \
	mds/Anchor.cc \
	mds/Capability.cc \
	mds/Dumper.cc \
	mds/Resetter.cc \
	mds/MDS.cc \
	mds/flock.cc \
	mds/locks.c \
	mds/journal.cc \
	mds/Server.cc \
	mds/Mutation.cc \
	mds/MDCache.cc \
	mds/Locker.cc \
	mds/Migrator.cc \
	mds/MDBalancer.cc \
	mds/CDentry.cc \
	mds/CDir.cc \
	mds/CInode.cc \
	mds/LogEvent.cc \
	mds/MDSTable.cc \
	mds/InoTable.cc \
	mds/MDSTableClient.cc \
	mds/MDSTableServer.cc \
	mds/AnchorServer.cc \
	mds/AnchorClient.cc \
	mds/SnapRealm.cc \
	mds/SnapServer.cc \
	mds/snap.cc \
	mds/SessionMap.cc \
	mds/MDLog.cc \
	osdc/Journaler.cc
noinst_LIBRARIES += libmds.a

libos_a_SOURCES = \
	os/FileJournal.cc \
	os/FileStore.cc \
	os/chain_xattr.cc \
	os/ObjectStore.cc \
	os/JournalingObjectStore.cc \
	os/LFNIndex.cc \
	os/HashIndex.cc \
	os/IndexManager.cc \
	os/FlatIndex.cc \
	os/DBObjectMap.cc \
	os/LevelDBStore.cc
libos_a_CXXFLAGS= ${AM_CXXFLAGS}
noinst_LIBRARIES += libos.a

libosd_a_SOURCES = \
	osd/PG.cc \
	osd/ReplicatedPG.cc \
	osd/Ager.cc \
	osd/OSD.cc \
	osd/OSDCap.cc \
	osd/Watch.cc \
	osd/ClassHandler.cc \
	osd/OpRequest.cc \
	osd/SnapMapper.cc
libosd_a_CXXFLAGS= ${AM_CXXFLAGS}
noinst_LIBRARIES += libosd.a

libosdc_la_SOURCES = \
	osdc/Objecter.cc \
	osdc/ObjectCacher.cc \
	osdc/Filer.cc \
	osdc/Striper.cc
libosdc_la_CXXFLAGS= ${AM_CXXFLAGS}
libosdc_la_LIBADD = libcommon.la
noinst_LTLIBRARIES += libosdc.la

libclient_la_SOURCES = \
	client/Client.cc \
	client/Inode.cc \
	client/Dentry.cc \
	client/MetaRequest.cc \
	client/ClientSnapRealm.cc \
	client/MetaSession.cc \
	client/Trace.cc
libclient_la_LIBADD = libosdc.la $(LIBEDIT_LIBS)
noinst_LTLIBRARIES += libclient.la

dist-hook:
	$(srcdir)/check_version $(srcdir)/.git_version

python_PYTHON = pybind/rados.py \
		pybind/rbd.py \
		pybind/cephfs.py

# headers... and everything else we want to include in a 'make dist' 
# that autotools doesn't magically identify.
noinst_HEADERS = \
	rados_sync.h \
	auth/cephx/CephxAuthorizeHandler.h\
	auth/cephx/CephxKeyServer.h\
	auth/cephx/CephxProtocol.h\
	auth/cephx/CephxClientHandler.h\
	auth/cephx/CephxServiceHandler.h\
	auth/cephx/CephxSessionHandler.h\
	auth/none/AuthNoneAuthorizeHandler.h\
	auth/none/AuthNoneClientHandler.h\
	auth/none/AuthNoneServiceHandler.h\
	auth/none/AuthNoneSessionHandler.h\
	auth/none/AuthNoneProtocol.h\
	auth/unknown/AuthUnknownAuthorizeHandler.h\
	auth/unknown/AuthUnknownClientHandler.h\
	auth/unknown/AuthUnknownServiceHandler.h\
	auth/unknown/AuthUnknownSessionHandler.h\
	auth/unknown/AuthUnknownProtocol.h\
	auth/Auth.h\
	auth/AuthMethodList.h\
	auth/AuthClientHandler.h\
	auth/AuthServiceHandler.h\
	auth/AuthSessionHandler.h\
	auth/AuthAuthorizeHandler.h\
	auth/KeyRing.h\
	auth/RotatingKeyRing.h\
	auth/Crypto.h\
	bash_completion/ceph\
	bash_completion/rados\
	bash_completion/rbd\
	bash_completion/radosgw-admin\
        client/Client.h\
	client/Dentry.h\
	client/Dir.h\
	client/Fh.h\
	client/Inode.h\
	client/MetaRequest.h\
	client/MetaSession.h\
	client/ClientSnapRealm.h\
        client/SyntheticClient.h\
        client/Trace.h\
        client/fuse_ll.h\
	client/ioctl.h\
        client/hadoop/CephFSInterface.h\
	client/ObjecterWriteback.h\
	cls_acl.cc\
	cls_crypto.cc\
	cls/lock/cls_lock_types.h\
	cls/lock/cls_lock_ops.h\
	cls/lock/cls_lock_client.h\
	cls/rbd/cls_rbd.h\
	cls/rbd/cls_rbd_client.h\
	cls/refcount/cls_refcount_ops.h\
	cls/refcount/cls_refcount_client.h\
	cls/version/cls_version_types.h\
	cls/version/cls_version_ops.h\
	cls/version/cls_version_client.h\
	cls/log/cls_log_types.h\
	cls/log/cls_log_ops.h\
	cls/log/cls_log_client.h\
	cls/statelog/cls_statelog_types.h\
	cls/statelog/cls_statelog_ops.h\
	cls/statelog/cls_statelog_client.h\
	cls/replica_log/cls_replica_log_types.h\
	cls/replica_log/cls_replica_log_ops.h\
	cls/replica_log/cls_replica_log_client.h\
	cls/rgw/cls_rgw_client.h\
	cls/rgw/cls_rgw_ops.h\
	cls/rgw/cls_rgw_types.h\
	common/BackTrace.h\
	common/RefCountedObj.h\
	common/HeartbeatMap.h\
	common/LogClient.h\
	common/LogEntry.h\
	common/Preforker.h\
	common/WorkQueue.h\
	common/PrioritizedQueue.h\
	common/ceph_argparse.h\
	common/ceph_context.h\
	common/xattr.h\
	common/blkdev.h\
	common/compiler_extensions.h\
	common/debug.h\
	common/dout.h\
	common/escape.h\
	common/fd.h\
	common/version.h\
	common/hex.h\
	common/entity_name.h\
	common/errno.h\
	common/environment.h\
	common/likely.h\
	common/lockdep.h\
	common/obj_bencher.h\
	common/snap_types.h\
        common/Clock.h\
        common/Cond.h\
        common/ConfUtils.h\
        common/DecayCounter.h\
        common/Finisher.h\
	common/Formatter.h\
        common/perf_counters.h\
	common/OutputDataSocket.h \
	common/admin_socket.h \
	common/admin_socket_client.h \
	common/shared_cache.hpp \
	common/tracked_int_ptr.hpp \
	common/simple_cache.hpp \
	common/sharedptr_registry.hpp \
	common/map_cacher.hpp \
        common/MemoryModel.h\
        common/Mutex.h\
	common/PrebufferedStreambuf.h\
        common/RWLock.h\
        common/Semaphore.h\
	common/SimpleRNG.h\
	common/TextTable.h\
        common/Thread.h\
        common/Throttle.h\
        common/Timer.h\
	common/TrackedOp.h\
        common/arch.h\
        common/armor.h\
	global/global_init.h \
	global/global_context.h \
        common/common_init.h\
        common/pipe.h\
	common/code_environment.h \
        common/signal.h\
        global/signal_handler.h\
        common/simple_spin.h\
        common/run_cmd.h\
	common/safe_io.h\
        common/config.h\
        common/config_obs.h\
	common/config_opts.h\
	common/ceph_crypto.h\
	common/ceph_crypto_cms.h\
	common/ceph_json.h\
	common/lru_map.h\
	common/utf8.h\
	common/mime.h\
	common/pick_address.h\
	common/secret.h\
	common/strtol.h\
	common/static_assert.h\
	common/AsyncReserver.h\
	crush/CrushCompiler.h\
	crush/CrushTester.h\
        crush/CrushWrapper.h\
        crush/CrushWrapper.i\
        crush/builder.h\
        crush/crush.h\
        crush/grammar.h\
        crush/hash.h\
        crush/mapper.h\
        crush/sample.txt\
        crush/types.h\
	fetch_config\
	include/bloom_filter.hpp\
        include/Context.h\
	include/CompatSet.h\
        include/Distribution.h\
	include/addr_parsing.h\
	include/assert.h\
        include/atomic.h\
        include/bitmapper.h\
        include/blobhash.h\
        include/buffer.h\
        include/byteorder.h\
	include/cephfs/libcephfs.h\
	include/ceph_features.h\
	include/ceph_frag.h\
        include/ceph_fs.h\
        include/ceph_hash.h\
	include/cmp.h\
	include/color.h\
	include/compat.h\
	include/crc32c.h\
        include/encoding.h\
        include/err.h\
        include/error.h\
        include/fiemap.h\
        include/filepath.h\
        include/frag.h\
        include/hash.h\
        include/intarith.h\
        include/interval_set.h\
        include/inttypes.h\
	include/ipaddr.h\
	include/linux_fiemap.h\
        include/lru.h\
	include/msgr.h\
        include/object.h\
        include/page.h\
        include/rangeset.h\
	include/rados.h\
	include/rbd_types.h\
        include/statlite.h\
	include/str_list.h\
	include/stringify.h\
        include/triple.h\
        include/types.h\
        include/utime.h\
        include/dlist.h\
        include/elist.h\
	include/uuid.h\
        include/xlist.h\
	include/rados/librados.h\
	include/rados/rados_types.h\
	include/rados/rados_types.hpp\
	include/rados/librados.hpp\
	include/rados/librgw.h\
	include/rados/page.h\
	include/rados/crc32c.h\
	include/rados/buffer.h\
	include/rbd/features.h\
	include/rbd/librbd.h\
	include/rbd/librbd.hpp\
        include/util.h\
	librados/snap_set_diff.h\
	librados/AioCompletionImpl.h\
	librados/IoCtxImpl.h\
	librados/PoolAsyncCompletionImpl.h\
	librados/RadosClient.h\
	librbd/AioCompletion.h\
	librbd/AioRequest.h\
	librbd/ImageCtx.h\
	librbd/internal.h\
	librbd/LibrbdWriteback.h\
	librbd/parent_types.h\
	librbd/SnapInfo.h\
	librbd/WatchCtx.h\
	logrotate.conf\
	json_spirit/json_spirit.h\
	json_spirit/json_spirit_error_position.h\
	json_spirit/json_spirit_reader.h\
	json_spirit/json_spirit_reader_template.h\
	json_spirit/json_spirit_stream_reader.h\
	json_spirit/json_spirit_utils.h\
	json_spirit/json_spirit_value.h\
	json_spirit/json_spirit_writer.h\
	json_spirit/json_spirit_writer_options.h\
	json_spirit/json_spirit_writer_template.h\
	key_value_store/key_value_structure.h\
	key_value_store/kv_flat_btree_async.h\
	key_value_store/kvs_arg_types.h\
	log/Entry.h\
	log/EntryQueue.h\
	log/Log.h\
	log/SubsystemMap.h\
	mds/inode_backtrace.h\
	mds/flock.h\
	mds/locks.c\
	mds/locks.h\
        mds/Anchor.h\
        mds/AnchorClient.h\
        mds/AnchorServer.h\
        mds/CDentry.h\
        mds/CDir.h\
        mds/CInode.h\
        mds/Capability.h\
	mds/Dumper.h\
        mds/InoTable.h\
        mds/LocalLock.h\
        mds/Locker.h\
        mds/LogEvent.h\
        mds/LogSegment.h\
        mds/MDBalancer.h\
        mds/MDCache.h\
        mds/MDLog.h\
        mds/MDS.h\
        mds/MDSMap.h\
	mds/MDSTable.h\
	mds/MDSTableServer.h\
	mds/MDSTableClient.h\
	mds/Mutation.h\
        mds/Migrator.h\
	mds/Resetter.h\
        mds/ScatterLock.h\
        mds/Server.h\
        mds/SessionMap.h\
        mds/SimpleLock.h\
        mds/SnapClient.h\
	mds/SnapRealm.h\
        mds/SnapServer.h\
        mds/events/ECommitted.h\
        mds/events/EExport.h\
        mds/events/EFragment.h\
        mds/events/EImportFinish.h\
        mds/events/EImportStart.h\
        mds/events/EMetaBlob.h\
        mds/events/EOpen.h\
	mds/events/EResetJournal.h\
        mds/events/ESession.h\
        mds/events/ESessions.h\
        mds/events/ESlaveUpdate.h\
        mds/events/ESubtreeMap.h\
	mds/events/ETableClient.h\
	mds/events/ETableServer.h\
        mds/events/EUpdate.h\
        mds/mds_table_types.h\
        mds/mdstypes.h\
        mds/snap.h\
        messages/MAuth.h\
        messages/MAuthReply.h\
	messages/MCacheExpire.h\
        messages/MClientCaps.h\
        messages/MClientCapRelease.h\
        messages/MClientLease.h\
        messages/MClientReconnect.h\
        messages/MClientReply.h\
        messages/MClientRequest.h\
        messages/MClientRequestForward.h\
        messages/MClientSession.h\
        messages/MClientSnap.h\
        messages/MCommand.h\
        messages/MCommandReply.h\
        messages/MDentryLink.h\
        messages/MDentryUnlink.h\
        messages/MDirUpdate.h\
        messages/MDiscover.h\
        messages/MDiscoverReply.h\
        messages/MExportCaps.h\
        messages/MExportCapsAck.h\
        messages/MExportDir.h\
        messages/MExportDirAck.h\
        messages/MExportDirCancel.h\
        messages/MExportDirDiscover.h\
        messages/MExportDirDiscoverAck.h\
        messages/MExportDirFinish.h\
        messages/MExportDirNotify.h\
        messages/MExportDirNotifyAck.h\
        messages/MExportDirPrep.h\
        messages/MExportDirPrepAck.h\
        messages/MGenericMessage.h\
        messages/MGetPoolStats.h\
        messages/MGetPoolStatsReply.h\
        messages/MHeartbeat.h\
        messages/MInodeFileCaps.h\
        messages/MLock.h\
	messages/MLog.h\
	messages/MLogAck.h\
        messages/MMDSBeacon.h\
        messages/MMDSCacheRejoin.h\
	messages/MMDSLoadTargets.h\
	messages/MMDSFindIno.h\
	messages/MMDSFindInoReply.h\
        messages/MMDSFragmentNotify.h\
        messages/MMDSMap.h\
        messages/MMDSResolve.h\
        messages/MMDSResolveAck.h\
        messages/MMDSSlaveRequest.h\
        messages/MMDSTableRequest.h\
        messages/MMonCommand.h\
        messages/MMonCommandAck.h\
        messages/MMonElection.h\
        messages/MMonGetMap.h\
        messages/MMonGetVersion.h\
        messages/MMonGetVersionReply.h\
	messages/MMonGlobalID.h\
	messages/MMonHealth.h\
	messages/MMonJoin.h\
        messages/MMonMap.h\
        messages/MMonPaxos.h\
        messages/MMonProbe.h\
        messages/MMonSubscribe.h\
        messages/MMonSubscribeAck.h\
        messages/MMonSync.h \
        messages/MOSDAlive.h\
        messages/MOSDBoot.h\
        messages/MOSDFailure.h\
        messages/MOSDMarkMeDown.h\
        messages/MOSDMap.h\
        messages/MOSDOp.h\
        messages/MOSDOpReply.h\
	messages/MOSDPGBackfill.h\
        messages/MOSDPGCreate.h\
        messages/MOSDPGInfo.h\
        messages/MOSDPGLog.h\
        messages/MOSDPGMissing.h\
        messages/MOSDPGNotify.h\
        messages/MOSDPGQuery.h\
        messages/MOSDPGRemove.h\
	messages/MOSDPGScan.h\
        messages/MBackfillReserve.h\
        messages/MRecoveryReserve.h\
	messages/MMonQuorumService.h\
        messages/MOSDPGTemp.h\
        messages/MOSDPGTrim.h\
        messages/MOSDPing.h\
	messages/MOSDRepScrub.h\
	messages/MOSDScrub.h\
        messages/MOSDSubOp.h\
        messages/MOSDSubOpReply.h\
        messages/MPGStats.h\
        messages/MPGStatsAck.h\
        messages/MPing.h\
	messages/MPoolOp.h\
	messages/MPoolOpReply.h\
        messages/MRemoveSnaps.h\
	messages/MRoute.h\
	messages/MForward.h\
        messages/MStatfs.h\
        messages/MStatfsReply.h\
	messages/MTimeCheck.h\
        messages/MWatchNotify.h\
	messages/PaxosServiceMessage.h\
	mon/AuthMonitor.h\
        mon/DataHealthService.h\
        mon/Elector.h\
	mon/LogMonitor.h\
	mon/ConfigKeyService.h\
        mon/HealthMonitor.h\
        mon/HealthService.h\
        mon/MDSMonitor.h\
	mon/MonmapMonitor.h\
        mon/MonCaps.h\
        mon/MonClient.h\
        mon/MonMap.h\
        mon/Monitor.h\
	mon/MonitorStore.h\
        mon/MonitorDBStore.h\
        mon/OSDMonitor.h\
        mon/PGMap.h\
        mon/PGMonitor.h\
        mon/Paxos.h\
        mon/PaxosService.h\
	mon/QuorumService.h\
        mon/Session.h\
        mon/mon_types.h\
	mount/canonicalize.c\
	mount/mtab.c\
	msg/Accepter.h\
	msg/DispatchQueue.h\
        msg/Dispatcher.h\
        msg/Message.h\
        msg/Messenger.h\
	msg/Pipe.h\
        msg/SimpleMessenger.h\
        msg/msg_types.h\
	objclass/objclass.h\
	os/btrfs_ioctl.h\
	os/chain_xattr.h\
	os/hobject.h \
	os/CollectionIndex.h\
        os/FileJournal.h\
        os/FileStore.h\
	os/FlatIndex.h\
	os/HashIndex.h\
	os/IndexManager.h\
        os/Journal.h\
        os/JournalingObjectStore.h\
	os/LFNIndex.h\
        os/ObjectStore.h\
	os/SequencerPosition.h\
        osd/Ager.h\
	osd/ClassHandler.h\
        osd/OSD.h\
        osd/OSDCap.h\
        osd/OSDMap.h\
        osd/ObjectVersioner.h\
	osd/OpRequest.h\
	osd/SnapMapper.h\
        osd/PG.h\
        osd/ReplicatedPG.h\
        osd/Watch.h\
        osd/osd_types.h\
        osdc/Blinker.h\
        osdc/Filer.h\
        osdc/Journaler.h\
        osdc/ObjectCacher.h\
        osdc/Objecter.h\
	osdc/Striper.h\
	osdc/WritebackHandler.h\
        perfglue/cpu_profiler.h\
        perfglue/heap_profiler.h\
	rgw/logrotate.conf\
	rgw/rgw_acl.h\
	rgw/rgw_acl_s3.h\
	rgw/rgw_acl_swift.h\
	rgw/rgw_client_io.h\
	rgw/rgw_fcgi.h\
	rgw/rgw_xml.h\
	rgw/rgw_cache.h\
	rgw/rgw_common.h\
	rgw/rgw_cors.h\
	rgw/rgw_cors_s3.h\
	rgw/rgw_cors_swift.h\
	rgw/rgw_string.h\
	rgw/rgw_formats.h\
	rgw/rgw_http_errors.h\
	rgw/rgw_log.h\
	rgw/rgw_multi.h\
	rgw/rgw_policy_s3.h\
	rgw/rgw_gc.h\
	rgw/rgw_metadata.h\
	rgw/rgw_multi_del.h\
	rgw/rgw_op.h\
	rgw/rgw_http_client.h\
	rgw/rgw_swift.h\
	rgw/rgw_swift_auth.h\
	rgw/rgw_rados.h\
	rgw/rgw_replica_log.h \
	rgw/rgw_resolve.h\
	rgw/rgw_rest.h\
	rgw/rgw_rest_swift.h\
	rgw/rgw_rest_s3.h\
	rgw/rgw_auth_s3.h\
	rgw/rgw_rest_admin.h\
	rgw/rgw_rest_usage.h\
	rgw/rgw_rest_user.h\
	rgw/rgw_rest_bucket.h\
	rgw/rgw_rest_client.h\
	rgw/rgw_rest_conn.h\
	rgw/rgw_tools.h\
	rgw/rgw_rest_metadata.h\
	rgw/rgw_rest_log.h\
	rgw/rgw_rest_opstate.h\
	rgw/rgw_rest_replica_log.h\
	rgw/rgw_rest_config.h\
	rgw/rgw_usage.h\
	rgw/rgw_user.h\
	rgw/rgw_bucket.h\
        sample.ceph.conf\
	tools/common.h\
	test/osd/RadosModel.h\
	test/osd/Object.h\
	test/osd/TestOpStat.h\
	global/pidfile.h\
	common/sync_filesystem.h \
        test/bench/distribution.h \
        test/bench/rados_backend.h \
        test/bench/rbd_backend.h \
        test/bench/bencher.h \
        test/bench/backend.h \
        test/bench/dumb_backend.h \
        test/bench/stat_collector.h \
        test/bench/detailed_stat_collector.h \
        test/bench/filestore_backend.h \
        test/common/ObjectContents.h \
        test/encoding/types.h \
        test/filestore/DeterministicOpSequence.h \
        test/filestore/FileStoreTracker.h \
        test/filestore/FileStoreDiff.h \
        test/filestore/TestFileStoreState.h \
        test/filestore/workload_generator.h \
        test/kv_store_bench.h \
        test/librados/test.h \
        test/ObjectMap/KeyValueDBMemory.h \
        test/omap_bench.h \
        test/osd/Object.h \
        test/osd/RadosModel.h \
        test/osd/TestOpStat.h \
        test/osdc/FakeWriteback.h \
        test/system/cross_process_sem.h \
        test/system/st_rados_create_pool.h \
        test/system/st_rados_list_objects.h \
        test/system/st_rados_delete_objs.h \
        test/system/st_rados_delete_pool.h \
        test/system/st_rados_notify.h \
        test/system/st_rados_watch.h \
        test/system/systest_runnable.h \
        test/system/systest_settings.h \
        test/unit.h \
	os/ObjectMap.h \
	os/DBObjectMap.h \
	os/KeyValueDB.h \
	os/LevelDBStore.h

if ENABLE_COVERAGE
COV_DIR = $(DESTDIR)$(libdir)/ceph/coverage
COV_FILES = $(srcdir)/*.gcno
COV_LIB_FILES = $(srcdir)/.libs/*.gcno
endif

install-coverage:
if ENABLE_COVERAGE
	-mkdir -p $(COV_DIR)/.libs
	-$(INSTALL_DATA) $(COV_FILES) $(COV_DIR)
	-$(INSTALL_DATA) $(COV_LIB_FILES) $(COV_DIR)/.libs
endif

uninstall-coverage:
if ENABLE_COVERAGE
	-rm $(COV_DIR)/*.gcno
	-rm $(COV_DIR)/.libs/*.gcno
	-rmdir -p $(COV_DIR)/.libs
	-rmdir -p $(COV_DIR)
endif

check-coverage:
if ENABLE_COVERAGE
	-test/coverage.sh -d $(srcdir) -o check-coverage make check
endif

install-data-local: install-coverage
	-mkdir -p $(DESTDIR)$(sysconfdir)/ceph
	-mkdir -p $(DESTDIR)$(localstatedir)/log/ceph
	-mkdir -p $(DESTDIR)$(localstatedir)/lib/ceph/tmp

uninstall-local: uninstall-coverage
	-rmdir -p $(DESTDIR)$(sysconfdir)/ceph/
	-rmdir -p $(DESTDIR)$(localstatedir)/log/ceph
	-rmdir -p $(DESTDIR)$(localstatedir)/lib/ceph/tmp

# if we are doing a debug build, tell make to actually build the debug
# targets
if WITH_DEBUG
bin_PROGRAMS += $(bin_DEBUGPROGRAMS)
endif


#
# coverity rules expect:
#  - cov-build to be in the path
#  - password in ~/coverity.build.pass.txt
#  - ability to scp into the ceph.com directory
#
project.tgz: clean
	rm -rf cov-int
	cov-build --dir cov-int make
	echo Sage Weil sage@newdream.net ceph >> README
	tar czvf project.tgz README cov-int
	rm -f README

coverity-submit:
	scp project.tgz ceph.com:/home/ceph_site/ceph.com/coverity/`git describe`.tgz
	curl --data "project=ceph&password=`cat ~/coverity.build.pass.txt`&email=sage@newdream.net&url=http://ceph.com/coverity/`git describe`.tgz" http://scan5.coverity.com/cgi-bin/submit_build.py
