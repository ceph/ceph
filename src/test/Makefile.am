include test/erasure-code/Makefile.am

## Unknown/other tests

ceph_test_timers_SOURCES = test/TestTimers.cc
ceph_test_timers_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_timers

ceph_test_signal_handlers_SOURCES = test/TestSignalHandlers.cc
ceph_test_signal_handlers_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_signal_handlers

ceph_test_rados_SOURCES = \
	test/osd/TestRados.cc \
	test/osd/TestOpStat.cc \
	test/osd/Object.cc \
	test/osd/RadosModel.cc
ceph_test_rados_LDADD = $(LIBRADOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_rados

ceph_test_mutate_SOURCES = test/test_mutate.cc
ceph_test_mutate_LDADD = $(LIBRADOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_mutate

ceph_test_rewrite_latency_SOURCES = test/test_rewrite_latency.cc
ceph_test_rewrite_latency_LDADD = $(LIBCOMMON) $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_DEBUGPROGRAMS += ceph_test_rewrite_latency

ceph_test_msgr_SOURCES = test/testmsgr.cc
ceph_test_msgr_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_msgr

ceph_test_async_driver_SOURCES = test/msgr/test_async_driver.cc
ceph_test_async_driver_LDADD = $(LIBOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
ceph_test_async_driver_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_async_driver

ceph_streamtest_SOURCES = test/streamtest.cc
ceph_streamtest_LDADD = $(LIBOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_streamtest

ceph_test_trans_SOURCES = test/test_trans.cc
ceph_test_trans_LDADD = $(LIBOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_trans

ceph_test_crypto_SOURCES = test/testcrypto.cc
ceph_test_crypto_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_crypto

ceph_test_keys_SOURCES = test/testkeys.cc
ceph_test_keys_LDADD = $(LIBMON) $(CEPH_GLOBAL) 
bin_DEBUGPROGRAMS += ceph_test_keys


## Dencoder test

ceph_dencoder_SOURCES = \
	test/encoding/ceph_dencoder.cc \
	$(DENCODER_SOURCES)
ceph_dencoder_LDADD = \
	$(LIBOSD_TYPES) \
	$(LIBOS_TYPES) \
	$(LIBMDS) \
	$(LIBMON_TYPES) \
	$(DENCODER_DEPS) \
	$(CEPH_GLOBAL)

# These should always use explicit _CFLAGS/_CXXFLAGS so avoid basename conflicts
ceph_dencoder_CFLAGS = ${AM_CFLAGS}
ceph_dencoder_CXXFLAGS = ${AM_CXXFLAGS}

if COMPILER_HAS_VTA
ceph_dencoder_CFLAGS += -fno-var-tracking-assignments
ceph_dencoder_CXXFLAGS += -fno-var-tracking-assignments
endif

bin_PROGRAMS += ceph-dencoder

get_command_descriptions_SOURCES = test/common/get_command_descriptions.cc
get_command_descriptions_LDADD = $(LIBMON) $(LIBCOMMON) $(CEPH_GLOBAL)
noinst_PROGRAMS += get_command_descriptions


## Build tests
# These should all use explicit _CXXFLAGS so avoid basename conflicts

if WITH_BUILD_TESTS
test_build_libcommon_SOURCES = \
	test/buildtest_skeleton.cc \
	$(libcommon_la_SOURCES)
test_build_libcommon_LDADD = \
	$(LIBCOMMON_DEPS) \
	$(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS)
test_build_libcommon_LDFLAGS = -static-libtool-libs
test_build_libcommon_CFLAGS = $(AM_CFLAGS)
test_build_libcommon_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += test_build_libcommon

test_build_librados_SOURCES = \
	test/buildtest_skeleton.cc \
	$(librados_la_SOURCES)
test_build_librados_LDADD = \
	$(LIBRADOS_DEPS) \
	$(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS)
test_build_librados_LDFLAGS = -static-libtool-libs
test_build_librados_CFLAGS = $(AM_CFLAGS)
test_build_librados_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += test_build_librados

test_build_librgw_SOURCES = \
	test/buildtest_skeleton.cc \
	$(librgw_la_SOURCES)
test_build_librgw_LDADD = \
	$(LIBRGW_DEPS) \
	$(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS) \
	$(CEPH_GLOBAL)
test_build_librgw_LDFLAGS = -static-libtool-libs
test_build_librgw_CFLAGS = $(AM_CFLAGS)
test_build_librgw_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += test_build_librgw

# I dont get this one... testing the osdc build but link in libcephfs?
test_build_libcephfs_SOURCES = \
	test/buildtest_skeleton.cc \
	$(libosdc_la_SOURCES)
test_build_libcephfs_LDADD = \
	$(LIBCEPHFS) -lexpat \
	$(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS)
test_build_libcephfs_LDFLAGS = -static-libtool-libs
test_build_libcephfs_CFLAGS = $(AM_CFLAGS)
test_build_libcephfs_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += test_build_libcephfs

endif # WITH_BUILD_TESTS


## Benchmarks

ceph_smalliobench_SOURCES = \
	test/bench/small_io_bench.cc \
	test/bench/rados_backend.cc \
	test/bench/detailed_stat_collector.cc \
	test/bench/bencher.cc
ceph_smalliobench_LDADD = $(LIBRADOS) $(BOOST_PROGRAM_OPTIONS_LIBS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_smalliobench

ceph_smalliobenchfs_SOURCES = \
	test/bench/small_io_bench_fs.cc \
	test/bench/testfilestore_backend.cc \
	test/bench/detailed_stat_collector.cc \
	test/bench/bencher.cc
ceph_smalliobenchfs_LDADD = $(LIBRADOS) $(BOOST_PROGRAM_OPTIONS_LIBS) $(LIBOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_smalliobenchfs

ceph_smalliobenchdumb_SOURCES = \
	test/bench/small_io_bench_dumb.cc \
	test/bench/dumb_backend.cc \
	test/bench/detailed_stat_collector.cc \
	test/bench/bencher.cc
ceph_smalliobenchdumb_LDADD = $(LIBRADOS) $(BOOST_PROGRAM_OPTIONS_LIBS) $(LIBOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_smalliobenchdumb

ceph_smalliobenchrbd_SOURCES = \
	test/bench/small_io_bench_rbd.cc \
	test/bench/rbd_backend.cc \
	test/bench/detailed_stat_collector.cc \
	test/bench/bencher.cc
ceph_smalliobenchrbd_LDADD = $(LIBRBD) $(LIBRADOS) $(BOOST_PROGRAM_OPTIONS_LIBS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_smalliobenchrbd

ceph_tpbench_SOURCES = \
	test/bench/tp_bench.cc \
	test/bench/detailed_stat_collector.cc
ceph_tpbench_LDADD = $(LIBRADOS) $(BOOST_PROGRAM_OPTIONS_LIBS) $(LIBOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_tpbench

ceph_omapbench_SOURCES = test/omap_bench.cc
ceph_omapbench_LDADD = $(LIBRADOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_omapbench

if LINUX
ceph_kvstorebench_SOURCES = \
	test/kv_store_bench.cc \
	key_value_store/kv_flat_btree_async.cc
ceph_kvstorebench_LDADD = $(LIBRADOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_kvstorebench
endif

## System tests

if LINUX
libsystest_la_SOURCES = \
	test/system/cross_process_sem.cc \
	test/system/systest_runnable.cc \
	test/system/systest_settings.cc
libsystest_la_LIBADD = $(CEPH_GLOBAL)
noinst_LTLIBRARIES += libsystest.la

ceph_test_rados_list_parallel_SOURCES = \
	test/system/rados_list_parallel.cc \
	test/system/st_rados_create_pool.cc \
	test/system/st_rados_list_objects.cc
ceph_test_rados_list_parallel_LDADD = $(LIBRADOS) libsystest.la $(PTHREAD_LIBS)
bin_DEBUGPROGRAMS += ceph_test_rados_list_parallel

ceph_test_rados_open_pools_parallel_SOURCES = \
	test/system/rados_open_pools_parallel.cc \
	test/system/st_rados_create_pool.cc
ceph_test_rados_open_pools_parallel_LDADD = $(LIBRADOS) libsystest.la $(PTHREAD_LIBS)
bin_DEBUGPROGRAMS += ceph_test_rados_open_pools_parallel

ceph_test_rados_delete_pools_parallel_SOURCES = \
	test/system/rados_delete_pools_parallel.cc \
	test/system/st_rados_create_pool.cc \
	test/system/st_rados_delete_pool.cc \
	test/system/st_rados_list_objects.cc
ceph_test_rados_delete_pools_parallel_LDADD = $(LIBRADOS) libsystest.la $(PTHREAD_LIBS)
bin_DEBUGPROGRAMS += ceph_test_rados_delete_pools_parallel

ceph_test_rados_watch_notify_SOURCES = \
	test/system/rados_watch_notify.cc \
	test/system/st_rados_create_pool.cc \
	test/system/st_rados_delete_pool.cc \
	test/system/st_rados_delete_objs.cc \
	test/system/st_rados_watch.cc \
	test/system/st_rados_notify.cc
ceph_test_rados_watch_notify_LDADD = $(LIBRADOS) libsystest.la $(PTHREAD_LIBS)
bin_DEBUGPROGRAMS += ceph_test_rados_watch_notify
endif

ceph_bench_log_SOURCES = test/bench_log.cc
ceph_bench_log_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_bench_log



## Unit tests

check_SCRIPTS += \
	test/ceph_objectstore_tool.py \
	test/cephtool-test-osd.sh \
	test/cephtool-test-mon.sh \
	test/cephtool-test-mds.sh \
	unittest_bufferlist.sh \
	test/encoding/check-generated.sh \
	test/mon/osd-pool-create.sh \
	test/mon/misc.sh \
	test/mon/osd-crush.sh \
	test/mon/osd-erasure-code-profile.sh \
	test/mon/mkfs.sh \
	test/osd/osd-config.sh \
	test/osd/osd-bench.sh \
	test/ceph-disk.sh \
	test/mon/mon-handle-forward.sh

if ENABLE_DOCKER
check_SCRIPTS += \
	test/ceph-disk-root.sh
endif

EXTRA_DIST += \
	$(srcdir)/test/mon/mon-test-helpers.sh \
	$(srcdir)/test/osd/osd-test-helpers.sh \
        $(srcdir)/test/coverage.sh \
	$(patsubst %,$(srcdir)/%,$(check_SCRIPTS))

# target to build but not run the unit tests
unittests:: $(check_PROGRAMS)

UNITTEST_CXXFLAGS = \
	$(AM_CXXFLAGS) \
	-I$(top_srcdir)/src/gtest/include \
	-I$(top_builddir)/src/gtest/include
UNITTEST_LDADD = \
	$(top_builddir)/src/gtest/lib/libgtest.a \
	$(top_builddir)/src/gtest/lib/libgtest_main.a \
	$(PTHREAD_LIBS)

unittest_encoding_SOURCES = test/encoding.cc
unittest_encoding_LDADD = $(LIBCEPHFS) $(LIBRADOS) -lm $(UNITTEST_LDADD)
unittest_encoding_CXXFLAGS = $(UNITTEST_CXXFLAGS) -fno-strict-aliasing
check_PROGRAMS += unittest_encoding

unittest_addrs_SOURCES = test/test_addrs.cc
unittest_addrs_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_addrs_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_addrs

unittest_bloom_filter_SOURCES = test/common/test_bloom_filter.cc
unittest_bloom_filter_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_bloom_filter_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_bloom_filter

unittest_histogram_SOURCES = test/common/histogram.cc
unittest_histogram_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_histogram_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_histogram

unittest_str_map_SOURCES = test/common/test_str_map.cc
unittest_str_map_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_str_map_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_str_map

unittest_sharedptr_registry_SOURCES = test/common/test_sharedptr_registry.cc
unittest_sharedptr_registry_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_sharedptr_registry_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_sharedptr_registry

unittest_shared_cache_SOURCES = test/common/test_shared_cache.cc
unittest_shared_cache_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_shared_cache_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_shared_cache

unittest_sloppy_crc_map_SOURCES = test/common/test_sloppy_crc_map.cc
unittest_sloppy_crc_map_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_sloppy_crc_map_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_sloppy_crc_map

unittest_util_SOURCES = test/common/test_util.cc
unittest_util_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_util_LDADD = $(LIBCOMMON) -lm $(UNITTEST_LDADD) $(CRYPTO_LIBS) $(EXTRALIBS)
check_PROGRAMS += unittest_util

unittest_crush_indep_SOURCES = test/crush/indep.cc
unittest_crush_indep_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_crush_indep_LDADD = $(LIBCOMMON) -lm $(UNITTEST_LDADD) $(CEPH_CRUSH) $(EXTRALIBS) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_crush_indep

unittest_osdmap_SOURCES = test/osd/TestOSDMap.cc
unittest_osdmap_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_osdmap_LDADD = $(UNITTEST_LDADD) $(LIBCOMMON) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_osdmap

unittest_workqueue_SOURCES = test/test_workqueue.cc
unittest_workqueue_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_workqueue_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_workqueue

unittest_striper_SOURCES = test/test_striper.cc 
unittest_striper_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_striper_LDADD = $(LIBOSDC) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_striper

unittest_prebufferedstreambuf_SOURCES = test/test_prebufferedstreambuf.cc 
unittest_prebufferedstreambuf_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_prebufferedstreambuf_LDADD = $(LIBCOMMON) $(UNITTEST_LDADD) $(EXTRALIBS)
check_PROGRAMS += unittest_prebufferedstreambuf

unittest_str_list_SOURCES = test/test_str_list.cc
unittest_str_list_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_str_list_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_str_list

unittest_log_SOURCES = log/test.cc
unittest_log_LDADD = $(LIBCOMMON) $(UNITTEST_LDADD)
unittest_log_CXXFLAGS = $(UNITTEST_CXXFLAGS) -O2
check_PROGRAMS += unittest_log

unittest_throttle_SOURCES = test/common/Throttle.cc
unittest_throttle_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_throttle_CXXFLAGS = $(UNITTEST_CXXFLAGS) -O2
check_PROGRAMS += unittest_throttle

unittest_crush_wrapper_SOURCES = test/crush/TestCrushWrapper.cc
unittest_crush_wrapper_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL) $(LIBCRUSH)
unittest_crush_wrapper_CXXFLAGS = $(UNITTEST_CXXFLAGS) -O2
check_PROGRAMS += unittest_crush_wrapper

unittest_base64_SOURCES = test/base64.cc
unittest_base64_LDADD = $(LIBCEPHFS) -lm $(UNITTEST_LDADD)
unittest_base64_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_base64

unittest_ceph_argparse_SOURCES = test/ceph_argparse.cc
unittest_ceph_argparse_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_ceph_argparse_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_ceph_argparse

unittest_ceph_compatset_SOURCES = test/ceph_compatset.cc
unittest_ceph_compatset_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_ceph_compatset_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_ceph_compatset

unittest_mds_types_SOURCES = test/fs/mds_types.cc
unittest_mds_types_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_mds_types_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_mds_types

unittest_osd_types_SOURCES = test/osd/types.cc
unittest_osd_types_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_osd_types_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL) 
check_PROGRAMS += unittest_osd_types

unittest_pglog_SOURCES = test/osd/TestPGLog.cc
unittest_pglog_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_pglog_LDADD = $(LIBOSD) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_pglog
if LINUX
unittest_pglog_LDADD += -ldl
endif # LINUX

unittest_ecbackend_SOURCES = test/osd/TestECBackend.cc
unittest_ecbackend_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_ecbackend_LDADD = $(LIBOSD) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_ecbackend

unittest_hitset_SOURCES = test/osd/hitset.cc
unittest_hitset_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_hitset_LDADD = $(LIBOSD) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_hitset

unittest_lru_SOURCES = test/common/test_lru.cc
unittest_lru_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_lru_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_lru

unittest_io_priority_SOURCES = test/common/test_io_priority.cc
unittest_io_priority_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_io_priority_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_PROGRAMS += unittest_io_priority

unittest_gather_SOURCES = test/gather.cc
unittest_gather_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_gather_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_gather

unittest_run_cmd_SOURCES = test/run_cmd.cc
unittest_run_cmd_LDADD = $(LIBCEPHFS) $(UNITTEST_LDADD)
unittest_run_cmd_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_run_cmd

unittest_signals_SOURCES = test/signals.cc
unittest_signals_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_signals_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_signals

unittest_simple_spin_SOURCES = test/simple_spin.cc
unittest_simple_spin_LDADD = $(LIBCEPHFS) $(UNITTEST_LDADD)
unittest_simple_spin_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_simple_spin

unittest_librados_SOURCES = test/librados/librados.cc
unittest_librados_LDADD = $(LIBRADOS) $(UNITTEST_LDADD)
unittest_librados_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_librados

unittest_bufferlist_SOURCES = test/bufferlist.cc
unittest_bufferlist_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL) 
unittest_bufferlist_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_bufferlist

unittest_crc32c_SOURCES = test/common/test_crc32c.cc
unittest_crc32c_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_crc32c_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_crc32c

unittest_arch_SOURCES = test/test_arch.cc
unittest_arch_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_arch_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_arch

unittest_crypto_SOURCES = test/crypto.cc
unittest_crypto_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_crypto_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_crypto

unittest_crypto_init_SOURCES = test/crypto_init.cc
unittest_crypto_init_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_crypto_init_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_crypto_init

unittest_perf_counters_SOURCES = test/perf_counters.cc
unittest_perf_counters_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_perf_counters_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_perf_counters

unittest_admin_socket_SOURCES = test/admin_socket.cc
unittest_admin_socket_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_admin_socket_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_admin_socket

unittest_ceph_crypto_SOURCES = test/ceph_crypto.cc
unittest_ceph_crypto_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_ceph_crypto_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_ceph_crypto

unittest_utf8_SOURCES = test/utf8.cc
unittest_utf8_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_utf8_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_utf8

unittest_mime_SOURCES = test/mime.cc
unittest_mime_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_mime_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_mime

unittest_escape_SOURCES = test/escape.cc
unittest_escape_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_escape_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_escape

unittest_chain_xattr_SOURCES = test/objectstore/chain_xattr.cc
unittest_chain_xattr_LDADD = $(LIBOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_chain_xattr_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_chain_xattr

unittest_flatindex_SOURCES = test/os/TestFlatIndex.cc
unittest_flatindex_LDADD = $(LIBOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_flatindex_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_flatindex

unittest_strtol_SOURCES = test/strtol.cc
unittest_strtol_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_strtol_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_strtol

unittest_confutils_SOURCES = test/confutils.cc
unittest_confutils_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_confutils_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_confutils

unittest_config_SOURCES = test/common/test_config.cc
unittest_config_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_config_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_config

unittest_context_SOURCES = test/common/test_context.cc
unittest_context_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_context_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_context

unittest_heartbeatmap_SOURCES = test/heartbeat_map.cc
unittest_heartbeatmap_LDADD = $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_heartbeatmap_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_heartbeatmap

# why does this include rgw/rgw_formats.cc...?
unittest_formatter_SOURCES = \
	test/formatter.cc \
	rgw/rgw_formats.cc
unittest_formatter_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_formatter_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_formatter

unittest_libcephfs_config_SOURCES = test/libcephfs_config.cc
unittest_libcephfs_config_LDADD = $(LIBCEPHFS) $(UNITTEST_LDADD)
unittest_libcephfs_config_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_libcephfs_config

unittest_lfnindex_SOURCES = test/os/TestLFNIndex.cc
unittest_lfnindex_LDADD = $(LIBOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_lfnindex_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_lfnindex

unittest_librados_config_SOURCES = test/librados/librados_config.cc
unittest_librados_config_LDADD = $(LIBRADOS) $(UNITTEST_LDADD)
unittest_librados_config_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_librados_config

#unittest_librgw_link_SOURCES = test/librgw_link.cc
#unittest_librgw_link_LDFLAGS = $(PTHREAD_CFLAGS) ${AM_LDFLAGS}
#unittest_librgw_link_LDADD = $(LIBRGW) ${UNITTEST_LDADD}
#unittest_librgw_link_CXXFLAGS = ${CRYPTO_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
#check_PROGRAMS += unittest_librgw_link

unittest_daemon_config_SOURCES = test/daemon_config.cc
unittest_daemon_config_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_daemon_config_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_daemon_config

unittest_osd_osdcap_SOURCES = test/osd/osdcap.cc 
unittest_osd_osdcap_LDADD = $(LIBOSD) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_osd_osdcap_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_osd_osdcap

unittest_mds_authcap_SOURCES = test/mds/TestMDSAuthCaps.cc 
unittest_mds_authcap_LDADD = $(LIBMDS) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_mds_authcap_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_mds_authcap

unittest_mon_moncap_SOURCES = test/mon/moncap.cc
unittest_mon_moncap_LDADD = $(LIBMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_mon_moncap_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_mon_moncap

unittest_mon_pgmap_SOURCES = test/mon/PGMap.cc
unittest_mon_pgmap_LDADD = $(LIBMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_mon_pgmap_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_mon_pgmap

#if WITH_RADOSGW
#unittest_librgw_SOURCES = test/librgw.cc
#unittest_librgw_LDFLAGS = -lrt $(PTHREAD_CFLAGS) -lcurl ${AM_LDFLAGS}
#unittest_librgw_LDADD =  librgw.la $(LIBRADOS) ${UNITTEST_LDADD} -lexpat $(CEPH_GLOBAL)
#unittest_librgw_CXXFLAGS = ${CRYPTO_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
#check_PROGRAMS += unittest_librgw
#endif # WITH_RADOSGW

unittest_ipaddr_SOURCES = test/test_ipaddr.cc
unittest_ipaddr_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_ipaddr_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_ipaddr

unittest_texttable_SOURCES = test/test_texttable.cc
unittest_texttable_LDADD = $(LIBCOMMON) $(UNITTEST_LDADD)
unittest_texttable_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_texttable

unittest_on_exit_SOURCES = test/on_exit.cc
unittest_on_exit_LDADD = $(PTHREAD_LIBS)
check_PROGRAMS += unittest_on_exit

unittest_rbd_replay_SOURCES = test/test_rbd_replay.cc
unittest_rbd_replay_LDADD = $(LIBRBD) \
	$(LIBRADOS) \
	$(CEPH_GLOBAL) \
	librbd_replay.la \
	librbd_replay_ios.la \
	$(UNITTEST_LDADD)
unittest_rbd_replay_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_rbd_replay

unittest_readahead_SOURCES = test/common/Readahead.cc
unittest_readahead_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_readahead_CXXFLAGS = $(UNITTEST_CXXFLAGS) -O2
check_PROGRAMS += unittest_readahead


check_SCRIPTS += test/pybind/test_ceph_argparse.py

if WITH_RADOSGW
ceph_test_cors_SOURCES = test/test_cors.cc
ceph_test_cors_LDADD = \
	$(LIBRADOS) $(LIBRGW) $(CEPH_GLOBAL) \
	$(UNITTEST_LDADD) \
	-lcurl -luuid -lexpat
ceph_test_cors_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cors

ceph_test_rgw_manifest_SOURCES = test/rgw/test_rgw_manifest.cc
ceph_test_rgw_manifest_LDADD = \
	$(LIBRADOS) $(LIBRGW) $(LIBRGW_DEPS) $(CEPH_GLOBAL) \
	$(UNITTEST_LDADD) $(CRYPTO_LIBS) \
	-lcurl -luuid -lexpat

ceph_test_rgw_manifest_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rgw_manifest

ceph_test_cls_rgw_meta_SOURCES = test/test_rgw_admin_meta.cc
ceph_test_cls_rgw_meta_LDADD = \
	$(LIBRADOS) $(LIBRGW) $(CEPH_GLOBAL) \
	$(UNITTEST_LDADD) $(CRYPTO_LIBS) \
	-lcurl -luuid -lexpat \
	libcls_version_client.a libcls_log_client.a \
	libcls_statelog_client.a libcls_refcount_client.la \
	libcls_rgw_client.la libcls_user_client.a libcls_lock_client.la
ceph_test_cls_rgw_meta_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cls_rgw_meta

ceph_test_cls_rgw_log_SOURCES = test/test_rgw_admin_log.cc
ceph_test_cls_rgw_log_LDADD = \
	$(LIBRADOS) $(LIBRGW) $(CEPH_GLOBAL) \
	$(UNITTEST_LDADD) $(CRYPTO_LIBS) \
	-lcurl -luuid -lexpat \
	libcls_version_client.a libcls_log_client.a \
	libcls_statelog_client.a libcls_refcount_client.la \
	libcls_rgw_client.la libcls_user_client.a libcls_lock_client.la
ceph_test_cls_rgw_log_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cls_rgw_log

ceph_test_cls_rgw_opstate_SOURCES = test/test_rgw_admin_opstate.cc
ceph_test_cls_rgw_opstate_LDADD = \
	$(LIBRADOS) $(LIBRGW) $(CEPH_GLOBAL) \
	$(UNITTEST_LDADD) $(CRYPTO_LIBS) \
	-lcurl -luuid -lexpat \
	libcls_version_client.a libcls_log_client.a \
	libcls_statelog_client.a libcls_refcount_client.la \
	libcls_rgw_client.la libcls_user_client.a libcls_lock_client.la
ceph_test_cls_rgw_opstate_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cls_rgw_opstate
endif # WITH_RADOSGW

libradostest_la_SOURCES = \
	test/librados/test.cc \
	test/librados/TestCase.cc
noinst_LTLIBRARIES += libradostest.la
libradostest_la_CXXFLAGS = $(UNITTEST_CXXFLAGS)
RADOS_TEST_LDADD = libradostest.la

libradosstripertest_la_SOURCES = test/libradosstriper/TestCase.cc
noinst_LTLIBRARIES += libradosstripertest.la
libradosstripertest_la_LIBADD = $(RADOS_TEST_LDADD)
libradosstripertest_la_CXXFLAGS = $(UNITTEST_CXXFLAGS)
RADOS_STRIPER_TEST_LDADD = libradosstripertest.la

ceph_multi_stress_watch_SOURCES = test/multi_stress_watch.cc
ceph_multi_stress_watch_LDADD = $(LIBRADOS) $(CEPH_GLOBAL) $(RADOS_TEST_LDADD)
bin_DEBUGPROGRAMS += ceph_multi_stress_watch

ceph_test_librbd_SOURCES = test/librbd/test_librbd.cc
ceph_test_librbd_LDADD = $(LIBRBD) $(LIBRADOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL) $(RADOS_TEST_LDADD)
ceph_test_librbd_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_librbd

if LINUX
# Force use of C++ linker with dummy.cc - LIBKRBD is a C++ library
ceph_test_librbd_fsx_SOURCES = test/librbd/fsx.c common/dummy.cc
ceph_test_librbd_fsx_LDADD = $(LIBKRBD) $(LIBRBD) $(LIBRADOS)
ceph_test_librbd_fsx_CFLAGS = ${AM_CFLAGS}
bin_DEBUGPROGRAMS += ceph_test_librbd_fsx
endif

ceph_test_cls_rbd_SOURCES = test/cls_rbd/test_cls_rbd.cc
ceph_test_cls_rbd_LDADD = $(LIBRADOS) libcls_rbd_client.la libcls_lock_client.la $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_cls_rbd_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cls_rbd

ceph_test_cls_refcount_SOURCES = test/cls_refcount/test_cls_refcount.cc
ceph_test_cls_refcount_LDADD = $(LIBRADOS) libcls_refcount_client.la $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_cls_refcount_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cls_refcount

ceph_test_cls_version_SOURCES = test/cls_version/test_cls_version.cc
ceph_test_cls_version_LDADD = $(LIBRADOS) libcls_version_client.a $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_cls_version_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cls_version

ceph_test_cls_log_SOURCES = test/cls_log/test_cls_log.cc
ceph_test_cls_log_LDADD = $(LIBRADOS) libcls_log_client.a $(UNITTEST_LDADD) $(CEPH_GLOBAL) $(RADOS_TEST_LDADD)
ceph_test_cls_log_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cls_log

ceph_test_cls_statelog_SOURCES = test/cls_statelog/test_cls_statelog.cc
ceph_test_cls_statelog_LDADD = $(LIBRADOS) libcls_statelog_client.a $(UNITTEST_LDADD) $(CEPH_GLOBAL) $(RADOS_TEST_LDADD)
ceph_test_cls_statelog_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cls_statelog

ceph_test_cls_replica_log_SOURCES = test/cls_replica_log/test_cls_replica_log.cc
ceph_test_cls_replica_log_LDADD = \
	$(LIBRADOS) libcls_replica_log_client.a \
	$(UNITTEST_LDADD) $(CEPH_GLOBAL) $(RADOS_TEST_LDADD)
ceph_test_cls_replica_log_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cls_replica_log

ceph_test_cls_lock_SOURCES = test/cls_lock/test_cls_lock.cc
ceph_test_cls_lock_LDADD = $(LIBRADOS) libcls_lock_client.la $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_cls_lock_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cls_lock

ceph_test_cls_hello_SOURCES = test/cls_hello/test_cls_hello.cc
ceph_test_cls_hello_LDADD = \
	$(LIBRADOS) $(CRYPTO_LIBS) \
	$(UNITTEST_LDADD) $(CEPH_GLOBAL) $(RADOS_TEST_LDADD)
ceph_test_cls_hello_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cls_hello

if WITH_RADOSGW
ceph_test_cls_rgw_SOURCES = test/cls_rgw/test_cls_rgw.cc
ceph_test_cls_rgw_LDADD = $(LIBRADOS) libcls_rgw_client.la $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_cls_rgw_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_cls_rgw
endif # WITH_RADOSGW

ceph_test_mon_workloadgen_SOURCES = test/mon/test_mon_workloadgen.cc
ceph_test_mon_workloadgen_LDADD = $(LIBOS) $(LIBOSDC) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_mon_workloadgen

ceph_test_mon_msg_SOURCES = test/mon/test-mon-msg.cc
ceph_test_mon_msg_LDADD = $(LIBOS) $(LIBOSDC) $(CEPH_GLOBAL) $(UNITTEST_LDADD)
ceph_test_mon_msg_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_mon_msg

ceph_test_rados_api_cmd_SOURCES = test/librados/cmd.cc
ceph_test_rados_api_cmd_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_cmd_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_cmd

ceph_test_rados_api_io_SOURCES = test/librados/io.cc
ceph_test_rados_api_io_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_io_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_io

ceph_test_rados_api_c_write_operations_SOURCES = \
	test/librados/c_write_operations.cc
ceph_test_rados_api_c_write_operations_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_c_write_operations_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_c_write_operations

ceph_test_rados_api_c_read_operations_SOURCES = \
	test/librados/c_read_operations.cc
ceph_test_rados_api_c_read_operations_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_c_read_operations_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_c_read_operations

ceph_test_rados_api_aio_SOURCES = test/librados/aio.cc
ceph_test_rados_api_aio_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_aio_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_aio

ceph_test_rados_api_list_SOURCES = test/librados/list.cc
ceph_test_rados_api_list_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_list_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_list

ceph_test_rados_api_nlist_SOURCES = test/librados/nlist.cc
ceph_test_rados_api_nlist_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_nlist_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_nlist

ceph_test_rados_api_pool_SOURCES = test/librados/pool.cc
ceph_test_rados_api_pool_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_pool_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_pool

ceph_test_rados_api_stat_SOURCES = test/librados/stat.cc
ceph_test_rados_api_stat_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_stat_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_stat

ceph_test_rados_api_watch_notify_SOURCES = test/librados/watch_notify.cc
ceph_test_rados_api_watch_notify_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_watch_notify_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_watch_notify

ceph_test_rados_api_snapshots_SOURCES = test/librados/snapshots.cc
ceph_test_rados_api_snapshots_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_snapshots_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_snapshots

ceph_test_rados_api_cls_SOURCES = test/librados/cls.cc
ceph_test_rados_api_cls_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_cls_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_cls

ceph_test_rados_api_misc_SOURCES = test/librados/misc.cc
ceph_test_rados_api_misc_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL) $(RADOS_TEST_LDADD)
ceph_test_rados_api_misc_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_misc

ceph_test_rados_api_tier_SOURCES = \
	test/librados/tier.cc \
	osd/HitSet.cc
ceph_test_rados_api_tier_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL) $(RADOS_TEST_LDADD)
ceph_test_rados_api_tier_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_tier

ceph_test_rados_api_lock_SOURCES = test/librados/lock.cc
ceph_test_rados_api_lock_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_rados_api_lock_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_api_lock

ceph_test_rados_striper_api_io_SOURCES = test/libradosstriper/io.cc
ceph_test_rados_striper_api_io_LDADD = $(LIBRADOS) $(LIBRADOSSTRIPER) $(UNITTEST_LDADD) $(RADOS_STRIPER_TEST_LDADD)
ceph_test_rados_striper_api_io_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_striper_api_io

ceph_test_rados_striper_api_aio_SOURCES = test/libradosstriper/aio.cc
ceph_test_rados_striper_api_aio_LDADD = $(LIBRADOS) $(LIBRADOSSTRIPER) $(UNITTEST_LDADD) $(RADOS_STRIPER_TEST_LDADD)
ceph_test_rados_striper_api_aio_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_striper_api_aio

ceph_test_rados_striper_api_striping_SOURCES = test/libradosstriper/striping.cc
ceph_test_rados_striper_api_striping_LDADD = $(LIBRADOS) $(LIBRADOSSTRIPER) $(UNITTEST_LDADD) $(RADOS_STRIPER_TEST_LDADD)
ceph_test_rados_striper_api_striping_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_rados_striper_api_striping

ceph_test_libcephfs_SOURCES = \
	test/libcephfs/test.cc \
	test/libcephfs/readdir_r_cb.cc \
	test/libcephfs/caps.cc \
	test/libcephfs/multiclient.cc
ceph_test_libcephfs_LDADD = $(LIBCEPHFS) $(UNITTEST_LDADD)
ceph_test_libcephfs_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_libcephfs

if LINUX
ceph_test_objectstore_SOURCES = test/objectstore/store_test.cc
ceph_test_objectstore_LDADD = $(LIBOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
ceph_test_objectstore_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_objectstore

ceph_test_filestore_SOURCES = test/filestore/TestFileStore.cc
ceph_test_filestore_LDADD = $(LIBOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
ceph_test_filestore_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_filestore
endif

ceph_test_objectstore_workloadgen_SOURCES = \
	test/objectstore/workload_generator.cc \
	test/objectstore/TestObjectStoreState.cc
ceph_test_objectstore_workloadgen_LDADD = $(LIBOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_objectstore_workloadgen

ceph_test_filestore_idempotent_SOURCES = \
	test/objectstore/test_idempotent.cc \
	test/objectstore/FileStoreTracker.cc \
	test/common/ObjectContents.cc
ceph_test_filestore_idempotent_LDADD = $(LIBOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_filestore_idempotent

ceph_test_filestore_idempotent_sequence_SOURCES = \
	test/objectstore/test_idempotent_sequence.cc \
	test/objectstore/DeterministicOpSequence.cc \
	test/objectstore/TestObjectStoreState.cc \
	test/objectstore/FileStoreDiff.cc
ceph_test_filestore_idempotent_sequence_LDADD = $(LIBOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_filestore_idempotent_sequence

ceph_xattr_bench_SOURCES = test/xattr_bench.cc
ceph_xattr_bench_LDADD = $(LIBOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
ceph_xattr_bench_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_xattr_bench

ceph_test_filejournal_SOURCES = test/test_filejournal.cc
ceph_test_filejournal_LDADD = $(LIBOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
ceph_test_filejournal_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_filejournal

ceph_test_stress_watch_SOURCES = test/test_stress_watch.cc
ceph_test_stress_watch_LDADD = $(LIBRADOS) $(UNITTEST_LDADD) $(RADOS_TEST_LDADD)
ceph_test_stress_watch_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_stress_watch

ceph_test_objectcacher_stress_SOURCES = \
	test/osdc/object_cacher_stress.cc \
	test/osdc/FakeWriteback.cc
ceph_test_objectcacher_stress_LDADD = $(LIBOSDC) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_objectcacher_stress

ceph_test_snap_mapper_SOURCES = test/test_snap_mapper.cc
ceph_test_snap_mapper_LDADD = $(LIBOSD) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
ceph_test_snap_mapper_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_snap_mapper

ceph_test_object_map_SOURCES = \
	test/ObjectMap/test_object_map.cc \
	test/ObjectMap/KeyValueDBMemory.cc
ceph_test_object_map_LDADD = $(LIBOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
ceph_test_object_map_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_object_map

ceph_test_keyvaluedb_atomicity_SOURCES = test/ObjectMap/test_keyvaluedb_atomicity.cc
ceph_test_keyvaluedb_atomicity_LDADD = $(LIBOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
ceph_test_keyvaluedb_atomicity_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_keyvaluedb_atomicity

ceph_test_keyvaluedb_iterators_SOURCES = \
	test/ObjectMap/test_keyvaluedb_iterators.cc \
	test/ObjectMap/KeyValueDBMemory.cc
ceph_test_keyvaluedb_iterators_LDADD = $(LIBOS) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
ceph_test_keyvaluedb_iterators_CXXFLAGS = $(UNITTEST_CXXFLAGS)
bin_DEBUGPROGRAMS += ceph_test_keyvaluedb_iterators

ceph_test_cfuse_cache_invalidate_SOURCES = test/test_cfuse_cache_invalidate.cc
bin_DEBUGPROGRAMS += ceph_test_cfuse_cache_invalidate

ceph_test_c_headers_SOURCES = test/test_c_headers.c
ceph_test_c_headers_LDADD = $(LIBRADOS) $(LIBCEPHFS)
ceph_test_c_headers_CFLAGS = $(AM_CFLAGS) \
	-Werror \
	-Wstrict-prototypes \
	-Wredundant-decls \
	-Wall \
	-Wundef \
	-Wwrite-strings \
	-Wmissing-prototypes \
	-Wendif-labels \
	-Wmissing-include-dirs \
	-Wempty-body \
	-Wnested-externs \
	-Wformat-security \
	-Wformat-y2k \
	-Winit-self \
	-Wignored-qualifiers \
	-Wold-style-declaration \
	-Wold-style-definition \
	-Wtype-limits
bin_DEBUGPROGRAMS += ceph_test_c_headers

ceph_test_get_blkdev_size_SOURCES = test/test_get_blkdev_size.cc
ceph_test_get_blkdev_size_LDADD = $(LIBCOMMON)
bin_DEBUGPROGRAMS += ceph_test_get_blkdev_size

noinst_HEADERS += \
	test/bench/backend.h \
	test/bench/bencher.h \
	test/bench/detailed_stat_collector.h \
	test/bench/distribution.h \
	test/bench/dumb_backend.h \
	test/bench/rados_backend.h \
	test/bench/rbd_backend.h \
	test/bench/stat_collector.h \
	test/bench/testfilestore_backend.h \
	test/common/ObjectContents.h \
	test/encoding/types.h \
	test/objectstore/DeterministicOpSequence.h \
	test/objectstore/FileStoreDiff.h \
	test/objectstore/FileStoreTracker.h \
	test/objectstore/TestObjectStoreState.h \
	test/objectstore/workload_generator.h \
	test/kv_store_bench.h \
	test/librados/test.h \
	test/librados/TestCase.h \
	test/libradosstriper/TestCase.h \
	test/ObjectMap/KeyValueDBMemory.h \
	test/omap_bench.h \
	test/osdc/FakeWriteback.h \
	test/osd/Object.h \
	test/osd/RadosModel.h \
	test/osd/TestOpStat.h \
	test/system/cross_process_sem.h \
	test/system/st_rados_create_pool.h \
	test/system/st_rados_delete_objs.h \
	test/system/st_rados_delete_pool.h \
	test/system/st_rados_list_objects.h \
	test/system/st_rados_notify.h \
	test/system/st_rados_watch.h \
	test/system/systest_runnable.h \
	test/system/systest_settings.h \
	test/unit.h

