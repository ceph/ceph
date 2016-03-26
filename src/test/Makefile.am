include test/erasure-code/Makefile.am
include test/messenger/Makefile.am
include test/compressor/Makefile.am

if ENABLE_CLIENT
include test/Makefile-client.am
endif

if ENABLE_SERVER
include test/Makefile-server.am
endif

## Unknown/other tests

ceph_test_timers_SOURCES = test/TestTimers.cc
ceph_test_timers_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_timers

ceph_test_signal_handlers_SOURCES = test/TestSignalHandlers.cc
ceph_test_signal_handlers_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_signal_handlers

ceph_test_rewrite_latency_SOURCES = test/test_rewrite_latency.cc
ceph_test_rewrite_latency_LDADD = $(LIBCOMMON) $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_DEBUGPROGRAMS += ceph_test_rewrite_latency

ceph_test_crypto_SOURCES = test/testcrypto.cc
ceph_test_crypto_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_crypto


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
endif # WITH_BUILD_TESTS


if LINUX
libsystest_la_SOURCES = \
	test/system/cross_process_sem.cc \
	test/system/systest_runnable.cc \
	test/system/systest_settings.cc
libsystest_la_LIBADD = $(CEPH_GLOBAL)
noinst_LTLIBRARIES += libsystest.la
endif # LINUX


ceph_bench_log_SOURCES = test/bench_log.cc
ceph_bench_log_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_bench_log



## Unit tests

check_SCRIPTS += \
	test/ceph_objectstore_tool.py \
	test/test-ceph-helpers.sh \
	test/cephtool-test-osd.sh \
	test/cephtool-test-mon.sh \
	test/cephtool-test-mds.sh \
	test/cephtool-test-rados.sh \
       test/test_pool_create.sh \
	unittest_bufferlist.sh \
	test/encoding/check-generated.sh \
	test/mon/osd-pool-create.sh \
	test/mon/misc.sh \
	test/mon/osd-crush.sh \
	test/mon/mon-ping.sh \
	test/mon/mon-created-time.sh \
	test/mon/osd-erasure-code-profile.sh \
	test/mon/mkfs.sh \
	test/mon/mon-scrub.sh \
       test/mon/test_pool_quota.sh \
	test/osd/osd-scrub-repair.sh \
	test/osd/osd-scrub-snaps.sh \
	test/osd/osd-config.sh \
	test/osd/osd-reuse-id.sh \
	test/osd/osd-bench.sh \
	test/osd/osd-reactivate.sh \
	test/osd/osd-copy-from.sh \
	test/osd/osd-markdown.sh \
	test/mon/mon-handle-forward.sh \
	test/libradosstriper/rados-striper.sh \
	test/test_objectstore_memstore.sh \
        test/test_pidfile.sh

EXTRA_DIST += \
	$(srcdir)/test/python/brag-client/setup.py \
	$(srcdir)/test/python/brag-client/tox.ini \
	$(srcdir)/test/python/brag-client/tests/test_ceph_brag.py \
	$(srcdir)/test/debian-jessie/Dockerfile.in \
	$(srcdir)/test/debian-jessie/install-deps.sh \
	$(srcdir)/test/debian-jessie/debian \
	$(srcdir)/test/ubuntu-12.04/Dockerfile.in \
	$(srcdir)/test/ubuntu-12.04/install-deps.sh \
	$(srcdir)/test/ubuntu-12.04/debian \
	$(srcdir)/test/ubuntu-14.04/Dockerfile.in \
	$(srcdir)/test/ubuntu-14.04/install-deps.sh \
	$(srcdir)/test/ubuntu-14.04/debian \
	$(srcdir)/test/fedora-21/Dockerfile.in \
	$(srcdir)/test/fedora-21/install-deps.sh \
	$(srcdir)/test/fedora-21/ceph.spec.in \
	$(srcdir)/test/centos-6/Dockerfile.in \
	$(srcdir)/test/centos-6/install-deps.sh \
	$(srcdir)/test/centos-6/ceph.spec.in \
	$(srcdir)/test/centos-7/Dockerfile.in \
	$(srcdir)/test/centos-7/install-deps.sh \
	$(srcdir)/test/centos-7/ceph.spec.in \
	$(srcdir)/test/opensuse-13.2/Dockerfile.in \
	$(srcdir)/test/opensuse-13.2/install-deps.sh \
	$(srcdir)/test/opensuse-13.2/ceph.spec.in \
        $(srcdir)/test/coverage.sh \
	$(patsubst %,$(srcdir)/%,$(check_SCRIPTS))

# target to build but not run the unit tests
unittests:: $(check_PROGRAMS)

UNITTEST_CXXFLAGS = \
	$(AM_CXXFLAGS) \
	-I$(top_srcdir)/src/gmock/include \
	-I$(top_builddir)/src/gmock/include \
	-I$(top_srcdir)/src/gmock/gtest/include \
	-I$(top_builddir)/src/gmock/gtest/include
UNITTEST_LDADD = \
	$(top_builddir)/src/gmock/lib/libgmock_main.la \
	$(top_builddir)/src/gmock/lib/libgmock.la \
	$(top_builddir)/src/gmock/gtest/lib/libgtest.la \
	$(PTHREAD_LIBS)

if SOLARIS
UNITTEST_LDADD += \
	-lsocket -lnsl
endif

unittest_addrs_SOURCES = test/test_addrs.cc
unittest_addrs_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_addrs_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_addrs

unittest_blkdev_SOURCES = test/common/test_blkdev.cc
unittest_blkdev_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_blkdev_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
if LINUX
check_TESTPROGRAMS += unittest_blkdev
endif

unittest_bloom_filter_SOURCES = test/common/test_bloom_filter.cc
unittest_bloom_filter_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_bloom_filter_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_bloom_filter

unittest_histogram_SOURCES = test/common/histogram.cc
unittest_histogram_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_histogram_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_histogram

unittest_prioritized_queue_SOURCES = test/common/test_prioritized_queue.cc
unittest_prioritized_queue_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_prioritized_queue_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_prioritized_queue

unittest_weighted_priority_queue_SOURCES = test/common/test_weighted_priority_queue.cc
unittest_weighted_priority_queue_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_weighted_priority_queue_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_weighted_priority_queue

unittest_str_map_SOURCES = test/common/test_str_map.cc
unittest_str_map_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_str_map_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_str_map

unittest_mutex_debug_SOURCES = test/common/test_mutex_debug.cc
unittest_mutex_debug_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_mutex_debug_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL) ${EXTRALIBS}
check_TESTPROGRAMS += unittest_mutex_debug

unittest_shunique_lock_SOURCES = test/common/test_shunique_lock.cc
unittest_shunique_lock_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_shunique_lock_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL) ${EXTRALIBS}
check_TESTPROGRAMS += unittest_shunique_lock

unittest_sharedptr_registry_SOURCES = test/common/test_sharedptr_registry.cc
unittest_sharedptr_registry_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_sharedptr_registry_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_sharedptr_registry

unittest_shared_cache_SOURCES = test/common/test_shared_cache.cc
unittest_shared_cache_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_shared_cache_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_shared_cache

unittest_sloppy_crc_map_SOURCES = test/common/test_sloppy_crc_map.cc
unittest_sloppy_crc_map_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_sloppy_crc_map_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_sloppy_crc_map

unittest_time_SOURCES = test/common/test_time.cc
unittest_time_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_time_LDADD = $(LIBCOMMON) -lm $(UNITTEST_LDADD) $(CRYPTO_LIBS) $(EXTRALIBS)
check_TESTPROGRAMS += unittest_time

unittest_util_SOURCES = test/common/test_util.cc
unittest_util_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_util_LDADD = $(LIBCOMMON) -lm $(UNITTEST_LDADD) $(CRYPTO_LIBS) $(EXTRALIBS)
check_TESTPROGRAMS += unittest_util

unittest_crush_wrapper_SOURCES = test/crush/CrushWrapper.cc
unittest_crush_wrapper_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL) $(LIBCRUSH)
unittest_crush_wrapper_CXXFLAGS = $(UNITTEST_CXXFLAGS) -O2
check_TESTPROGRAMS += unittest_crush_wrapper

unittest_crush_SOURCES = test/crush/crush.cc
unittest_crush_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_crush_LDADD = $(LIBCOMMON) -lm $(UNITTEST_LDADD) $(CEPH_CRUSH) $(EXTRALIBS) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_crush

unittest_osdmap_SOURCES = test/osd/TestOSDMap.cc
unittest_osdmap_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_osdmap_LDADD = $(UNITTEST_LDADD) $(LIBCOMMON) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_osdmap

unittest_workqueue_SOURCES = test/test_workqueue.cc
unittest_workqueue_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_workqueue_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_workqueue

unittest_striper_SOURCES = test/test_striper.cc
unittest_striper_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_striper_LDADD = $(LIBOSDC) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_striper

unittest_prebufferedstreambuf_SOURCES = test/test_prebufferedstreambuf.cc
unittest_prebufferedstreambuf_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_prebufferedstreambuf_LDADD = $(LIBCOMMON) $(UNITTEST_LDADD) $(EXTRALIBS)
check_TESTPROGRAMS += unittest_prebufferedstreambuf

unittest_str_list_SOURCES = test/test_str_list.cc
unittest_str_list_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_str_list_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_str_list

unittest_log_SOURCES = log/test.cc
unittest_log_LDADD = $(LIBCOMMON) $(UNITTEST_LDADD)
unittest_log_CXXFLAGS = $(UNITTEST_CXXFLAGS) -O2
check_TESTPROGRAMS += unittest_log

unittest_throttle_SOURCES = test/common/Throttle.cc
unittest_throttle_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_throttle_CXXFLAGS = $(UNITTEST_CXXFLAGS) -O2
check_TESTPROGRAMS += unittest_throttle

unittest_ceph_argparse_SOURCES = test/ceph_argparse.cc
unittest_ceph_argparse_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_ceph_argparse_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_ceph_argparse

unittest_ceph_compatset_SOURCES = test/ceph_compatset.cc
unittest_ceph_compatset_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_ceph_compatset_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_ceph_compatset

unittest_mds_types_SOURCES = test/fs/mds_types.cc
unittest_mds_types_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_mds_types_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_mds_types

unittest_osd_types_SOURCES = test/osd/types.cc
unittest_osd_types_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_osd_types_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_osd_types

unittest_lru_SOURCES = test/common/test_lru.cc
unittest_lru_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_lru_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_lru

unittest_io_priority_SOURCES = test/common/test_io_priority.cc
unittest_io_priority_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_io_priority_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_io_priority

unittest_gather_SOURCES = test/gather.cc
unittest_gather_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_gather_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_gather

unittest_signals_SOURCES = test/signals.cc
unittest_signals_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_signals_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_signals

unittest_bufferlist_SOURCES = test/bufferlist.cc
unittest_bufferlist_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_bufferlist_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_bufferlist

unittest_xlist_SOURCES = test/test_xlist.cc
unittest_xlist_LDADD = $(UNITTEST_LDADD) $(LIBCOMMON)
unittest_xlist_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_xlist

unittest_crc32c_SOURCES = test/common/test_crc32c.cc
unittest_crc32c_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_crc32c_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_crc32c

unittest_arch_SOURCES = test/test_arch.cc
unittest_arch_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_arch_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_arch

unittest_crypto_SOURCES = test/crypto.cc
unittest_crypto_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_crypto_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_crypto

unittest_crypto_init_SOURCES = test/crypto_init.cc
unittest_crypto_init_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_crypto_init_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_crypto_init

unittest_perf_counters_SOURCES = test/perf_counters.cc
unittest_perf_counters_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_perf_counters_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_perf_counters

unittest_admin_socket_SOURCES = test/admin_socket.cc
unittest_admin_socket_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_admin_socket_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_admin_socket

unittest_ceph_crypto_SOURCES = test/ceph_crypto.cc
unittest_ceph_crypto_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_ceph_crypto_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_ceph_crypto

unittest_utf8_SOURCES = test/utf8.cc
unittest_utf8_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_utf8_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_utf8

unittest_mime_SOURCES = test/mime.cc
unittest_mime_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_mime_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_mime

unittest_escape_SOURCES = test/escape.cc
unittest_escape_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_escape_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_escape

unittest_strtol_SOURCES = test/strtol.cc
unittest_strtol_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_strtol_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_strtol

unittest_confutils_SOURCES = test/confutils.cc
unittest_confutils_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_confutils_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_confutils

unittest_config_SOURCES = test/common/test_config.cc
unittest_config_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_config_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_config

unittest_context_SOURCES = test/common/test_context.cc
unittest_context_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_context_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_context

unittest_safe_io_SOURCES = test/common/test_safe_io.cc
unittest_safe_io_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_safe_io_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_safe_io

unittest_heartbeatmap_SOURCES = test/heartbeat_map.cc
unittest_heartbeatmap_LDADD = $(LIBCOMMON) $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_heartbeatmap_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_heartbeatmap

# why does this include rgw/rgw_formats.cc...?
unittest_formatter_SOURCES = \
	test/formatter.cc \
	rgw/rgw_formats.cc
unittest_formatter_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_formatter_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_formatter

unittest_daemon_config_SOURCES = test/daemon_config.cc
unittest_daemon_config_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_daemon_config_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_daemon_config

unittest_ipaddr_SOURCES = test/test_ipaddr.cc
unittest_ipaddr_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_ipaddr_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_ipaddr

unittest_texttable_SOURCES = test/test_texttable.cc
unittest_texttable_LDADD = $(LIBCOMMON) $(UNITTEST_LDADD)
unittest_texttable_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_TESTPROGRAMS += unittest_texttable

unittest_on_exit_SOURCES = test/on_exit.cc
unittest_on_exit_LDADD = $(PTHREAD_LIBS)
check_TESTPROGRAMS += unittest_on_exit

unittest_readahead_SOURCES = test/common/Readahead.cc
unittest_readahead_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
unittest_readahead_CXXFLAGS = $(UNITTEST_CXXFLAGS) -O2
check_TESTPROGRAMS += unittest_readahead

unittest_tableformatter_SOURCES = test/common/test_tableformatter.cc
unittest_tableformatter_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_tableformatter_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_tableformatter

unittest_bit_vector_SOURCES = test/common/test_bit_vector.cc
unittest_bit_vector_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_bit_vector_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_bit_vector

unittest_interval_set_SOURCES = test/common/test_interval_set.cc
unittest_interval_set_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_interval_set_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL)
check_TESTPROGRAMS += unittest_interval_set

unittest_subprocess_SOURCES = test/test_subprocess.cc
unittest_subprocess_LDADD = $(LIBCOMMON) $(UNITTEST_LDADD)
unittest_subprocess_CXXFLAGS = $(UNITTEST_CXXFLAGS)
check_PROGRAMS += unittest_subprocess

unittest_async_compressor_SOURCES = test/common/test_async_compressor.cc
unittest_async_compressor_CXXFLAGS = $(UNITTEST_CXXFLAGS)
unittest_async_compressor_LDADD = $(UNITTEST_LDADD) $(CEPH_GLOBAL) $(LIBCOMPRESSOR) $(LIBCOMMON)
check_PROGRAMS += unittest_async_compressor

check_SCRIPTS += test/pybind/test_ceph_argparse.py
check_SCRIPTS += test/pybind/test_ceph_daemon.py

ceph_test_objectcacher_stress_SOURCES = \
	test/osdc/object_cacher_stress.cc \
	test/osdc/FakeWriteback.cc
ceph_test_objectcacher_stress_LDADD = $(LIBOSDC) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_objectcacher_stress

ceph_test_cfuse_cache_invalidate_SOURCES = test/test_cfuse_cache_invalidate.cc
bin_DEBUGPROGRAMS += ceph_test_cfuse_cache_invalidate

if LINUX
ceph_test_get_blkdev_size_SOURCES = test/test_get_blkdev_size.cc
ceph_test_get_blkdev_size_LDADD = $(LIBCOMMON)
bin_DEBUGPROGRAMS += ceph_test_get_blkdev_size
endif

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
	test/unit.h \
	test/journal/RadosTestFixture.h
