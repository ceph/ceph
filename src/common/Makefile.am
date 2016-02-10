libcommon_internal_la_SOURCES = \
	ceph_ver.c \
	common/DecayCounter.cc \
	common/LogClient.cc \
	common/LogEntry.cc \
	common/Graylog.cc \
	common/PrebufferedStreambuf.cc \
	common/SloppyCRCMap.cc \
	common/BackTrace.cc \
	common/perf_counters.cc \
	common/mutex_debug.cc \
	common/Mutex.cc \
	common/OutputDataSocket.cc \
	common/admin_socket.cc \
	common/admin_socket_client.cc \
	common/cmdparse.cc \
	common/escape.c \
	common/io_priority.cc \
	common/ceph_time.cc \
	common/Clock.cc \
	common/Throttle.cc \
	common/Timer.cc \
	common/Finisher.cc \
	common/environment.cc\
	common/assert.cc \
	xxHash/xxhash.c \
	common/run_cmd.cc \
	common/WorkQueue.cc \
	common/ConfUtils.cc \
	common/MemoryModel.cc \
	common/armor.c \
	common/fd.cc \
	common/fs_types.cc \
	common/safe_io.c \
	common/snap_types.cc \
	common/str_list.cc \
	common/str_map.cc \
	common/errno.cc \
	common/RefCountedObj.cc \
	common/common_init.cc \
	common/pipe.c \
	common/ceph_argparse.cc \
	common/ceph_context.cc \
	common/types.cc \
	common/code_environment.cc \
	common/dout.cc \
	common/histogram.cc \
	common/signal.cc \
	common/simple_spin.cc \
	common/Thread.cc \
	common/Formatter.cc \
	common/HTMLFormatter.cc \
	common/HeartbeatMap.cc \
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
	common/TextTable.cc \
	common/ceph_fs.cc \
	common/ceph_hash.cc \
	common/ceph_strings.cc \
	common/ceph_frag.cc \
	common/addr_parsing.c \
	common/hobject.cc \
	common/bloom_filter.cc \
	common/module.c \
	common/Readahead.cc \
	common/Cycles.cc \
	common/ContextCompletion.cc \
	common/TracepointProvider.cc \
	common/PluginRegistry.cc \
	common/scrub_types.cc \
	common/blkdev.cc

common/PluginRegistry.cc: ./ceph_ver.h

if ENABLE_SERVER
libcommon_internal_la_SOURCES += \
	common/xattr.c \
	common/ipaddr.cc \
	common/ceph_json.cc \
	common/util.cc \
	common/pick_address.cc
endif

if LINUX
libcommon_internal_la_SOURCES += \
	common/linux_version.c
endif

if SOLARIS
libcommon_internal_la_SOURCES += \
        common/solaris_errno.cc
endif

if AIX
libcommon_internal_la_SOURCES += \
        common/aix_errno.cc
endif

if ENABLE_XIO
libcommon_internal_la_SOURCES += \
	common/address_helper.cc
endif

# these should go out of libcommon_internal
libcommon_internal_la_SOURCES += \
	mon/MonCap.cc \
	mon/MonClient.cc \
	mon/MonMap.cc \
	osd/OSDMap.cc \
	osd/osd_types.cc \
	osd/ECMsgTypes.cc \
	osd/HitSet.cc \
	mds/MDSMap.cc \
	mds/FSMap.cc \
	mds/inode_backtrace.cc \
	mds/mdstypes.cc \
	mds/flock.cc

LIBCOMMON_DEPS += libcommon_internal.la
noinst_LTLIBRARIES += libcommon_internal.la

# inject crc in common
libcommon_crc_la_SOURCES = \
	common/sctp_crc32.c \
	common/crc32c.cc \
	common/crc32c_intel_baseline.c \
	common/crc32c_intel_fast.c

if WITH_GOOD_YASM_ELF64
libcommon_crc_la_SOURCES += common/crc32c_intel_fast_asm.S common/crc32c_intel_fast_zero_asm.S
libcommon_crc_la_LIBTOOLFLAGS = --tag=CC
endif
LIBCOMMON_DEPS += libcommon_crc.la
noinst_LTLIBRARIES += libcommon_crc.la

if HAVE_ARMV8_CRC
libcommon_crc_aarch64_la_SOURCES = common/crc32c_aarch64.c
libcommon_crc_aarch64_la_CFLAGS = $(AM_CFLAGS) $(ARM_CRC_FLAGS)
LIBCOMMON_DEPS += libcommon_crc_aarch64.la
noinst_LTLIBRARIES += libcommon_crc_aarch64.la
endif

noinst_HEADERS += \
	common/bloom_filter.hpp \
	common/sctp_crc32.h \
	common/crc32c_intel_baseline.h \
	common/crc32c_intel_fast.h \
	common/crc32c_aarch64.h \
	common/cohort_lru.h \
	common/sstring.hh \
	xxHash/xxhash.h


# important; libmsg before libauth!
LIBCOMMON_DEPS += \
	$(LIBERASURE_CODE) \
	$(LIBCOMPRESSOR) \
	$(LIBMSG) $(LIBAUTH) \
	$(LIBCRUSH) $(LIBJSON_SPIRIT) $(LIBLOG) $(LIBARCH) \
	$(BOOST_RANDOM_LIBS) \
	-luuid

if LINUX
LIBCOMMON_DEPS += -lrt -lblkid
endif # LINUX

libcommon_la_SOURCES = common/buffer.cc
libcommon_la_LIBADD = $(LIBCOMMON_DEPS)
noinst_LTLIBRARIES += libcommon.la

noinst_HEADERS += \
	common/BackTrace.h \
	common/RefCountedObj.h \
	common/HeartbeatMap.h \
	common/LogClient.h \
	common/LogEntry.h \
	common/Graylog.h \
	common/Preforker.h \
	common/SloppyCRCMap.h \
	common/WorkQueue.h \
	common/OpQueue.h \
	common/PrioritizedQueue.h \
	common/WeightedPriorityQueue.h \
	common/ceph_argparse.h \
	common/ceph_context.h \
	common/xattr.h \
	common/blkdev.h \
	common/compiler_extensions.h \
	common/debug.h \
	common/dout.h \
	common/escape.h \
	common/fd.h \
	common/version.h \
	common/hex.h \
	common/histogram.h \
	common/entity_name.h \
	common/errno.h \
	common/environment.h \
	common/likely.h \
	common/lockdep.h \
	common/obj_bencher.h \
	common/snap_types.h \
	common/Clock.h \
	common/Cond.h \
	common/ConfUtils.h \
	common/DecayCounter.h \
	common/Finisher.h \
	common/Formatter.h \
	common/HTMLFormatter.h \
	common/perf_counters.h \
	common/OutputDataSocket.h \
	common/admin_socket.h \
	common/admin_socket_client.h \
	common/random_cache.hpp \
	common/shared_cache.hpp \
	common/tracked_int_ptr.hpp \
	common/simple_cache.hpp \
	common/sharedptr_registry.hpp \
	common/map_cacher.hpp \
	common/MemoryModel.h \
	common/Mutex.h \
	common/QueueRing.h \
	common/PrebufferedStreambuf.h \
	common/RWLock.h \
	common/Semaphore.h \
	common/TextTable.h \
	common/Thread.h \
	common/Throttle.h \
	common/Timer.h \
	common/TrackedOp.h \
	common/arch.h \
	common/armor.h \
	common/common_init.h \
	common/io_priority.h \
	common/pipe.h \
	common/code_environment.h \
	common/signal.h \
	common/simple_spin.h \
	common/run_cmd.h \
	common/safe_io.h \
	common/config.h \
	common/config_obs.h \
	common/config_opts.h \
	common/ceph_crypto.h \
	common/ceph_crypto_cms.h \
	common/ceph_json.h \
	common/lru_map.h \
	common/utf8.h \
	common/mime.h \
	common/pick_address.h \
	common/strtol.h \
	common/static_assert.h \
	common/AsyncReserver.h \
	common/sync_filesystem.h \
	common/cmdparse.h \
	common/hobject.h \
	common/linux_version.h \
	common/module.h \
	common/Continuation.h \
	common/Readahead.h \
	common/Cycles.h \
	common/Initialize.h \
	common/ContextCompletion.h \
	common/bit_vector.hpp \
	common/SubProcess.h \
	common/valgrind.h \
	common/TracepointProvider.h \
	common/event_socket.h \
	common/PluginRegistry.h \
	common/scrub_types.h \
	common/ceph_time.h \
	common/ceph_timer.h \
	common/align.h \
	common/mutex_debug.h \
	common/shunique_lock.h \
	common/cxx_function.hpp \
	common/waiter.h

if ENABLE_XIO
noinst_HEADERS += \
	common/address_helper.h
endif


libsecret_la_SOURCES = common/secret.c
libsecret_la_LIBADD = $(KEYUTILS_LIB)
if LINUX
noinst_LTLIBRARIES += libsecret.la
endif

noinst_HEADERS += common/secret.h
