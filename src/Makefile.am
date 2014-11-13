include Makefile-env.am

SUBDIRS += ocf java tracing
DIST_SUBDIRS += gtest ocf libs3 java tracing



# subdirs

include arch/Makefile.am
include auth/Makefile.am
include brag/Makefile.am
include crush/Makefile.am
include mon/Makefile.am
include mds/Makefile.am
include os/Makefile.am
include osd/Makefile.am
include erasure-code/Makefile.am
include osdc/Makefile.am
include client/Makefile.am
include global/Makefile.am
include json_spirit/Makefile.am
include log/Makefile.am
include perfglue/Makefile.am
include common/Makefile.am
include msg/Makefile.am
include messages/Makefile.am
include include/Makefile.am
include librados/Makefile.am
include libradosstriper/Makefile.am
include librbd/Makefile.am
include rgw/Makefile.am
include cls/Makefile.am
include key_value_store/Makefile.am
include rbd_replay/Makefile.am
include test/Makefile.am
include tools/Makefile.am


# core daemons

ceph_mon_SOURCES = ceph_mon.cc
ceph_mon_LDADD = $(LIBMON) $(LIBOS) $(CEPH_GLOBAL) $(LIBCOMMON)
bin_PROGRAMS += ceph-mon

ceph_osd_SOURCES = ceph_osd.cc
ceph_osd_LDADD = $(LIBOSD) $(CEPH_GLOBAL) $(LIBCOMMON)
bin_PROGRAMS += ceph-osd

ceph_mds_SOURCES = ceph_mds.cc
ceph_mds_LDADD = $(LIBMDS) $(LIBOSDC) $(CEPH_GLOBAL) $(LIBCOMMON)
bin_PROGRAMS += ceph-mds


# admin tools


# user tools

mount_ceph_SOURCES = mount/mount.ceph.c
mount_ceph_LDADD = $(LIBSECRET) $(LIBCOMMON)
if LINUX
su_sbin_PROGRAMS += mount.ceph
endif # LINUX
su_sbin_SCRIPTS += mount.fuse.ceph

cephfs_SOURCES = cephfs.cc
cephfs_LDADD = $(LIBCOMMON)
bin_PROGRAMS += cephfs

librados_config_SOURCES = librados-config.cc
librados_config_LDADD = $(LIBRADOS) $(CEPH_GLOBAL)
bin_PROGRAMS += librados-config

ceph_syn_SOURCES = ceph_syn.cc
ceph_syn_SOURCES += client/SyntheticClient.cc # uses g_conf.. needs cleanup
ceph_syn_LDADD = $(LIBCLIENT) $(CEPH_GLOBAL)
bin_PROGRAMS += ceph-syn


libkrbd_la_SOURCES = krbd.cc
libkrbd_la_LIBADD = $(LIBSECRET) -lblkid -ludev
if LINUX
noinst_LTLIBRARIES += libkrbd.la
endif LINUX

rbd_SOURCES = rbd.cc
rbd_LDADD = $(LIBKRBD) $(LIBRBD) $(LIBRADOS) $(CEPH_GLOBAL)
if LINUX
bin_PROGRAMS += rbd
endif #LINUX


# Fuse targets

if WITH_FUSE
ceph_fuse_SOURCES = ceph_fuse.cc
ceph_fuse_LDADD = $(LIBCLIENT_FUSE) $(CEPH_GLOBAL)
bin_PROGRAMS += ceph-fuse

rbd_fuse_SOURCES = rbd_fuse/rbd-fuse.c
rbd_fuse_LDADD = -lfuse $(LIBRBD) $(LIBRADOS) $(CEPH_GLOBAL)
bin_PROGRAMS += rbd-fuse
endif # WITH_FUSE


# libcephfs (this should go somewhere else in the future)

libcephfs_la_SOURCES = libcephfs.cc
libcephfs_la_LIBADD = $(LIBCLIENT) $(LIBCOMMON) $(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS)
libcephfs_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '^ceph_.*'
lib_LTLIBRARIES += libcephfs.la

# jni library (java source is in src/java)

if ENABLE_CEPHFS_JAVA
libcephfs_jni_la_SOURCES = \
	java/native/libcephfs_jni.cc \
	java/native/ScopedLocalRef.h \
	java/native/JniConstants.cpp \
	java/native/JniConstants.h
libcephfs_jni_la_LIBADD = $(LIBCEPHFS) $(EXTRALIBS)
libcephfs_jni_la_CPPFLAGS = $(JDK_CPPFLAGS) $(AM_CPPFLAGS)
libcephfs_jni_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
lib_LTLIBRARIES += libcephfs_jni.la
endif


# shell scripts

editpaths = sed \
	-e 's|@bindir[@]|$(bindir)|g' \
	-e 's|@sbindir[@]|$(sbindir)|g' \
	-e 's|@libdir[@]|$(libdir)|g' \
	-e 's|@sysconfdir[@]|$(sysconfdir)|g' \
	-e 's|@datadir[@]|$(pkgdatadir)|g' \
	-e 's|@prefix[@]|$(prefix)|g' \
	-e 's|@@GCOV_PREFIX_STRIP[@][@]|$(GCOV_PREFIX_STRIP)|g'
shell_scripts = ceph-debugpack ceph-post-file ceph-crush-location
$(shell_scripts): Makefile
$(shell_scripts): %: %.in
	rm -f $@ $@.tmp
	$(editpaths) '$(srcdir)/$@.in' >$@.tmp
	chmod +x $@.tmp
	chmod a-w $@.tmp
	mv $@.tmp $@

EXTRA_DIST += $(srcdir)/$(shell_scripts:%=%.in)
CLEANFILES += $(shell_scripts)


# extra bits

EXTRA_DIST += \
	$(srcdir)/verify-mds-journal.sh \
	$(srcdir)/vstart.sh \
	$(srcdir)/stop.sh \
	ceph-run \
	$(srcdir)/ceph-osd-prestart.sh \
	$(srcdir)/ceph_common.sh \
	$(srcdir)/init-radosgw \
	$(srcdir)/init-radosgw.sysv \
	$(srcdir)/init-rbdmap \
	$(srcdir)/ceph-clsinfo \
	$(srcdir)/make_version \
	$(srcdir)/check_version \
	$(srcdir)/.git_version \
	$(srcdir)/ceph-rbdnamer \
	$(srcdir)/test/encoding/readable.sh \
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
	$(srcdir)/upstart/rbdmap.conf \
	ceph.in \
	ceph-disk \
	ceph-disk-prepare \
	ceph-disk-activate \
	ceph-disk-udev \
	ceph-create-keys \
	ceph-rest-api \
	ceph-crush-location \
	mount.fuse.ceph \
	rbd-replay-many \
	rbdmap \
	yasm-wrapper

EXTRA_DIST += \
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

if WITH_SLIBROCKSDB
  SUBDIRS += rocksdb
else
  EXTRA_DIST += \
        rocksdb/.arcconfig \
        rocksdb/.clang-format \
        rocksdb/.gitignore \
        rocksdb/CONTRIBUTING.md \
        rocksdb/HISTORY.md \
        rocksdb/INSTALL.md \
        rocksdb/LICENSE \
        rocksdb/Makefile.am \
        rocksdb/PATENTS \
        rocksdb/README \
        rocksdb/ROCKSDB_LITE.md \
        rocksdb/build_tools/build_detect_platform \
        rocksdb/build_tools/build_detect_version \
        rocksdb/build_tools/fbcode.clang31.sh \
        rocksdb/build_tools/fbcode.gcc471.sh \
        rocksdb/build_tools/fbcode.gcc481.sh \
        rocksdb/build_tools/format-diff.sh \
        rocksdb/build_tools/mac-install-gflags.sh \
        rocksdb/build_tools/make_new_version.sh \
        rocksdb/build_tools/regression_build_test.sh \
        rocksdb/build_tools/valgrind_test.sh \
        rocksdb/configure.ac \
        rocksdb/coverage/coverage_test.sh \
        rocksdb/coverage/parse_gcov_output.py \
        rocksdb/db/builder.cc \
        rocksdb/db/builder.h \
        rocksdb/db/c.cc \
        rocksdb/db/c_test.c \
        rocksdb/db/column_family.cc \
        rocksdb/db/column_family.h \
        rocksdb/db/column_family_test.cc \
        rocksdb/db/compaction.cc \
        rocksdb/db/compaction.h \
        rocksdb/db/compaction_picker.cc \
        rocksdb/db/compaction_picker.h \
        rocksdb/db/corruption_test.cc \
        rocksdb/db/db_bench.cc \
        rocksdb/db/db_filesnapshot.cc \
        rocksdb/db/db_impl.cc \
        rocksdb/db/db_impl.h \
        rocksdb/db/db_impl_debug.cc \
        rocksdb/db/db_impl_readonly.cc \
        rocksdb/db/db_impl_readonly.h \
        rocksdb/db/db_iter.cc \
        rocksdb/db/db_iter.h \
        rocksdb/db/db_stats_logger.cc \
        rocksdb/db/db_test.cc \
        rocksdb/db/dbformat.cc \
        rocksdb/db/dbformat.h \
        rocksdb/db/dbformat_test.cc \
        rocksdb/db/deletefile_test.cc \
        rocksdb/db/file_indexer.cc \
        rocksdb/db/file_indexer.h \
        rocksdb/db/file_indexer_test.cc \
        rocksdb/db/filename.cc \
        rocksdb/db/filename.h \
        rocksdb/db/filename_test.cc \
        rocksdb/db/internal_stats.cc \
        rocksdb/db/internal_stats.h \
        rocksdb/db/log_and_apply_bench.cc \
        rocksdb/db/log_format.h \
        rocksdb/db/log_reader.cc \
        rocksdb/db/log_reader.h \
        rocksdb/db/log_test.cc \
        rocksdb/db/log_writer.cc \
        rocksdb/db/log_writer.h \
        rocksdb/db/memtable.cc \
        rocksdb/db/memtable.h \
        rocksdb/db/memtable_list.cc \
        rocksdb/db/memtable_list.h \
        rocksdb/db/merge_context.h \
        rocksdb/db/merge_helper.cc \
        rocksdb/db/merge_helper.h \
        rocksdb/db/merge_operator.cc \
        rocksdb/db/merge_test.cc \
        rocksdb/db/perf_context_test.cc \
        rocksdb/db/plain_table_db_test.cc \
        rocksdb/db/prefix_test.cc \
        rocksdb/db/repair.cc \
        rocksdb/db/simple_table_db_test.cc \
        rocksdb/db/skiplist.h \
        rocksdb/db/skiplist_test.cc \
        rocksdb/db/snapshot.h \
        rocksdb/db/table_cache.cc \
        rocksdb/db/table_cache.h \
        rocksdb/db/table_properties_collector.cc \
        rocksdb/db/table_properties_collector.h \
        rocksdb/db/table_properties_collector_test.cc \
        rocksdb/db/tailing_iter.cc \
        rocksdb/db/tailing_iter.h \
        rocksdb/db/transaction_log_impl.cc \
        rocksdb/db/transaction_log_impl.h \
        rocksdb/db/version_edit.cc \
        rocksdb/db/version_edit.h \
        rocksdb/db/version_edit_test.cc \
        rocksdb/db/version_set.cc \
        rocksdb/db/version_set.h \
        rocksdb/db/version_set_test.cc \
        rocksdb/db/write_batch.cc \
        rocksdb/db/write_batch_internal.h \
        rocksdb/db/write_batch_test.cc \
        rocksdb/doc/doc.css \
        rocksdb/doc/index.html \
        rocksdb/doc/log_format.txt \
        rocksdb/doc/rockslogo.jpg \
        rocksdb/doc/rockslogo.png \
        rocksdb/hdfs/README \
        rocksdb/hdfs/env_hdfs.h \
        rocksdb/hdfs/hdfs.h \
        rocksdb/hdfs/libhdfs.a \
        rocksdb/helpers/memenv/memenv.cc \
        rocksdb/helpers/memenv/memenv_test.cc \
        rocksdb/include/rocksdb/c.h \
        rocksdb/include/rocksdb/cache.h \
        rocksdb/include/rocksdb/compaction_filter.h \
        rocksdb/include/rocksdb/comparator.h \
        rocksdb/include/rocksdb/db.h \
        rocksdb/include/rocksdb/env.h \
        rocksdb/include/rocksdb/filter_policy.h \
        rocksdb/include/rocksdb/flush_block_policy.h \
        rocksdb/include/rocksdb/iterator.h \
        rocksdb/include/rocksdb/ldb_tool.h \
        rocksdb/include/rocksdb/memtablerep.h \
        rocksdb/include/rocksdb/merge_operator.h \
        rocksdb/include/rocksdb/options.h \
        rocksdb/include/rocksdb/perf_context.h \
        rocksdb/include/rocksdb/slice.h \
        rocksdb/include/rocksdb/slice_transform.h \
        rocksdb/include/rocksdb/statistics.h \
        rocksdb/include/rocksdb/status.h \
        rocksdb/include/rocksdb/table.h \
        rocksdb/include/rocksdb/table_properties.h \
        rocksdb/include/rocksdb/transaction_log.h \
        rocksdb/include/rocksdb/types.h \
        rocksdb/include/rocksdb/universal_compaction.h \
        rocksdb/include/rocksdb/version.h \
        rocksdb/include/rocksdb/write_batch.h \
        rocksdb/include/utilities/backupable_db.h \
        rocksdb/include/utilities/db_ttl.h \
        rocksdb/include/utilities/geo_db.h \
        rocksdb/include/utilities/stackable_db.h \
        rocksdb/include/utilities/utility_db.h \
        rocksdb/java/Makefile \
        rocksdb/java/RocksDBSample.java \
        rocksdb/java/jdb_bench.sh \
        rocksdb/java/org/rocksdb/BackupableDB.java \
        rocksdb/java/org/rocksdb/BackupableDBOptions.java \
        rocksdb/java/org/rocksdb/BloomFilter.java \
        rocksdb/java/org/rocksdb/Filter.java \
        rocksdb/java/org/rocksdb/HashLinkedListMemTableConfig.java \
        rocksdb/java/org/rocksdb/HashSkipListMemTableConfig.java \
        rocksdb/java/org/rocksdb/HistogramData.java \
        rocksdb/java/org/rocksdb/HistogramType.java \
        rocksdb/java/org/rocksdb/Iterator.java \
        rocksdb/java/org/rocksdb/MemTableConfig.java \
        rocksdb/java/org/rocksdb/Options.java \
        rocksdb/java/org/rocksdb/PlainTableConfig.java \
        rocksdb/java/org/rocksdb/ReadOptions.java \
        rocksdb/java/org/rocksdb/RocksDB.java \
        rocksdb/java/org/rocksdb/RocksDBException.java \
        rocksdb/java/org/rocksdb/RocksObject.java \
        rocksdb/java/org/rocksdb/SkipListMemTableConfig.java \
        rocksdb/java/org/rocksdb/Statistics.java \
        rocksdb/java/org/rocksdb/TableFormatConfig.java \
        rocksdb/java/org/rocksdb/TickerType.java \
        rocksdb/java/org/rocksdb/VectorMemTableConfig.java \
        rocksdb/java/org/rocksdb/WriteBatch.java \
        rocksdb/java/org/rocksdb/WriteBatchTest.java \
        rocksdb/java/org/rocksdb/WriteOptions.java \
        rocksdb/java/org/rocksdb/benchmark/DbBenchmark.java \
        rocksdb/java/org/rocksdb/test/BackupableDBTest.java \
        rocksdb/java/org/rocksdb/test/OptionsTest.java \
        rocksdb/java/org/rocksdb/test/ReadOptionsTest.java \
        rocksdb/java/org/rocksdb/util/Environment.java \
        rocksdb/java/org/rocksdb/util/SizeUnit.java \
        rocksdb/java/rocksjni/backupablejni.cc \
        rocksdb/java/rocksjni/filter.cc \
        rocksdb/java/rocksjni/iterator.cc \
        rocksdb/java/rocksjni/memtablejni.cc \
        rocksdb/java/rocksjni/options.cc \
        rocksdb/java/rocksjni/portal.h \
        rocksdb/java/rocksjni/rocksjni.cc \
        rocksdb/java/rocksjni/statistics.cc \
        rocksdb/java/rocksjni/table.cc \
        rocksdb/java/rocksjni/write_batch.cc \
        rocksdb/linters/__phutil_library_init__.php \
        rocksdb/linters/__phutil_library_map__.php \
        rocksdb/linters/cpp_linter/ArcanistCpplintLinter.php \
        rocksdb/linters/cpp_linter/FbcodeCppLinter.php \
        rocksdb/linters/cpp_linter/PfffCppLinter.php \
        rocksdb/linters/cpp_linter/cpplint.py \
        rocksdb/linters/lint_engine/FacebookFbcodeLintEngine.php \
        rocksdb/m4/libtool.m4 \
        rocksdb/m4/ltoptions.m4 \
        rocksdb/m4/ltsugar.m4 \
        rocksdb/m4/ltversion.m4 \
        rocksdb/m4/lt~obsolete.m4 \
        rocksdb/port/README \
        rocksdb/port/atomic_pointer.h \
        rocksdb/port/likely.h \
        rocksdb/port/port.h \
        rocksdb/port/port_example.h \
        rocksdb/port/port_posix.cc \
        rocksdb/port/port_posix.h \
        rocksdb/port/stack_trace.cc \
        rocksdb/port/stack_trace.h \
        rocksdb/port/win/stdint.h \
        rocksdb/table/block.cc \
        rocksdb/table/block.h \
        rocksdb/table/block_based_table_builder.cc \
        rocksdb/table/block_based_table_builder.h \
        rocksdb/table/block_based_table_factory.cc \
        rocksdb/table/block_based_table_factory.h \
        rocksdb/table/block_based_table_reader.cc \
        rocksdb/table/block_based_table_reader.h \
        rocksdb/table/block_builder.cc \
        rocksdb/table/block_builder.h \
        rocksdb/table/block_hash_index.cc \
        rocksdb/table/block_hash_index.h \
        rocksdb/table/block_hash_index_test.cc \
        rocksdb/table/block_test.cc \
        rocksdb/table/filter_block.cc \
        rocksdb/table/filter_block.h \
        rocksdb/table/filter_block_test.cc \
        rocksdb/table/flush_block_policy.cc \
        rocksdb/table/format.cc \
        rocksdb/table/format.h \
        rocksdb/table/iter_heap.h \
        rocksdb/table/iterator.cc \
        rocksdb/table/iterator_wrapper.h \
        rocksdb/table/merger.cc \
        rocksdb/table/merger.h \
        rocksdb/table/meta_blocks.cc \
        rocksdb/table/meta_blocks.h \
        rocksdb/table/plain_table_builder.cc \
        rocksdb/table/plain_table_builder.h \
        rocksdb/table/plain_table_factory.cc \
        rocksdb/table/plain_table_factory.h \
        rocksdb/table/plain_table_reader.cc \
        rocksdb/table/plain_table_reader.h \
        rocksdb/table/table_builder.h \
        rocksdb/table/table_properties.cc \
        rocksdb/table/table_reader.h \
        rocksdb/table/table_reader_bench.cc \
        rocksdb/table/table_test.cc \
        rocksdb/table/two_level_iterator.cc \
        rocksdb/table/two_level_iterator.h \
        rocksdb/tools/auto_sanity_test.sh \
        rocksdb/tools/blob_store_bench.cc \
        rocksdb/tools/db_crashtest.py \
        rocksdb/tools/db_crashtest2.py \
        rocksdb/tools/db_repl_stress.cc \
        rocksdb/tools/db_sanity_test.cc \
        rocksdb/tools/db_stress.cc \
        rocksdb/tools/ldb.cc \
        rocksdb/tools/ldb_test.py \
        rocksdb/tools/reduce_levels_test.cc \
        rocksdb/tools/sst_dump.cc \
        rocksdb/util/arena.cc \
        rocksdb/util/arena.h \
        rocksdb/util/arena_test.cc \
        rocksdb/util/auto_roll_logger.cc \
        rocksdb/util/auto_roll_logger.h \
        rocksdb/util/auto_roll_logger_test.cc \
        rocksdb/util/autovector.h \
        rocksdb/util/autovector_test.cc \
        rocksdb/util/benchharness.cc \
        rocksdb/util/benchharness.h \
        rocksdb/util/benchharness_test.cc \
        rocksdb/util/blob_store.cc \
        rocksdb/util/blob_store.h \
        rocksdb/util/blob_store_test.cc \
        rocksdb/util/bloom.cc \
        rocksdb/util/bloom_test.cc \
        rocksdb/util/build_version.h \
        rocksdb/util/cache.cc \
        rocksdb/util/cache_test.cc \
        rocksdb/util/coding.cc \
        rocksdb/util/coding.h \
        rocksdb/util/coding_test.cc \
        rocksdb/util/comparator.cc \
        rocksdb/util/crc32c.cc \
        rocksdb/util/crc32c.h \
        rocksdb/util/crc32c_test.cc \
        rocksdb/util/dynamic_bloom.cc \
        rocksdb/util/dynamic_bloom.h \
        rocksdb/util/dynamic_bloom_test.cc \
        rocksdb/util/env.cc \
        rocksdb/util/env_hdfs.cc \
        rocksdb/util/env_posix.cc \
        rocksdb/util/env_test.cc \
        rocksdb/util/filelock_test.cc \
        rocksdb/util/filter_policy.cc \
        rocksdb/util/hash.cc \
        rocksdb/util/hash.h \
        rocksdb/util/hash_cuckoo_rep.cc \
        rocksdb/util/hash_cuckoo_rep.h \
        rocksdb/util/hash_linklist_rep.cc \
        rocksdb/util/hash_linklist_rep.h \
        rocksdb/util/hash_skiplist_rep.cc \
        rocksdb/util/hash_skiplist_rep.h \
        rocksdb/util/histogram.cc \
        rocksdb/util/histogram.h \
        rocksdb/util/histogram_test.cc \
        rocksdb/util/ldb_cmd.cc \
        rocksdb/util/ldb_cmd.h \
        rocksdb/util/ldb_cmd_execute_result.h \
        rocksdb/util/ldb_tool.cc \
        rocksdb/util/log_buffer.cc \
        rocksdb/util/log_buffer.h \
        rocksdb/util/log_write_bench.cc \
        rocksdb/util/logging.cc \
        rocksdb/util/logging.h \
        rocksdb/util/manual_compaction_test.cc \
        rocksdb/util/murmurhash.cc \
        rocksdb/util/murmurhash.h \
        rocksdb/util/mutexlock.h \
        rocksdb/util/options.cc \
        rocksdb/util/perf_context.cc \
        rocksdb/util/perf_context_imp.h \
        rocksdb/util/posix_logger.h \
        rocksdb/util/random.h \
        rocksdb/util/signal_test.cc \
        rocksdb/util/skiplistrep.cc \
        rocksdb/util/slice.cc \
        rocksdb/util/statistics.cc \
        rocksdb/util/statistics.h \
        rocksdb/util/stats_logger.h \
        rocksdb/util/status.cc \
        rocksdb/util/stl_wrappers.h \
        rocksdb/util/stop_watch.h \
        rocksdb/util/string_util.cc \
        rocksdb/util/string_util.h \
        rocksdb/util/sync_point.cc \
        rocksdb/util/sync_point.h \
        rocksdb/util/testharness.cc \
        rocksdb/util/testharness.h \
        rocksdb/util/testutil.cc \
        rocksdb/util/testutil.h \
        rocksdb/util/thread_local.cc \
        rocksdb/util/thread_local.h \
        rocksdb/util/thread_local_test.cc \
        rocksdb/util/vectorrep.cc \
        rocksdb/util/xxhash.cc \
        rocksdb/util/xxhash.h \
        rocksdb/utilities/backupable/backupable_db.cc \
        rocksdb/utilities/backupable/backupable_db_test.cc \
        rocksdb/utilities/geodb/geodb_impl.cc \
        rocksdb/utilities/geodb/geodb_impl.h \
        rocksdb/utilities/geodb/geodb_test.cc \
        rocksdb/utilities/merge_operators.h \
        rocksdb/utilities/merge_operators/put.cc \
        rocksdb/utilities/merge_operators/string_append/stringappend.cc \
        rocksdb/utilities/merge_operators/string_append/stringappend.h \
        rocksdb/utilities/merge_operators/string_append/stringappend2.cc \
        rocksdb/utilities/merge_operators/string_append/stringappend2.h \
        rocksdb/utilities/merge_operators/string_append/stringappend_test.cc \
        rocksdb/utilities/merge_operators/uint64add.cc \
        rocksdb/utilities/redis/README \
        rocksdb/utilities/redis/redis_list_exception.h \
        rocksdb/utilities/redis/redis_list_iterator.h \
        rocksdb/utilities/redis/redis_lists.cc \
        rocksdb/utilities/redis/redis_lists.h \
        rocksdb/utilities/redis/redis_lists_test.cc \
        rocksdb/utilities/ttl/db_ttl_impl.cc \
        rocksdb/utilities/ttl/db_ttl_impl.h \
        rocksdb/utilities/ttl/ttl_test.cc
endif


# work around old versions of automake that don't define $docdir
# NOTE: this won't work on suse, where docdir is /usr/share/doc/packages/$package.
docdir ?= ${datadir}/doc/ceph
doc_DATA = $(srcdir)/sample.ceph.conf sample.fetch_config


# various scripts

shell_commondir = $(libdir)/ceph
shell_common_SCRIPTS = ceph_common.sh

ceph_libexecdir = $(libexecdir)/ceph
ceph_libexec_SCRIPTS = ceph-osd-prestart.sh

bash_completiondir = $(sysconfdir)/bash_completion.d
bash_completion_DATA = $(srcdir)/bash_completion/ceph \
               $(srcdir)/bash_completion/rados \
               $(srcdir)/bash_completion/rbd \
               $(srcdir)/bash_completion/radosgw-admin

ceph_sbin_SCRIPTS = \
	ceph-disk \
	ceph-disk-prepare \
	ceph-disk-activate \
	ceph-disk-udev \
	ceph-create-keys

bin_SCRIPTS += \
	ceph \
	ceph-run \
	ceph-rest-api \
	ceph-clsinfo \
	ceph-debugpack \
	ceph-rbdnamer \
	ceph-post-file \
	ceph-crush-location \
	rbd-replay-many

BUILT_SOURCES += init-ceph

shell_scripts += init-ceph





# tests to actually run on "make check"; if you need extra, non-test,
# executables built, you need to replace this with manual assignments
# target by target

TESTS = \
	$(check_PROGRAMS) \
	$(check_SCRIPTS)

check-local:
	$(top_srcdir)/qa/workunits/erasure-code/encode-decode-non-regression.sh 
	$(srcdir)/test/encoding/readable.sh ../ceph-object-corpus

if WITH_LTTNG
# TODO: If we're running the parallel test harness (the preferred harness), this should be AM_TESTS_ENVIRONMENT instead.
# See: https://www.gnu.org/software/automake/manual/html_node/Scripts_002dbased-Testsuites.html
# I don't see the serial-tests Automake option anywhere, but my AM_TESTS_ENVIRONMENT was being ignored.
TESTS_ENVIRONMENT = LD_PRELOAD=liblttng-ust-fork.so; export LD_PRELOAD; echo "LD_PRELOAD=$${LD_PRELOAD}";
endif

# base targets

core-daemons: ceph-mon ceph-osd ceph-mds radosgw
admin-tools: monmaptool osdmaptool crushtool ceph-authtool
base: core-daemons admin-tools \
	cephfs ceph-syn ceph-conf \
	rados librados-config \
	init-ceph ceph_mon_store_converter ceph-post-file \
	ceph


# version stuff

FORCE:
.git_version: FORCE
	$(srcdir)/check_version $(srcdir)/.git_version

# if NO_VERSION is set, only generate a new ceph_ver.h if there currently 
# is none, and call "make_version -n" to fill it with a fixed string.
# Otherwise, set it from the contents of .git_version.

ceph_ver.h: .git_version
	if [ -n "$$NO_VERSION" ] ; then \
	    if [ ! -f ./ceph_ver.h ] ; then \
	        $(srcdir)/make_version -n ./ceph_ver.h ; \
	    fi; \
        else \
	    $(srcdir)/make_version $(srcdir)/.git_version ./ceph_ver.h ; \
	fi

ceph_ver.c: ./ceph_ver.h
common/version.cc: ./ceph_ver.h
test/encoding/ceph_dencoder.cc: ./ceph_ver.h

sample.fetch_config: fetch_config
	cp -f $(srcdir)/fetch_config ./sample.fetch_config

dist-hook:
	$(srcdir)/check_version $(srcdir)/.git_version

CLEANFILES += ceph_ver.h sample.fetch_config


# assemble Python script with global version variables
# NB: depends on format of ceph_ver.h

ceph: ceph.in ./ceph_ver.h Makefile
	rm -f $@ $@.tmp
	echo "#!/usr/bin/env python" >$@.tmp
	grep "#define CEPH_GIT_NICE_VER" ./ceph_ver.h | \
		sed -e 's/#define \(.*VER\) /\1=/' >>$@.tmp
	grep "#define CEPH_GIT_VER" ./ceph_ver.h | \
	  sed -e 's/#define \(.*VER\) /\1=/' -e 's/=\(.*\)$$/="\1"/' >>$@.tmp
	cat $(srcdir)/$@.in >>$@.tmp
	chmod a+x $@.tmp
	chmod a-w $@.tmp
	mv $@.tmp $@

# cleaning

clean-local:
	rm -f *.so 
	find . -name '*.gcno' -o -name '*.gcda' -o -name '*.lcov' -o -name "*.o" -o -name "*.lo" | xargs rm -f
	rm -f ceph java/java/com/ceph/crush/Bucket.class


# pybind

python_PYTHON = pybind/rados.py \
		pybind/rbd.py \
		pybind/cephfs.py \
		pybind/ceph_argparse.py \
		pybind/ceph_rest_api.py


# everything else we want to include in a 'make dist'

noinst_HEADERS += \
	cls_acl.cc\
	cls_crypto.cc\
	fetch_config\
	logrotate.conf\
	sample.ceph.conf\
	bash_completion/ceph \
	bash_completion/rados \
	bash_completion/rbd \
	bash_completion/radosgw-admin \
	mount/canonicalize.c \
	mount/mtab.c \
	objclass/objclass.h


# coverage

shell_scripts += ceph-coverage
bin_SCRIPTS += ceph-coverage


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
