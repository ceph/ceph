AUTOMAKE_OPTIONS = gnu
SUBDIRS =
DIST_SUBDIRS = gtest
CLEANFILES =
bin_PROGRAMS =
# like bin_PROGRAMS, but these targets are only built for debug builds
bin_DEBUGPROGRAMS =
sbin_PROGRAMS =
sbin_SCRIPTS =
bin_SCRIPTS = ceph-run $(srcdir)/ceph-clsinfo ceph-debugpack ceph-rbdnamer
dist_bin_SCRIPTS =
# C/C++ tests to build will be appended to this
check_PROGRAMS =
# tests to actually run on "make check"; if you need extra, non-test,
# executables built, you need to replace this with manual assignments
# target by target
TESTS = $(check_PROGRAMS)


EXTRALIBS = -lrt
if WITH_PROFILER
EXTRALIBS += -lprofiler
endif

LIBGLOBAL_LDA = libglobal.la -lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS)

# monitor
ceph_mon_SOURCES = ceph_mon.cc
ceph_mon_LDFLAGS = $(AM_LDFLAGS)
ceph_mon_LDADD = libmon.la $(LIBGLOBAL_LDA)
ceph_mon_CXXFLAGS = ${AM_CXXFLAGS}
bin_PROGRAMS += ceph-mon

# osd
ceph_osd_SOURCES = ceph_osd.cc objclass/class_debug.cc \
	       objclass/class_api.cc
ceph_osd_LDADD = libosd.la libos.la $(LIBGLOBAL_LDA) -ldl
bin_PROGRAMS += ceph-osd
ceph_osd_CXXFLAGS = ${AM_CXXFLAGS}

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

if WITH_GTK2
ceph_tool_guidir = ${datadir}/ceph_tool/gui_resources/
ceph_tool_gui_DATA = \
	tools/gui_resources/blacklist.svg \
	tools/gui_resources/client.svg \
	tools/gui_resources/cluster_stats_window.glade \
	tools/gui_resources/down_osd.svg \
	tools/gui_resources/failed_mds.svg \
	tools/gui_resources/gui_monitor.build \
	tools/gui_resources/gui_monitor.glade \
	tools/gui_resources/gui_monitor_old.glade \
	tools/gui_resources/main-window.glade \
	tools/gui_resources/mds.svg \
	tools/gui_resources/monitor.svg \
	tools/gui_resources/node_stats_window.glade \
	tools/gui_resources/osd.svg \
	tools/gui_resources/out_osd.svg \
	tools/gui_resources/pg.svg \
	tools/gui_resources/stats_window.glade \
	tools/gui_resources/stopped_mds.svg
gceph_SOURCES = \
	tools/common.cc \
	mon/PGMap.cc \
	tools/gceph.cc \
	tools/gui.cc
gceph_LDADD = $(LIBGLOBAL_LDA) $(LIBEDIT_LIBS) $(GTKMM_LIBS)
gceph_CXXFLAGS = ${AM_CXXFLAGS} $(GTKMM_CFLAGS) \
	-DCEPH_TOOL_GUIDIR="\"${ceph_tool_guidir}\""
bin_PROGRAMS += gceph
endif

ceph_conf_SOURCES = ceph_conf.cc
ceph_conf_LDADD = $(LIBGLOBAL_LDA)
ceph_authtool_SOURCES = ceph_authtool.cc
ceph_authtool_LDADD = $(LIBGLOBAL_LDA)
bin_PROGRAMS += ceph ceph-conf ceph-authtool

monmaptool_SOURCES = monmaptool.cc
monmaptool_LDADD = $(LIBGLOBAL_LDA)
crushtool_SOURCES = crushtool.cc
crushtool_LDADD = $(LIBGLOBAL_LDA)
osdmaptool_SOURCES = osdmaptool.cc
osdmaptool_LDADD = $(LIBGLOBAL_LDA)
bin_PROGRAMS += monmaptool crushtool osdmaptool

mount_ceph_SOURCES = mount/mount.ceph.c common/armor.c common/safe_io.c common/secret.c include/addr_parsing.c
mount_ceph_LDADD = -lkeyutils
sbin_PROGRAMS += mount.ceph

# user tools
cephfs_SOURCES = cephfs.cc
bin_PROGRAMS += cephfs

librados_config_SOURCES = librados-config.cc
librados_config_LDADD = libglobal.la librados.la $(EXTRALIBS) $(CRYPTO_LIBS)
bin_PROGRAMS += librados-config

# synthetic client
ceph_syn_SOURCES = ceph_syn.cc client/SyntheticClient.cc
ceph_syn_LDADD = libclient.la libosdc.la $(LIBGLOBAL_LDA)
bin_PROGRAMS += ceph-syn

core: ceph-mon ceph-osd ceph-mds ceph cephfs librados-config ceph-conf monmaptool osdmaptool crushtool ceph-syn


# fuse targets?
if WITH_FUSE
ceph_fuse_SOURCES = ceph_fuse.cc client/fuse_ll.cc
ceph_fuse_LDADD = -lfuse libclient.la libosdc.la $(LIBGLOBAL_LDA)
ceph_fuse_CXXFLAGS = ${AM_CXXFLAGS}
bin_PROGRAMS += ceph-fuse

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
psim_SOURCES = psim.cc
psim_LDADD = $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += psim

test_addrs_SOURCES = test/test_addrs.cc
test_addrs_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
test_addrs_LDADD = libglobal.la -lpthread -lm ${UNITTEST_LDADD} $(CRYPTO_LIBS) $(EXTRALIBS)
bin_DEBUGPROGRAMS += test_addrs

test_mutate_SOURCES = test/test_mutate.cc
test_mutate_LDADD = libglobal.la librados.la -lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_DEBUGPROGRAMS += test_mutate

testmsgr_SOURCES = testmsgr.cc
testmsgr_LDADD = $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += testmsgr

test_ioctls_SOURCES = client/test_ioctls.c
bin_DEBUGPROGRAMS += test_ioctls

dupstore_SOURCES = dupstore.cc
dupstore_LDADD = libos.la $(LIBGLOBAL_LDA)
streamtest_SOURCES = streamtest.cc
streamtest_LDADD = libos.la $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += dupstore streamtest

test_trans_SOURCES = test_trans.cc
test_trans_LDADD = libos.la $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += test_trans

testsnaps_SOURCES = test/osd/TestSnaps.cc test/osd/TestOpStat.cc test/osd/Object.cc  test/osd/RadosModel.cc
testsnaps_LDADD = librados.la $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += testsnaps

if WITH_BUILD_TESTS
test_libcommon_build_SOURCES = test/test_libcommon_build.cc $(libcommon_files)
test_libcommon_build_LDADD = -lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_DEBUGPROGRAMS += test_libcommon_build

test_librados_build_SOURCES = test/test_libcommon_build.cc $(libcommon_files) $(librados_SOURCES)
test_librados_build_LDADD = -lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS)
test_librados_build_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += test_librados_build

test_librgw_build_SOURCES = test/test_libcommon_build.cc $(libcommon_files) \
			    $(librados_SOURCES) $(librgw_la_SOURCES)
test_librgw_build_LDADD = -lexpat -lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS)
test_librgw_build_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += test_librgw_build

test_libcephfs_build_SOURCES = test/test_libcommon_build.cc $(libcommon_files) \
			    $(libcephfs_la_SOURCES) $(libosdc_la_SOURCES)
test_libcephfs_build_LDADD = -lexpat -lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS)
test_libcephfs_build_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += test_libcephfs_build
endif

if WITH_HADOOPCLIENT
test_libhadoopcephfs_build_SOURCES = test/test_libcommon_build.cc \
         $(libhadoopcephfs_la_SOURCES) $(libcephfs_la_SOURCES) \
	 $(libosdc_la_SOURCES) $(libcommon_files)
test_libhadoopcephfs_build_LDADD = -lexpat -lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS)
test_libhadoopcephfs_build_CXXFLAGS = $(AM_CXXFLAGS)
bin_DEBUGPROGRAMS += test_libhadoopcephfs_build
endif

##########
BUILT_SOURCES =
lib_LTLIBRARIES = 
noinst_LTLIBRARIES = 
noinst_LIBRARIES =

# libcephfs
libcephfs_la_SOURCES = \
	libcephfs.cc \
	client/Client.cc \
	client/Inode.cc \
	client/MetaRequest.cc
libcephfs_la_CFLAGS= ${CRYPTO_CFLAGS} ${AM_CFLAGS}
libcephfs_la_CXXFLAGS= ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
libcephfs_la_LIBADD = libosdc.la
libcephfs_la_LDFLAGS =  -lpthread $(CRYPTO_LIBS) $(EXTRALIBS) \
            ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '^ceph_.*'
lib_LTLIBRARIES += libcephfs.la

testceph_SOURCES = client/testceph.cc
testceph_LDADD = libcephfs.la -lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_DEBUGPROGRAMS += testceph

testtimers_SOURCES = test/TestTimers.cc
testtimers_LDADD = $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += testtimers

testdout_streambuf_SOURCES = test/TestDoutStreambuf.cc
testdout_streambuf_LDADD = $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += testdout_streambuf

testsignal_handlers_SOURCES = test/TestSignalHandlers.cc
testsignal_handlers_LDADD = $(LIBGLOBAL_LDA)
bin_DEBUGPROGRAMS += testsignal_handlers

# librados
librados_SOURCES = \
	librados.cc \
	osdc/Objecter.cc
librados_la_SOURCES = ${librados_SOURCES}
librados_la_CFLAGS = ${CRYPTO_CFLAGS} ${AM_CFLAGS}
librados_la_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
librados_la_LIBADD = libcommon.la -lpthread $(CRYPTO_LIBS) $(EXTRALIBS)
librados_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0 -export-symbols-regex '^rados_.*'
lib_LTLIBRARIES += librados.la

# librgw
if WITH_RADOSGW
librgw_SOURCES = \
	$(my_libradosgw_src)
librgw_la_SOURCES = ${librgw_SOURCES}
librgw_la_CFLAGS = ${CRYPTO_CFLAGS} ${AM_CFLAGS}
librgw_la_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
librgw_la_LIBADD = librados.la
librgw_la_LDFLAGS = -version-info 1:0:0 -lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS) -lexpat -lcurl
lib_LTLIBRARIES += librgw.la
endif

# librbd
librbd_la_SOURCES = librbd.cc
librbd_la_CFLAGS = ${AM_CFLAGS}
librbd_la_CXXFLAGS = ${AM_CXXFLAGS}
librbd_la_LIBADD = librados.la 
librbd_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 \
	-export-symbols-regex '^rbd_.*' -lpthread $(EXTRALIBS) 
lib_LTLIBRARIES += librbd.la

rados_SOURCES = rados.cc rados_import.cc rados_export.cc rados_sync.cc
rados_LDADD = libglobal.la librados.la -lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_PROGRAMS += rados

scratchtool_SOURCES = scratchtool.c
scratchtool_LDADD = librados.la -lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS)
scratchtoolpp_SOURCES = scratchtoolpp.cc
scratchtoolpp_LDADD = librados.la -lpthread -lm
radosacl_SOURCES = radosacl.cc
radosacl_LDADD = librados.la -lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS)
bin_DEBUGPROGRAMS += scratchtool scratchtoolpp radosacl

rbd_SOURCES = rbd.cc common/fiemap.cc common/secret.c
rbd_CXXFLAGS = ${AM_CXXFLAGS}
rbd_LDADD = libglobal.la librbd.la librados.la -lpthread -lm -lkeyutils $(CRYPTO_LIBS) $(EXTRALIBS)
bin_PROGRAMS += rbd

if WITH_RADOSGW
my_libradosgw_src = \
	rgw/librgw.cc \
	rgw/rgw_acl.cc \
	rgw/rgw_xml.cc

my_radosgw_src = \
	rgw/rgw_fs.cc \
	rgw/rgw_rados.cc \
	rgw/rgw_tools.cc \
	rgw/rgw_user.cc \
	rgw/rgw_access.cc \
	rgw/rgw_op.cc \
	rgw/rgw_rest.cc \
	rgw/rgw_rest_swift.cc \
	rgw/rgw_rest_s3.cc \
	rgw/rgw_common.cc \
	rgw/rgw_cache.cc \
	rgw/rgw_swift.cc \
	rgw/rgw_swift_auth.cc \
	rgw/rgw_formats.cc \
	rgw/rgw_log.cc \
	rgw/rgw_multi.cc \
	rgw/rgw_env.cc

my_radosgw_ldadd = \
	libglobal.la librgw.la librados.la -lfcgi -lcurl -lexpat \
	-lpthread -lm $(CRYPTO_LIBS) $(EXTRALIBS)

radosgw_SOURCES = $(my_radosgw_src) rgw/rgw_main.cc
radosgw_LDADD = $(my_radosgw_ldadd)
radosgw_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
radosgw_admin_SOURCES = $(my_radosgw_src) rgw/rgw_admin.cc
radosgw_admin_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
radosgw_admin_LDADD = $(my_radosgw_ldadd)
bin_PROGRAMS += radosgw radosgw-admin

rgw_multiparser_SOURCES = $(my_radosgw_src) rgw/rgw_multiparser.cc
rgw_multiparser_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
rgw_multiparser_LDADD = $(my_radosgw_ldadd)
bin_DEBUGPROGRAMS += rgw_multiparser
endif

testcrypto_SOURCES = testcrypto.cc
testcrypto_LDADD =  $(LIBGLOBAL_LDA)
testcrypto_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
bin_DEBUGPROGRAMS += testcrypto

testkeys_SOURCES = testkeys.cc
testkeys_LDADD = libmon.la $(LIBGLOBAL_LDA) 
testkeys_CXXFLAGS = ${AM_CXXFLAGS}
bin_DEBUGPROGRAMS += testkeys


## rados object classes

# rbd: rados block device class
libcls_rbd_la_SOURCES = cls_rbd.cc
libcls_rbd_la_CFLAGS = ${AM_CFLAGS}
libcls_rbd_la_CXXFLAGS= ${AM_CXXFLAGS}
libcls_rbd_la_LIBADD = -lpthread $(EXTRALIBS)
libcls_rbd_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__cls_.*'

radoslibdir = $(libdir)/rados-classes
radoslib_LTLIBRARIES = libcls_rbd.la


if WITH_RADOSGW
# rgw: rados gateway
libcls_rgw_la_SOURCES = cls_rgw.cc
libcls_rgw_la_CFLAGS = ${AM_CFLAGS}
libcls_rgw_la_CXXFLAGS= ${AM_CXXFLAGS}
libcls_rgw_la_LIBADD = -lpthread $(EXTRALIBS)
libcls_rgw_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '.*__cls_.*'

radoslib_LTLIBRARIES += libcls_rgw.la
endif

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

## System tests
libsystest_la_SOURCES = \
	test/system/cross_process_sem.cc \
	test/system/systest_runnable.cc \
	test/system/systest_settings.cc
libsystest_la_LIBADD = libglobal.la
noinst_LTLIBRARIES += libsystest.la

testrados_list_parallel_SOURCES = \
	test/system/rados_list_parallel.cc \
	test/system/st_rados_create_pool.cc \
	test/system/st_rados_list_objects.cc
testrados_list_parallel_LDADD = libsystest.la librados.la
bin_DEBUGPROGRAMS += testrados_list_parallel

testrados_open_pools_parallel_SOURCES = \
	test/system/rados_open_pools_parallel.cc \
	test/system/st_rados_create_pool.cc
testrados_open_pools_parallel_LDADD = libsystest.la librados.la
bin_DEBUGPROGRAMS += testrados_open_pools_parallel

testrados_delete_pools_parallel_SOURCES = \
	test/system/rados_delete_pools_parallel.cc \
	test/system/st_rados_create_pool.cc \
	test/system/st_rados_delete_pool.cc \
	test/system/st_rados_list_objects.cc
testrados_delete_pools_parallel_LDADD = libsystest.la librados.la
bin_DEBUGPROGRAMS += testrados_delete_pools_parallel

testrados_watch_notify_SOURCES = \
	test/system/rados_watch_notify.cc \
	test/system/st_rados_create_pool.cc \
	test/system/st_rados_delete_pool.cc \
	test/system/st_rados_delete_objs.cc \
	test/system/st_rados_watch.cc \
	test/system/st_rados_notify.cc
testrados_watch_notify_LDADD = libsystest.la librados.la
bin_DEBUGPROGRAMS += testrados_watch_notify

## unit tests

# target to build but not run the unit tests
unittests:: $(check_PROGRAMS)

UNITTEST_CXXFLAGS = \
	-I$(top_srcdir)/src/gtest/include \
	-I$(top_builddir)/src/gtest/include
UNITTEST_LDADD = \
	$(top_builddir)/src/gtest/lib/libgtest.la \
	$(top_builddir)/src/gtest/lib/libgtest_main.la \
	-lpthread
UNITTEST_STATIC_LDADD = \
	$(top_builddir)/src/gtest/lib/libgtest.a \
	$(top_builddir)/src/gtest/lib/libgtest_main.a \
	-lpthread

unittest_encoding_SOURCES = test/encoding.cc
unittest_encoding_LDADD = libcephfs.la -lpthread -lm \
			  ${UNITTEST_LDADD}
unittest_encoding_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS} \
		     -fno-strict-aliasing
check_PROGRAMS += unittest_encoding

unittest_base64_SOURCES = test/base64.cc
unittest_base64_LDFLAGS = -pthread ${AM_LDFLAGS}
unittest_base64_LDADD = libcephfs.la -lm ${UNITTEST_LDADD}
unittest_base64_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_base64

unittest_ceph_argparse_SOURCES = test/ceph_argparse.cc
unittest_ceph_argparse_LDFLAGS = -pthread ${AM_LDFLAGS}
unittest_ceph_argparse_LDADD = libglobal.la ${UNITTEST_LDADD}
unittest_ceph_argparse_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_ceph_argparse

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

unittest_librados_SOURCES = test/librados.cc
unittest_librados_LDFLAGS = -pthread ${AM_LDFLAGS}
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
unittest_crypto_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_crypto

unittest_perf_counters_SOURCES = test/perf_counters.cc
unittest_perf_counters_LDFLAGS = ${AM_LDFLAGS}
unittest_perf_counters_LDADD =  ${LIBGLOBAL_LDA} ${UNITTEST_LDADD}
unittest_perf_counters_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_perf_counters

unittest_admin_socket_SOURCES = test/admin_socket.cc
unittest_admin_socket_LDFLAGS = ${AM_LDFLAGS}
unittest_admin_socket_LDADD =  ${LIBGLOBAL_LDA} ${UNITTEST_LDADD}
unittest_admin_socket_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_admin_socket

unittest_ceph_crypto_SOURCES = test/ceph_crypto.cc
unittest_ceph_crypto_LDFLAGS = ${CRYPTO_LDFLAGS} ${AM_LDFLAGS}
unittest_ceph_crypto_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_ceph_crypto_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_ceph_crypto

unittest_utf8_SOURCES = test/utf8.cc
unittest_utf8_LDFLAGS = -pthread ${AM_LDFLAGS}
unittest_utf8_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_utf8_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_utf8

unittest_mime_SOURCES = test/mime.cc
unittest_mime_LDFLAGS = -pthread ${AM_LDFLAGS}
unittest_mime_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_mime_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_mime

unittest_escape_SOURCES = test/escape.cc
unittest_escape_LDFLAGS = -pthread ${AM_LDFLAGS}
unittest_escape_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_escape_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_escape

unittest_strtol_SOURCES = test/strtol.cc
unittest_strtol_LDFLAGS = -pthread ${AM_LDFLAGS}
unittest_strtol_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_strtol_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_strtol

unittest_confutils_SOURCES = test/confutils.cc
unittest_confutils_LDFLAGS = -pthread ${AM_LDFLAGS}
unittest_confutils_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_confutils_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_confutils

unittest_heartbeatmap_SOURCES = test/heartbeat_map.cc common/HeartbeatMap.cc
unittest_heartbeatmap_LDFLAGS = -pthread ${AM_LDFLAGS}
unittest_heartbeatmap_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_heartbeatmap_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_heartbeatmap

unittest_formatter_SOURCES = test/formatter.cc rgw/rgw_formats.cc
unittest_formatter_LDFLAGS = -pthread ${AM_LDFLAGS}
unittest_formatter_LDADD = ${UNITTEST_LDADD} $(LIBGLOBAL_LDA)
unittest_formatter_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_formatter

unittest_libcephfs_config_SOURCES = test/libcephfs_config.cc
unittest_libcephfs_config_LDFLAGS = -pthread ${AM_LDFLAGS}
unittest_libcephfs_config_LDADD =  libcephfs.la ${UNITTEST_LDADD}
unittest_libcephfs_config_CXXFLAGS = ${CRYPTO_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_libcephfs_config

unittest_librados_config_SOURCES = test/librados_config.cc
unittest_librados_config_LDFLAGS = -pthread ${AM_LDFLAGS}
unittest_librados_config_LDADD =  librados.la ${UNITTEST_LDADD}
unittest_librados_config_CXXFLAGS = ${CRYPTO_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_librados_config

unittest_daemon_config_SOURCES = test/daemon_config.cc
unittest_daemon_config_LDFLAGS = -pthread ${AM_LDFLAGS}
unittest_daemon_config_LDADD =  ${UNITTEST_LDADD} ${LIBGLOBAL_LDA}
unittest_daemon_config_CXXFLAGS = ${CRYPTO_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_daemon_config

if WITH_RADOSGW
unittest_librgw_SOURCES = test/librgw.cc
unittest_librgw_LDFLAGS = -lrt -pthread -lcurl ${AM_LDFLAGS}
unittest_librgw_LDADD =  librgw.la ${UNITTEST_LDADD} -lexpat -lfcgi $(LIBGLOBAL_LDA)
unittest_librgw_CXXFLAGS = ${CRYPTO_CFLAGS} ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
check_PROGRAMS += unittest_librgw
endif

test_librbd_SOURCES = test/test_librbd.cc
test_librbd_LDADD =  librbd.la librados.la ${UNITTEST_STATIC_LDADD}
test_librbd_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += test_librbd

test_rados_api_io_SOURCES = test/rados-api/io.cc test/rados-api/test.cc
test_rados_api_io_LDFLAGS = ${AM_LDFLAGS}
test_rados_api_io_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
test_rados_api_io_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += test_rados_api_io

test_rados_api_aio_SOURCES = test/rados-api/aio.cc test/rados-api/test.cc
test_rados_api_aio_LDFLAGS = ${AM_LDFLAGS}
test_rados_api_aio_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
test_rados_api_aio_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += test_rados_api_aio

test_rados_api_list_SOURCES = test/rados-api/list.cc test/rados-api/test.cc
test_rados_api_list_LDFLAGS = ${AM_LDFLAGS}
test_rados_api_list_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
test_rados_api_list_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += test_rados_api_list

test_rados_api_pool_SOURCES = test/rados-api/pool.cc test/rados-api/test.cc
test_rados_api_pool_LDFLAGS = ${AM_LDFLAGS}
test_rados_api_pool_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
test_rados_api_pool_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += test_rados_api_pool

test_rados_api_stat_SOURCES = test/rados-api/stat.cc test/rados-api/test.cc
test_rados_api_stat_LDFLAGS = ${AM_LDFLAGS}
test_rados_api_stat_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
test_rados_api_stat_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += test_rados_api_stat

test_rados_api_watch_notify_SOURCES = test/rados-api/watch_notify.cc test/rados-api/test.cc
test_rados_api_watch_notify_LDFLAGS = ${AM_LDFLAGS}
test_rados_api_watch_notify_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
test_rados_api_watch_notify_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += test_rados_api_watch_notify

test_rados_api_snapshots_SOURCES = test/rados-api/snapshots.cc test/rados-api/test.cc
test_rados_api_snapshots_LDFLAGS = ${AM_LDFLAGS}
test_rados_api_snapshots_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
test_rados_api_snapshots_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += test_rados_api_snapshots

test_rados_api_misc_SOURCES = test/rados-api/misc.cc test/rados-api/test.cc
test_rados_api_misc_LDFLAGS = ${AM_LDFLAGS}
test_rados_api_misc_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
test_rados_api_misc_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += test_rados_api_misc

test_store_SOURCES = test/store_test.cc
test_store_LDFLAGS = ${AM_LDFLAGS}
test_store_LDADD =  ${UNITTEST_STATIC_LDADD} libos.la $(LIBGLOBAL_LDA)
test_store_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += test_store

test_stress_watch_SOURCES = test/test_stress_watch.cc test/rados-api/test.cc
test_stress_watch_LDFLAGS = ${AM_LDFLAGS}
test_stress_watch_LDADD =  librados.la ${UNITTEST_STATIC_LDADD}
test_stress_watch_CXXFLAGS = ${AM_CXXFLAGS} ${UNITTEST_CXXFLAGS}
bin_DEBUGPROGRAMS += test_stress_watch

# shell scripts
editpaths = sed \
	-e 's|@bindir[@]|$(bindir)|g' \
	-e 's|@libdir[@]|$(libdir)|g' \
	-e 's|@sysconfdir[@]|$(sysconfdir)|g' \
	-e 's|@datadir[@]|$(pkgdatadir)|g' \
	-e 's|@prefix[@]|$(prefix)|g' \
	-e 's|@@GCOV_PREFIX_STRIP[@][@]|$(GCOV_PREFIX_STRIP)|g'

init-ceph mkcephfs ceph-debugpack ceph-coverage: init-ceph.in mkcephfs.in Makefile ceph-debugpack.in ceph-coverage.in
	rm -f $@ $@.tmp
	$(editpaths) '$(srcdir)/$@.in' >$@.tmp
	chmod +x $@.tmp
	chmod a-w $@.tmp
	mv $@.tmp $@

BUILT_SOURCES += init-ceph
sbin_SCRIPTS += mkcephfs

bin_SCRIPTS += obsync/obsync
bin_SCRIPTS += obsync/boto_tool
bin_SCRIPTS += ceph-coverage

CLEANFILES += \
	ceph-debugpack \
	ceph_ver.h \
	init-ceph \
	mkcephfs \
	sample.fetch_config \
	ceph-coverage

##

AM_COMMON_FLAGS = -Wall -D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT \
-D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE -rdynamic \
${WARN_TYPE_LIMITS} ${WARN_IGNORED_QUALIFIERS} -Winit-self -Wpointer-arith \
-fno-strict-aliasing

AM_CFLAGS = $(AM_COMMON_FLAGS)
AM_CXXFLAGS = $(AM_COMMON_FLAGS) -Wnon-virtual-dtor -Wno-invalid-offsetof \
-Wstrict-null-sentinel
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
EXTRA_DIST = $(srcdir)/verify-mds-journal.sh $(srcdir)/vstart.sh $(srcdir)/stop.sh \
	ceph-run $(srcdir)/ceph_common.sh $(srcdir)/init-ceph.in $(srcdir)/mkcephfs.in \
	$(srcdir)/init-radosgw \
	$(srcdir)/ceph-debugpack.in \
	$(srcdir)/ceph-coverage.in \
	$(srcdir)/ceph-clsinfo $(srcdir)/make_version $(srcdir)/check_version \
	$(srcdir)/.git_version \
	$(srcdir)/ceph-rbdnamer \
	$(ceph_tool_gui_DATA)

# work around old versions of automake that don't define $docdir
docdir = ${datadir}/doc/ceph
doc_DATA = $(srcdir)/sample.ceph.conf
doc_SCRIPTS = sample.fetch_config

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
	$(srcdir)/include/rbd/librbd.h \
	$(srcdir)/include/rbd/librbd.hpp

rados_includedir = $(includedir)/rados
rados_include_DATA = \
	$(srcdir)/include/rados/librados.h \
	$(srcdir)/include/rados/librados.hpp \
	$(srcdir)/include/buffer.h \
	$(srcdir)/include/page.h \
	$(srcdir)/include/crc32c.h

if WITH_RADOSGW
rgw_includedir = $(includedir)/rados
rgw_include_DATA = \
	$(srcdir)/include/rados/librgw.h
endif

crush_includedir = $(includedir)/crush
crush_include_DATA = \
	$(srcdir)/crush/hash.h \
	$(srcdir)/crush/crush.h \
	$(srcdir)/crush/mapper.h \
	$(srcdir)/crush/types.h

FORCE:
.git_version: FORCE
	$(srcdir)/check_version $(srcdir)/.git_version
ceph_ver.h: .git_version
	$(srcdir)/make_version $(srcdir)/.git_version ./ceph_ver.h

ceph_ver.c: ./ceph_ver.h
common/version.cc: ./ceph_ver.h

# cleaning
clean-local:
	-rm *.so *.gcno *.gcda

# libs
libcommon_la_SOURCES = $(libcommon_files)
libcommon_la_CFLAGS= ${CRYPTO_CFLAGS} ${AM_CFLAGS}
libcommon_la_CXXFLAGS= ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
libcommon_la_LDFLAGS = -lrt
noinst_LTLIBRARIES += libcommon.la

libglobal_la_SOURCES = \
	global/global_context.cc \
	global/global_init.cc \
	global/pidfile.cc \
	global/signal_handler.cc
libglobal_la_CFLAGS= ${CRYPTO_CFLAGS} ${AM_CFLAGS}
libglobal_la_CXXFLAGS= ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
libglobal_la_LIBADD= libcommon.la
noinst_LTLIBRARIES += libglobal.la

crush_files = \
	crush/builder.c \
	crush/mapper.c \
	crush/crush.c \
	crush/hash.c \
	crush/CrushWrapper.cc

# this list ommits the ceph_ver.c file
libcommon_files = \
	./ceph_ver.c \
	auth/AuthAuthorizeHandler.cc \
	auth/AuthClientHandler.cc \
	auth/AuthSupported.cc \
	auth/cephx/CephxAuthorizeHandler.cc \
	auth/cephx/CephxClientHandler.cc \
	auth/cephx/CephxProtocol.cc \
	auth/none/AuthNoneAuthorizeHandler.cc \
	auth/Crypto.cc \
	auth/KeyRing.cc \
	auth/RotatingKeyRing.cc \
	common/LogClient.cc \
	msg/Message.cc \
	msg/msg_types.cc \
	common/BackTrace.cc \
	common/perf_counters.cc \
	common/admin_socket.cc \
	common/admin_socket_client.cc \
	common/escape.c \
	common/Clock.cc \
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
	common/safe_io.c \
	common/str_list.cc \
	common/errno.cc \
	msg/SimpleMessenger.cc \
	mon/MonMap.cc \
	mon/MonClient.cc \
	osd/OSDMap.cc \
	osd/osd_types.cc \
	mds/MDSMap.cc \
	common/common_init.cc \
	common/pipe.c \
	common/ceph_argparse.cc \
	common/ceph_context.cc \
	common/buffer.cc \
	common/code_environment.cc \
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
	common/DoutStreambuf.cc \
	common/version.cc \
	common/hex.cc \
	common/entity_name.cc \
	common/ceph_crypto.cc \
	include/addr_parsing.c \
	$(crush_files)

if WITH_PROFILER
libcommon_files += perfglue/cpu_profiler.cc
else
libcommon_files += perfglue/disabled_stubs.cc
endif



libmon_la_SOURCES = \
	auth/cephx/CephxKeyServer.cc \
	auth/cephx/CephxServiceHandler.cc \
	auth/AuthServiceHandler.cc \
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
	mon/MonCaps.cc
libmon_la_LIBADD = libglobal.la
noinst_LTLIBRARIES += libmon.la

libmds_a_SOURCES = \
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
	mds/SnapServer.cc \
	mds/snap.cc \
	mds/SessionMap.cc \
	mds/MDLog.cc
noinst_LIBRARIES += libmds.a

libos_la_SOURCES = \
	os/FileJournal.cc \
	os/FileStore.cc \
	os/JournalingObjectStore.cc \
	os/LFNIndex.cc \
	os/HashIndex.cc \
	os/IndexManager.cc \
	os/FlatIndex.cc
libos_la_CXXFLAGS= ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
libos_la_LIBADD = libglobal.la
noinst_LTLIBRARIES += libos.la

libosd_la_SOURCES = \
	osd/PG.cc \
	osd/ReplicatedPG.cc \
	osd/Ager.cc \
	osd/OSD.cc \
	osd/OSDCaps.cc \
	osd/Watch.cc \
        osd/ClassHandler.cc
libosd_la_CXXFLAGS= ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
libosd_la_LIBADD = libglobal.la
noinst_LTLIBRARIES += libosd.la

libosdc_la_SOURCES = \
	osdc/Objecter.cc \
	osdc/ObjectCacher.cc \
	osdc/Filer.cc \
	osdc/Journaler.cc
libosdc_la_LIBADD = libcommon.la
noinst_LTLIBRARIES += libosdc.la

libclient_la_SOURCES = \
	client/Client.cc \
	client/Inode.cc \
	client/MetaRequest.cc \
	client/Trace.cc
libclient_la_LIBADD = libcommon.la $(LIBEDIT_LIBS)
noinst_LTLIBRARIES += libclient.la

dist-hook:
	$(srcdir)/check_version $(srcdir)/.git_version

python_PYTHON = pybind/rados.py \
		pybind/rgw.py \
		pybind/rbd.py

if WITH_DEBUG
dist_bin_SCRIPTS += test/ceph-pybind-test.py \
		    test/ceph-pybind-rgw-test.py
endif

# headers... and everything else we want to include in a 'make dist' 
# that autotools doesn't magically identify.
noinst_HEADERS = \
	rados_sync.h \
	auth/cephx/CephxAuthorizeHandler.h\
	auth/cephx/CephxKeyServer.h\
	auth/cephx/CephxProtocol.h\
	auth/cephx/CephxClientHandler.h\
	auth/cephx/CephxServiceHandler.h\
	auth/none/AuthNoneAuthorizeHandler.h\
	auth/none/AuthNoneClientHandler.h\
	auth/none/AuthNoneServiceHandler.h\
	auth/none/AuthNoneProtocol.h\
	auth/Auth.h\
	auth/AuthSupported.h\
	auth/AuthClientHandler.h\
	auth/AuthServiceHandler.h\
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
	client/SnapRealm.h\
        client/SyntheticClient.h\
        client/Trace.h\
        client/fuse_ll.h\
	client/ioctl.h\
        client/hadoop/CephFSInterface.h\
	cls_acl.cc\
	cls_crypto.cc\
	common/BackTrace.h\
	common/DoutStreambuf.h\
	common/HeartbeatMap.h\
	common/LogClient.h\
	common/LogEntry.h\
	common/WorkQueue.h\
	common/ceph_argparse.h\
	common/ceph_context.h\
	common/compiler_extensions.h\
	common/debug.h\
	common/dout.h\
	common/escape.h\
	common/version.h\
	common/hex.h\
	common/entity_name.h\
	common/errno.h\
	common/environment.h\
	common/likely.h\
	common/lockdep.h\
        common/Clock.h\
        common/Cond.h\
        common/ConfUtils.h\
        common/DecayCounter.h\
        common/Finisher.h\
	common/Formatter.h\
        common/perf_counters.h\
	common/admin_socket.h \
	common/admin_socket_client.h \
        common/MemoryModel.h\
        common/Mutex.h\
        common/RWLock.h\
        common/Semaphore.h\
        common/Thread.h\
        common/Throttle.h\
        common/Timer.h\
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
	common/utf8.h\
	common/mime.h\
	common/secret.h\
	common/strtol.h\
	common/static_assert.h\
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
	include/ceph_frag.h\
        include/ceph_fs.h\
        include/ceph_hash.h\
	include/color.h\
	include/crc32c.h\
        include/cstring.h\
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
	include/linux_fiemap.h\
        include/lru.h\
	include/msgr.h\
        include/nstring.h\
        include/object.h\
        include/page.h\
        include/rangeset.h\
	include/rados.h\
	include/rbd_types.h\
        include/statlite.h\
	include/str_list.h\
        include/triple.h\
        include/tstring.h\
        include/types.h\
        include/utime.h\
        include/dlist.h\
        include/elist.h\
        include/xlist.h\
	include/rados/librados.h\
	include/rados/librados.hpp\
	include/rados/librgw.h\
	include/rados/page.h\
	include/rados/crc32c.h\
	include/rados/buffer.h\
	include/rbd/librbd.h\
	include/rbd/librbd.hpp\
	logrotate.conf\
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
        mds/events/EString.h\
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
        messages/MMonMap.h\
        messages/MMonObserve.h\
        messages/MMonObserveNotify.h\
        messages/MMonPaxos.h\
        messages/MMonSubscribe.h\
        messages/MMonSubscribeAck.h\
        messages/MOSDAlive.h\
        messages/MOSDBoot.h\
        messages/MOSDFailure.h\
        messages/MOSDMap.h\
        messages/MOSDOp.h\
        messages/MOSDOpReply.h\
        messages/MOSDPGCreate.h\
        messages/MOSDPGInfo.h\
        messages/MOSDPGLog.h\
        messages/MOSDPGMissing.h\
        messages/MOSDPGNotify.h\
        messages/MOSDPGQuery.h\
        messages/MOSDPGRemove.h\
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
        messages/MWatchNotify.h\
	messages/PaxosServiceMessage.h\
	mon/AuthMonitor.h\
        mon/Elector.h\
	mon/LogMonitor.h\
        mon/MDSMonitor.h\
	mon/MonmapMonitor.h\
        mon/MonCaps.h\
        mon/MonClient.h\
        mon/MonMap.h\
        mon/Monitor.h\
        mon/MonitorStore.h\
        mon/OSDMonitor.h\
        mon/PGMap.h\
        mon/PGMonitor.h\
        mon/Paxos.h\
        mon/PaxosService.h\
        mon/Session.h\
        mon/mon_types.h\
	mount/canonicalize.c\
	mount/mtab.c\
        msg/Dispatcher.h\
        msg/Message.h\
        msg/Messenger.h\
        msg/SimpleMessenger.h\
        msg/msg_types.h\
        msg/tcp.cc\
        msg/tcp.h\
	objclass/objclass.h\
	obsync/obsync\
	obsync/boto_tool\
	os/btrfs_ioctl.h\
	os/CollectionIndex.h\
        os/Fake.h\
        os/FileJournal.h\
        os/FileStore.h\
	os/FlatIndex.h\
	os/HashIndex.h\
	os/IndexManager.h\
        os/Journal.h\
        os/JournalingObjectStore.h\
	os/LFNIndex.h\
        os/ObjectStore.h\
        osd/Ager.h\
	osd/ClassHandler.h\
        osd/OSD.h\
        osd/OSDCaps.h\
        osd/OSDMap.h\
        osd/ObjectVersioner.h\
        osd/PG.h\
        osd/PGLS.h\
        osd/ReplicatedPG.h\
        osd/Watch.h\
        osd/osd_types.h\
	osdc/rados_bencher.h\
        osdc/Blinker.h\
        osdc/Filer.h\
        osdc/Journaler.h\
        osdc/ObjectCacher.h\
        osdc/Objecter.h\
        perfglue/cpu_profiler.h\
        perfglue/heap_profiler.h\
	rgw/rgw_access.h\
	rgw/rgw_acl.h\
	rgw/rgw_xml.h\
	rgw/rgw_cache.h\
	rgw/rgw_cls_api.h\
	rgw/rgw_common.h\
	rgw/rgw_formats.h\
	rgw/rgw_fs.h\
	rgw/rgw_log.h\
	rgw/rgw_multi.h\
	rgw/rgw_op.h\
	rgw/rgw_swift.h\
	rgw/rgw_swift_auth.h\
	rgw/rgw_rados.h\
	rgw/rgw_rest.h\
	rgw/rgw_rest_swift.h\
	rgw/rgw_rest_s3.h\
	rgw/rgw_tools.h\
	rgw/rgw_user.h\
	sample.ceph.conf\
	tools/common.h\
	tools/gui.h\
	tools/gui_resources.h\
	test/osd/RadosModel.h\
	global/pidfile.h\
	common/sync_filesystem.h \
	test/system/cross_process_sem.h \
	test/system/st_rados_create_pool.h \
	test/system/st_rados_list_objects.h \
	test/system/systest_runnable.h \
	test/system/systest_settings.h \
	test/rados-api/test.h \
	test/unit.h

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


project.tgz: clean
	cov-configure -co /usr/bin/gcc
	cov-configure -co /usr/bin/g++
	cov-build -e emit -o output make
	tar czvf project.tgz README emit output

submit-coverity:
	scp project.tgz ceph.newdream.net:ceph.newdream.net/coverity/`git describe`.tgz
	echo "v2;ceph;"`cat .coverity.build.pass`";MAIL;http://ceph.newdream.net/coverity/"`git describe`.tgz | mail build@submit.scan.coverity.com
