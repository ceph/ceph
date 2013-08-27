AUTOMAKE_OPTIONS = gnu subdir-objects
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
	unittest_bufferlist.sh \
	yasm-wrapper

noinst_HEADERS =
CLEANFILES =
bin_PROGRAMS =
# like bin_PROGRAMS, but these targets are only built for debug builds
bin_DEBUGPROGRAMS =
sbin_PROGRAMS =
# like sbin_SCRIPTS but can be used to install to e.g. /usr/sbin
ceph_sbindir = $(exec_prefix)$(sbindir)
ceph_sbin_SCRIPTS = \
	ceph-disk \
	ceph-disk-prepare \
	ceph-disk-activate \
	ceph-disk-udev \
	ceph-create-keys

sbin_SCRIPTS = \
	mount.fuse.ceph

bin_SCRIPTS = ceph ceph-run ceph-rest-api ceph-clsinfo ceph-debugpack ceph-rbdnamer ceph-post-file
dist_bin_SCRIPTS =
# C/C++ tests to build will be appended to this
check_PROGRAMS =
# tests to actually run on "make check"; if you need extra, non-test,
# executables built, you need to replace this with manual assignments
# target by target
TESTS = $(check_PROGRAMS) unittest_bufferlist.sh

radoslibdir = $(libdir)/rados-classes

check-local:
	$(srcdir)/test/encoding/check-generated.sh
	$(srcdir)/test/encoding/readable.sh ../ceph-object-corpus

core-daemons: ceph-mon ceph-osd ceph-mds radosgw
admin-tools: monmaptool osdmaptool crushtool ceph-authtool

base: core-daemons admin-tools \
	cephfs ceph-syn ceph-conf \
	rados librados-config \
	init-ceph mkcephfs ceph-mon-store-converter ceph-post-file


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

LIBGLOBAL = libglobal.la
LIBCOMMON = libcommon.la
LIBAUTH = libauth.la
LIBCRUSH = libcrush.la
LIBJSON_SPIRIT = libjson_spirit.la
LIBLOG = liblog.la
LIBOS = libos.la
LIBOSD = libosd.la
LIBOSDC = libosdc.la
LIBMON = libmon.la
LIBMDS = libmds.la
LIBCLIENT = libclient.la
LIBCLIENT_FUSE = libclient_fuse.la
LIBRADOS = librados.la
LIBRGW = librgw.la
LIBRBD = librbd.la
LIBCEPHFS = libcephfs.la

if WITH_LIBAIO
LIBOS += -laio
endif

if WITH_LIBZFS
LIBOS += libos_zfs.a -lzfs
endif

# use system leveldb
LIBOS += -lleveldb -lsnappy

tcmalloc_safety_flags = -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free

CEPH_GLOBAL = $(LIBGLOBAL) $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)

######################
## Programs

# Daemons

ceph_mon_SOURCES = ceph_mon.cc
ceph_mon_LDADD = $(LIBMON) $(LIBOS) $(CEPH_GLOBAL)
if WITH_TCMALLOC
ceph_mon_LDADD += -ltcmalloc
ceph_mon_CXXFLAGS = ${AM_CXXFLAGS} ${tcmalloc_safety_flags}
ceph_mon_SOURCES += perfglue/heap_profiler.cc
else
ceph_mon_SOURCES += perfglue/disabled_heap_profiler.cc
endif # WITH_TCMALLOC
bin_PROGRAMS += ceph-mon

ceph_osd_SOURCES = ceph_osd.cc
ceph_osd_LDADD = $(LIBOSD) $(LIBOS) $(CEPH_GLOBAL)
if LINUX
ceph_osd_LDADD += -ldl
endif # LINUX
if WITH_TCMALLOC
ceph_osd_LDADD += -ltcmalloc
ceph_osd_CXXFLAGS = ${AM_CXXFLAGS} ${tcmalloc_safety_flags}
ceph_osd_SOURCES += perfglue/heap_profiler.cc
else
ceph_osd_SOURCES += perfglue/disabled_heap_profiler.cc
endif # WITH_TCMALLOC
bin_PROGRAMS += ceph-osd

ceph_mds_SOURCES = ceph_mds.cc
ceph_mds_LDADD = $(LIBMDS) $(LIBOSDC) $(CEPH_GLOBAL)
if WITH_TCMALLOC
ceph_mds_LDADD += -ltcmalloc
ceph_mds_CXXFLAGS = ${AM_CXXFLAGS} ${tcmalloc_safety_flags}
ceph_mds_SOURCES += perfglue/heap_profiler.cc
else
ceph_mds_SOURCES += perfglue/disabled_heap_profiler.cc
endif # WITH_TCMALLOC
bin_PROGRAMS += ceph-mds

# Admin tools

ceph_conf_SOURCES = ceph_conf.cc
ceph_conf_LDADD = $(CEPH_GLOBAL)
bin_PROGRAMS += ceph-conf

ceph_authtool_SOURCES = ceph_authtool.cc
ceph_authtool_LDADD = $(CEPH_GLOBAL)
bin_PROGRAMS += ceph-authtool

ceph_filestore_dump_SOURCES = \
	tools/ceph-filestore-dump.cc \
	perfglue/disabled_heap_profiler.cc
ceph_filestore_dump_LDADD = $(LIBOSD) $(LIBOS) $(CEPH_GLOBAL) -lboost_program_options
if LINUX
ceph_filestore_dump_LDADD += -ldl
endif # LINUX
bin_PROGRAMS += ceph_filestore_dump

ceph_mon_store_converter_SOURCES = mon_store_converter.cc
ceph_mon_store_converter_LDADD = $(LIBMON) $(LIBOS) $(CEPH_GLOBAL)
bin_PROGRAMS += ceph-mon-store-converter

ceph_dencoder_SOURCES = \
	test/encoding/ceph_dencoder.cc \
	rgw/rgw_dencoder.cc \
	rgw/rgw_acl.cc \
	rgw/rgw_common.cc \
	rgw/rgw_env.cc \
	rgw/rgw_json_enc.cc \
	perfglue/disabled_heap_profiler.cc
ceph_dencoder_LDADD = \
	$(LIBOSD) $(LIBMDS) $(LIBOSDC) $(LIBOS) $(LIBMON) \
	libcls_lock_client.la \
	libcls_rgw_client.la \
	libcls_replica_log_client.a \
	libcls_refcount_client.la \
	$(CEPH_GLOBAL)
ceph_dencoder_CXXFLAGS = ${AM_CXXFLAGS} -fno-var-tracking-assignments
bin_PROGRAMS += ceph-dencoder

# User tools

mount_ceph_SOURCES = mount/mount.ceph.c
mount_ceph_LDADD = $(LIBCOMMON)
if LINUX
sbin_PROGRAMS += mount.ceph
endif # LINUX

cephfs_SOURCES = cephfs.cc
cephfs_LDADD = $(LIBCOMMON)
bin_PROGRAMS += cephfs

librados_config_SOURCES = librados-config.cc
librados_config_LDADD = $(LIBRADOS) $(CEPH_GLOBAL)
bin_PROGRAMS += librados-config

# SyntheticClient.cc requires g_conf, so it can't
# go in LIBCLIENT.
ceph_syn_SOURCES = \
	ceph_syn.cc \
	client/SyntheticClient.cc
ceph_syn_LDADD = $(LIBCLIENT) $(CEPH_GLOBAL)
bin_PROGRAMS += ceph-syn

monmaptool_SOURCES = monmaptool.cc
monmaptool_LDADD = $(CEPH_GLOBAL)
bin_PROGRAMS += monmaptool

crushtool_SOURCES = crushtool.cc
crushtool_LDADD = $(CEPH_GLOBAL)
bin_PROGRAMS += crushtool

osdmaptool_SOURCES = osdmaptool.cc
osdmaptool_LDADD = $(CEPH_GLOBAL)
bin_PROGRAMS += osdmaptool

rados_SOURCES = \
	rados.cc \
	rados_import.cc \
	rados_export.cc \
	rados_sync.cc \
	common/obj_bencher.cc
rados_LDADD = libcls_lock_client.la $(LIBRADOS) $(CEPH_GLOBAL)
bin_PROGRAMS += rados

if WITH_REST_BENCH
rest_bench_SOURCES = \
	tools/rest_bench.cc \
	common/obj_bencher.cc
rest_bench_LDADD = $(CEPH_GLOBAL)
bin_PROGRAMS += rest-bench

if WITH_SYSTEM_LIBS3
rest_bench_LDADD += -ls3
else
rest_bench_LDADD += libs3/build/lib/libs3.a -lcurl -lxml2
rest_bench_CXXFLAGS = ${AM_CXXFLAGS} -I$(top_srcdir)/src/libs3/inc
SUBDIRS += libs3
endif # WITH_SYSTEM_LIBS3
endif # WITH_REST_BENCH

rbd_SOURCES = rbd.cc 
rbd_LDADD = $(LIBRBD) $(CEPH_GLOBAL)
if LINUX
bin_PROGRAMS += rbd
endif #LINUX

rbd-tool: rbd

# Fuse targets

if WITH_FUSE
ceph_fuse_SOURCES = ceph_fuse.cc
ceph_fuse_LDADD = $(LIBCLIENT_FUSE) $(CEPH_GLOBAL)
if WITH_TCMALLOC
ceph_fuse_LDADD += -ltcmalloc
ceph_fuse_CXXFLAGS = ${AM_CXXFLAGS} ${tcmalloc_safety_flags}
endif # WITH_TCMALLOC
bin_PROGRAMS += ceph-fuse

rbd_fuse_SOURCES = rbd_fuse/rbd-fuse.c
rbd_fuse_LDADD = -lfuse $(LIBRBD) $(CEPH_GLOBAL)
bin_PROGRAMS += rbd-fuse
endif # WITH_FUSE

# Debug targets

ceph_osdomap_tool_SOURCES = tools/ceph-osdomap-tool.cc
ceph_osdomap_tool_LDADD = $(LIBOS) $(CEPH_GLOBAL) -lboost_program_options
bin_DEBUGPROGRAMS += ceph-osdomap-tool

ceph_monstore_tool_SOURCES = tools/ceph-monstore-tool.cc
ceph_monstore_tool_LDADD = $(LIBOS) $(CEPH_GLOBAL) -lboost_program_options
bin_DEBUGPROGRAMS += ceph-monstore-tool

ceph_psim_SOURCES = psim.cc
ceph_psim_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_psim

ceph_test_msgr_SOURCES = testmsgr.cc
ceph_test_msgr_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_msgr

ceph_test_ioctls_SOURCES = client/test_ioctls.c
bin_DEBUGPROGRAMS += ceph_test_ioctls

ceph_dupstore_SOURCES = dupstore.cc
ceph_dupstore_LDADD = $(LIBOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_dupstore

ceph_streamtest_SOURCES = streamtest.cc
ceph_streamtest_LDADD = $(LIBOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_streamtest

ceph_test_trans_SOURCES = test_trans.cc
ceph_test_trans_LDADD = $(LIBOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_trans

ceph_scratchtool_SOURCES = scratchtool.c
ceph_scratchtool_LDADD = $(LIBRADOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_scratchtool

ceph_scratchtoolpp_SOURCES = scratchtoolpp.cc
ceph_scratchtoolpp_LDADD = $(LIBRADOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_scratchtoolpp

ceph_radosacl_SOURCES = radosacl.cc
ceph_radosacl_LDADD = $(LIBRADOS) $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_radosacl

ceph_test_crypto_SOURCES = testcrypto.cc
ceph_test_crypto_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_test_crypto

ceph_test_keys_SOURCES = testkeys.cc
ceph_test_keys_LDADD = $(LIBMON) $(CEPH_GLOBAL) 
if WITH_TCMALLOC
ceph_test_keys_LDADD += -ltcmalloc
ceph_test_keys_CXXFLAGS = ${tcmalloc_safety_flags}
ceph_test_keys_SOURCES += perfglue/heap_profiler.cc
endif # WITH_TCMALLOC
bin_DEBUGPROGRAMS += ceph_test_keys



######################
## Libraries

BUILT_SOURCES =
lib_LTLIBRARIES = 
noinst_LTLIBRARIES = 
noinst_LIBRARIES =
radoslib_LTLIBRARIES =

include librados/Makefile.am
include librbd/Makefile.am
include rgw/Makefile.am
include cls/Makefile.am
include key_value_store/Makefile.am
include test/Makefile.am

# libcephfs
libcephfs_la_SOURCES = libcephfs.cc
libcephfs_la_LIBADD = $(LIBCLIENT) $(LIBCOMMON) $(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS)
libcephfs_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0 -export-symbols-regex '^ceph_.*'
lib_LTLIBRARIES += libcephfs.la

## CephFS Java Wrappers
##  - The JNI library is here
##  - The Java source Makefile.am is in src/java
if ENABLE_CEPHFS_JAVA
libcephfs_jni_la_SOURCES = \
	java/native/libcephfs_jni.cc \
	java/native/ScopedLocalRef.h \
	java/native/JniConstants.cpp \
	java/native/JniConstants.h
libcephfs_jni_la_LIBADD = $(LIBCEPHFS) $(EXTRALIBS)
libcephfs_jni_la_CPPFLAGS = $(JDK_CPPFLAGS)
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

shell_scripts = init-ceph mkcephfs ceph-debugpack ceph-coverage ceph-post-file

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

AM_COMMON_FLAGS = \
	-D__CEPH__ \
	-D_FILE_OFFSET_BITS=64 \
	-D_REENTRANT \
	-D_THREAD_SAFE \
	-D__STDC_FORMAT_MACROS \
	-D_GNU_SOURCE \
	-Wall \
	${WARN_TYPE_LIMITS} \
	${WARN_IGNORED_QUALIFIERS} \
	-Winit-self \
	-Wpointer-arith \
	-Werror=format-security \
	-fno-strict-aliasing \
	-fsigned-char
if !CLANG
    AM_COMMON_FLAGS += -rdynamic
endif

AM_CFLAGS = $(AM_COMMON_FLAGS)
AM_CXXFLAGS = \
	@AM_CXXFLAGS@ \
	$(AM_COMMON_FLAGS) \
	-DCEPH_LIBDIR=\"${libdir}\" \
	-Wnon-virtual-dtor \
	-Wno-invalid-offsetof

if !CLANG
    AM_CXXFLAGS += -Wstrict-null-sentinel
endif
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
	$(srcdir)/init-rbdmap \
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
	ceph.in \
	ceph-disk \
	ceph-disk-prepare \
	ceph-disk-activate \
	ceph-disk-udev \
	ceph-create-keys \
	mount.fuse.ceph \
	ceph-rest-api \
	mount.fuse.ceph \
	rbdmap

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

# assemble Python script with global version variables
# NB: depends on format of ceph_ver.h

ceph: ceph.in ./ceph_ver.h Makefile
	rm -f $@ $@.tmp
	echo "#!/usr/bin/python" >$@.tmp
	grep "#define CEPH_GIT_NICE_VER" ./ceph_ver.h | \
		sed -e 's/#define \(.*VER\) /\1=/' >>$@.tmp
	grep "#define CEPH_GIT_VER" ./ceph_ver.h | \
	  sed -e 's/#define \(.*VER\) /\1=/' -e 's/=\(.*\)$$/="\1"/' >>$@.tmp
	cat $@.in >>$@.tmp
	chmod a+x $@.tmp
	chmod a-w $@.tmp
	mv $@.tmp $@

# cleaning
clean-local:
	-rm *.so *.gcno *.gcda

# libs in subdirs
include auth/Makefile.am
include crush/Makefile.am
include mon/Makefile.am
include mds/Makefile.am
include os/Makefile.am
include osd/Makefile.am
include osdc/Makefile.am
include client/Makefile.am
include global/Makefile.am
include json_spirit/Makefile.am
include log/Makefile.am

# libcommon
# -----------
# We have libcommon here and not in common/Makefile.am because it
# includes files from other subdirs. This needs to be cleaned later.
libcommon_la_SOURCES = \
	ceph_ver.c \
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
	common/cmdparse.cc \
	common/escape.c \
	common/Clock.cc \
	common/Throttle.cc \
	common/Timer.cc \
	common/Finisher.cc \
	common/environment.cc\
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
	common/RefCountedObj.cc \
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
	common/util.cc \
	common/TextTable.cc \
	common/secret.c \
	mon/MonCap.cc \
	mon/MonClient.cc \
	mon/MonMap.cc \
	msg/Accepter.cc \
	msg/DispatchQueue.cc \
	msg/Message.cc \
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
	include/ceph_fs.cc \
	include/ceph_hash.cc \
	include/ceph_strings.cc \
	include/ceph_frag.cc \
	include/addr_parsing.c

if WITH_PROFILER
libcommon_la_SOURCES += perfglue/cpu_profiler.cc
else
libcommon_la_SOURCES += perfglue/disabled_stubs.cc
endif # WITH_PROFILER

# We need this to avoid basename conflicts with the libcommon build tests in test/Makefile.am
libcommon_la_CFLAGS = ${AM_CFLAGS}

libcommon_la_LIBADD = $(LIBAUTH) $(LIBCRUSH) $(LIBJSON_SPIRIT) $(LIBLOG) -lkeyutils

if LINUX
libcommon_la_LIBADD += -lrt
endif # LINUX

noinst_LTLIBRARIES += libcommon.la

# crc injection into libcommon
libcommon_crc_la_SOURCES = \
	common/sctp_crc32.c \
	common/crc32c.cc \
	common/crc32c_intel_baseline.c \
	common/crc32c_intel_fast.c

if WITH_GOOD_YASM_ELF64
libcommon_crc_la_SOURCES += common/crc32c_intel_fast_asm.S
libcommon_crc_la_LIBTOOLFLAGS = --tag=CC
endif

noinst_LTLIBRARIES += libcommon_crc.la
libcommon_la_LIBADD += libcommon_crc.la

dist-hook:
	$(srcdir)/check_version $(srcdir)/.git_version

python_PYTHON = pybind/rados.py \
		pybind/rbd.py \
		pybind/cephfs.py \
		pybind/ceph_argparse.py \
		pybind/ceph_rest_api.py

# headers... and everything else we want to include in a 'make dist' 
# that autotools doesn't magically identify.
noinst_HEADERS += \
	rados_sync.h \
	arch/probe.h \
	arch/intel.h \
	cls_acl.cc\
	cls_crypto.cc\
	fetch_config\
	logrotate.conf\
	sample.ceph.conf\
	bash_completion/ceph\
	bash_completion/rados\
	bash_completion/rbd\
	bash_completion/radosgw-admin\
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
	common/common_init.h\
	common/pipe.h\
	common/code_environment.h \
	common/signal.h\
	common/simple_spin.h\
	common/run_cmd.h\
	common/safe_io.h\
	common/config.h\
	common/config_obs.h\
	common/config_opts.h\
	common/ceph_crypto.h\
	common/ceph_crypto_cms.h\
	common/ceph_json.h\
	common/crc32c_intel_baseline.h\
	common/crc32c_intel_fast.h\
	common/lru_map.h\
	common/utf8.h\
	common/mime.h\
	common/pick_address.h\
	common/sctp_crc32.h\
	common/secret.h\
	common/strtol.h\
	common/static_assert.h\
	common/AsyncReserver.h\
	common/sync_filesystem.h \
	common/cmdparse.h \
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
	messages/MMDSOpenIno.h \
	messages/MMDSOpenInoReply.h \
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
	messages/MMonScrub.h \
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
	messages/MOSDPGPush.h\
	messages/MOSDPGPull.h\
	messages/MOSDPGPushReply.h\
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
	perfglue/cpu_profiler.h\
	perfglue/heap_profiler.h\
	tools/common.h

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
