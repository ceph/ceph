if ENABLE_CLIENT
include tools/Makefile-client.am
endif

if ENABLE_SERVER
include tools/Makefile-server.am
endif

monmaptool_SOURCES = tools/monmaptool.cc
monmaptool_LDADD = $(CEPH_GLOBAL) $(LIBCOMMON)
bin_PROGRAMS += monmaptool

crushtool_SOURCES = tools/crushtool.cc
crushtool_LDADD = $(CEPH_GLOBAL)
bin_PROGRAMS += crushtool

osdmaptool_SOURCES = tools/osdmaptool.cc
osdmaptool_LDADD = $(CEPH_GLOBAL)
bin_PROGRAMS += osdmaptool

ceph_psim_SOURCES = tools/psim.cc
ceph_psim_LDADD = $(CEPH_GLOBAL)
bin_DEBUGPROGRAMS += ceph_psim

if WITH_REST_BENCH
rest_bench_SOURCES = tools/rest_bench.cc
rest_bench_SOURCES += common/obj_bencher.cc # needs cleanup so it can go in libcommon.la
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

ceph_conf_SOURCES = tools/ceph_conf.cc
ceph_conf_LDADD = $(CEPH_GLOBAL) $(LIBCOMMON)
bin_PROGRAMS += ceph-conf

ceph_authtool_SOURCES = tools/ceph_authtool.cc
ceph_authtool_LDADD = $(CEPH_GLOBAL) $(LIBCOMMON)
bin_PROGRAMS += ceph-authtool

noinst_HEADERS += \
	tools/cephfs/JournalTool.h \
	tools/cephfs/JournalScanner.h \
	tools/cephfs/JournalFilter.h \
	tools/cephfs/EventOutput.h \
	tools/cephfs/Resetter.h \
	tools/cephfs/Dumper.h \
	tools/cephfs/TableTool.h \
	tools/cephfs/MDSUtility.h \
	tools/rados/rados_sync.h
