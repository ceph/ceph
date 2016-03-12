if ENABLE_CLIENT
include tools/Makefile-client.am
endif

if ENABLE_SERVER
include tools/Makefile-server.am
endif

monmaptool_SOURCES = tools/monmaptool.cc
monmaptool_LDADD = $(CEPH_GLOBAL)
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

ceph_conf_SOURCES = tools/ceph_conf.cc
ceph_conf_LDADD = $(CEPH_GLOBAL)
bin_PROGRAMS += ceph-conf

ceph_authtool_SOURCES = tools/ceph_authtool.cc
ceph_authtool_LDADD = $(CEPH_GLOBAL)
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
	tools/RadosDump.h \
	tools/rados/RadosImport.h \
	tools/ceph_objectstore_tool.h \
	tools/rados/PoolDump.h \
	tools/cephfs/DataScan.h \
	tools/cephfs/RoleSelector.h

EXTRA_DIST += \
	tools/setup-virtualenv.sh
