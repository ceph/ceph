include os/filestore/Makefile.am
include os/keyvaluestore/Makefile.am
include os/memstore/Makefile.am

if ENABLE_SERVER

noinst_HEADERS += \
	os/ObjectStore.h

libos_la_SOURCES = \
	os/ObjectStore.cc \
	os/Transaction.cc \
	common/TrackedOp.cc

libos_la_CXXFLAGS = ${AM_CXXFLAGS}
#libos_la_LIBADD = $(LIBOS_TYPES)
if WITH_LTTNG
libos_la_LIBADD = $(LIBOS_TP)
endif

noinst_LTLIBRARIES += libos.la
endif # ENABLE_SERVER
