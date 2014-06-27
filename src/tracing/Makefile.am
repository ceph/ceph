%.c %.h: %.tp
	$(LTTNG_GEN_TP_PROG) $< -o $*.c -o $*.h

dist_noinst_DATA = \
	librados.tp \
	librbd.tp \
	mutex.tp \
	oprequest.tp \
	osd.tp \
	pg.tp

libcommon_tp_la_SOURCES = \
	mutex.c \
	mutex.h
libcommon_tp_la_LIBADD = -llttng-ust -ldl
libcommon_tp_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
libcommon_tp_la_LDFLAGS =

libosd_tp_la_SOURCES = \
	oprequest.c \
	oprequest.h \
	osd.c \
	osd.h \
	pg.h \
	pg.c
libosd_tp_la_LIBADD = -llttng-ust -ldl
libosd_tp_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
libosd_tp_la_LDFLAGS =

librados_tp_la_SOURCES = \
	librados.c \
	librados.h
librados_tp_la_LIBADD = -llttng-ust -ldl
librados_tp_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
librados_tp_la_LDFLAGS =

librbd_tp_la_SOURCES = \
	librbd.c \
	librbd.h
librbd_tp_la_LIBADD = -llttng-ust -ldl
librbd_tp_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
librbd_tp_la_LDFLAGS =

noinst_LTLIBRARIES = \
	libcommon_tp.la \
	libosd_tp.la \
	librados_tp.la \
	librbd_tp.la

BUILT_SOURCES = \
	librados.h \
	librbd.h \
	mutex.h \
	oprequest.h \
	osd.h \
	pg.h

CLEANFILES = \
	$(libcommon_tp_la_SOURCES) \
	$(libosd_tp_la_SOURCES) \
	$(librados_tp_la_SOURCES)
