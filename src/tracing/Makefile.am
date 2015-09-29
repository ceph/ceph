EXTRA_DIST = tracing-common.h

if WITH_LTTNG
%.h: %.tp
	$(LTTNG_GEN_TP_PROG) $< -o $*.h
endif

dist_noinst_DATA = \
	librados.tp \
	librbd.tp \
	oprequest.tp \
	osd.tp \
	pg.tp \
	objectstore.tp

if WITH_LTTNG
nodist_libosd_tp_la_SOURCES = \
	oprequest.c \
	oprequest.h \
	osd.c \
	osd.h \
	pg.h \
	pg.c
endif
libosd_tp_la_LIBADD = -llttng-ust -ldl
libosd_tp_la_CFLAGS = -I$(top_srcdir)/src $(AM_CFLAGS) -fpic
libosd_tp_la_LDFLAGS = -version-info 1:0:0

if WITH_LTTNG
nodist_librados_tp_la_SOURCES = \
	librados.c \
	librados.h
endif
librados_tp_la_LIBADD = -llttng-ust -ldl
librados_tp_la_CFLAGS = -I$(top_srcdir)/src $(AM_CFLAGS) -fpic
librados_tp_la_LDFLAGS = -version-info 2:0:0

if WITH_LTTNG
nodist_librbd_tp_la_SOURCES = \
	librbd.c \
	librbd.h
endif
librbd_tp_la_LIBADD = -llttng-ust -ldl
librbd_tp_la_CFLAGS = -I$(top_srcdir)/src $(AM_CFLAGS) -fpic
librbd_tp_la_LDFLAGS = -version-info 1:0:0

if WITH_LTTNG
nodist_libos_tp_la_SOURCES = \
	objectstore.c \
	objectstore.h
endif
libos_tp_la_LIBADD = -llttng-ust -ldl
libos_tp_la_CFLAGS = -I$(top_srcdir)/src $(AM_CFLAGS) -fpic
libos_tp_la_LDFLAGS = -version-info 1:0:0

if WITH_LTTNG
lib_LTLIBRARIES = \
	libosd_tp.la \
	libos_tp.la \
	librados_tp.la \
	librbd_tp.la

BUILT_SOURCES = \
	librados.h \
	librbd.h \
	objectstore.h \
	oprequest.h \
	osd.h \
	pg.h
endif

CLEANFILES = \
	$(BUILT_SOURCES)
