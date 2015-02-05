EXTRA_DIST = tracing-common.h

if WITH_LTTNG
%.c %.h: %.tp
	$(LTTNG_GEN_TP_PROG) $< -o $*.c -o $*.h
endif

dist_noinst_DATA = \
	librados.tp \
	librbd.tp \
	oprequest.tp \
	osd.tp \
	pg.tp \
	objectstore.tp \
	filejournal.tp \
	filestore.tp \
	pipe.tp \
	asyncmsgr.tp

if WITH_LTTNG
nodist_libosd_tp_la_SOURCES = \
	oprequest.c \
	oprequest.h \
	osd.c \
	osd.h \
	pg.h \
	pg.c \
	filejournal.h \
	filejournal.c
endif
libosd_tp_la_LIBADD = -llttng-ust -ldl
libosd_tp_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
libosd_tp_la_LDFLAGS =

if WITH_LTTNG
nodist_libpipe_tp_la_SOURCES = \
	pipe.c \
	pipe.h
endif
libpipe_tp_la_LIBADD = -llttng-ust -ldl
libpipe_tp_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
libpipe_tp_la_LDFLAGS =

if WITH_LTTNG
nodist_libasyncmsgr_tp_la_SOURCES = \
	asyncmsgr.c \
	asyncmsgr.h
endif
libasyncmsgr_tp_la_LIBADD = -llttng-ust -ldl
libasyncmsgr_tp_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
libasyncmsgr_tp_la_LDFLAGS =

if WITH_LTTNG
nodist_librados_tp_la_SOURCES = \
	librados.c \
	librados.h
endif
librados_tp_la_LIBADD = -llttng-ust -ldl
librados_tp_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
librados_tp_la_CFLAGS = -I$(top_srcdir)/src $(AM_CFLAGS)
librados_tp_la_LDFLAGS =

if WITH_LTTNG
nodist_librbd_tp_la_SOURCES = \
	librbd.c \
	librbd.h
endif
librbd_tp_la_LIBADD = -llttng-ust -ldl
librbd_tp_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
librbd_tp_la_CFLAGS = -I$(top_srcdir)/src $(AM_CFLAGS)
librbd_tp_la_LDFLAGS =

if WITH_LTTNG
nodist_libos_tp_la_SOURCES = \
	filejournal.c \
	filejournal.h \
	filestore.c \
	filestore.h \
	objectstore.c \
	objectstore.h
endif
libos_tp_la_LIBADD = -llttng-ust -ldl
libos_tp_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
libos_tp_la_CFLAGS = -I$(top_srcdir)/src $(AM_CFLAGS)
libos_tp_la_LDFLAGS =

if WITH_LTTNG
noinst_LTLIBRARIES = \
	libosd_tp.la \
	librados_tp.la \
	librbd_tp.la \
	libos_tp.la \
	libpipe_tp.la \
	libasyncmsgr_tp.la

BUILT_SOURCES = \
	librados.h \
	librbd.h \
	oprequest.h \
	osd.h \
	pg.h \
	objectstore.h \
	filejournal.h \
	filestore.h \
	pipe.h \
	asyncmsgr.h
endif

CLEANFILES = \
	$(nodist_libosd_tp_la_SOURCES) \
	$(nodist_librados_tp_la_SOURCES) \
	$(nodist_librbd_tp_la_SOURCES) \
	$(nodist_libos_tp_la_SOURCES) \
	$(nodist_libpipe_tp_la_SOURCES) \
	$(nodist_libaysncmsgr_tp_la_SOURCES)
