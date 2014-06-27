%.c %.h: %.tp
	$(LTTNG_GEN_TP_PROG) $< -o $*.c -o $*.h

dist_noinst_DATA = \
	librados.tp \
	mutex.tp \
	oprequest.tp \
	osd.tp \
	pg.tp

libtracepoints_la_SOURCES = \
	librados.c \
	librados.h \
	mutex.c \
	mutex.h \
	oprequest.c \
	oprequest.h \
	osd.c \
	osd.h \
	pg.h \
	pg.c

libtracepoints_la_LIBADD = -llttng-ust -ldl
libtracepoints_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
libtracepoints_la_LDFLAGS =
noinst_LTLIBRARIES = libtracepoints.la

BUILT_SOURCES = \
	librados.h \
	mutex.h \
	oprequest.h \
	osd.h \
	pg.h

CLEANFILES = $(libtracepoints_la_SOURCES)
