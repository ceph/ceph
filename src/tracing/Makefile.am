%.c %.h: %.tp
	$(LTTNG_GEN_TP_PROG) $<
	rm -f $<.o

dist_noinst_DATA = mutex.tp oprequest.tp osd.tp pg.tp

libtracepoints_la_SOURCES = \
	mutex.c \
	mutex.h \
	oprequest.c \
	oprequest.h \
	osd.c \
	osd.h \
	pg.h \
	pg.c \
	tracing-common.h

libtracepoints_la_LIBADD = -llttng-ust -ldl
libtracepoints_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
libtracepoints_la_LDFLAGS =
noinst_LTLIBRARIES = libtracepoints.la

BUILT_SOURCES = mutex.h oprequest.h osd.h pg.h

CLEANFILES = $(libtracepoints_la_SOURCES)
