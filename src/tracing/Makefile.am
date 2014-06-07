libtracepoints_la_SOURCES = \
	mutex.tp.c \
	mutex.tp.h \
	osd.tp.c \
	osd.tp.h \
	pg.tp.h \
	pg.tp.c

libtracepoints_la_LIBADD = -llttng-ust -ldl
libtracepoints_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
libtracepoints_la_LDFLAGS =
noinst_LTLIBRARIES = libtracepoints.la
