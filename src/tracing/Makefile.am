libtracepoints_la_SOURCES = \
	mutex.tp.c \
	mutex.tp.h

libtracepoints_la_LIBADD = -llttng-ust -ldl
libtracepoints_la_CPPFLAGS = -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
libtracepoints_la_LDFLAGS =
noinst_LTLIBRARIES = libtracepoints.la
