if ENABLE_SERVER
if ENABLE_XIO

noinst_HEADERS += \
	test/messenger/message_helper.h \
	test/messenger/simple_dispatcher.h

simple_server_SOURCES = \
	test/messenger/simple_server.cc \
	test/messenger/simple_dispatcher.cc

simple_server_CFLAGS = $(AM_CFLAGS)
simple_server_CXXFLAGS = $(AM_CXXFLAGS)

simple_server_LDADD = \
	$(LIBOS) $(LIBCOMMON) $(CEPH_GLOBAL) $(PTHREAD_LIBS) $(EXTRALIBS)

if LINUX
simple_server_LDADD += -ldl
endif

simple_client_SOURCES = \
	test/messenger/simple_client.cc \
	test/messenger/simple_dispatcher.cc

simple_client_CFLAGS = $(AM_CFLAGS)
simple_client_CXXFLAGS = $(AM_CXXFLAGS)

simple_client_LDADD = \
	$(LIBOS) $(LIBCOMMON) $(CEPH_GLOBAL) $(PTHREAD_LIBS) $(EXTRALIBS)

if LINUX
simple_client_LDADD += -ldl
endif

noinst_PROGRAMS += \
	simple_server \
	simple_client

noinst_HEADERS += \
	test/messenger/xio_dispatcher.h

xio_server_SOURCES = \
	test/messenger/xio_server.cc \
	test/messenger/xio_dispatcher.cc

xio_server_CFLAGS = $(AM_CFLAGS)
xio_server_CXXFLAGS = $(AM_CXXFLAGS)

xio_server_LDADD = \
	$(LIBOS) $(CEPH_GLOBAL) $(LIBCOMMON) $(PTHREAD_LIBS) $(EXTRALIBS)

if LINUX
xio_server_LDADD += -ldl
endif

xio_client_SOURCES = \
	test/messenger/xio_client.cc \
	test/messenger/xio_dispatcher.cc

xio_client_CFLAGS = $(AM_CFLAGS)
xio_client_CXXFLAGS = $(AM_CXXFLAGS)

xio_client_LDADD = \
	$(LIBOS) $(CEPH_GLOBAL) $(LIBCOMMON) $(PTHREAD_LIBS) $(EXTRALIBS)

if LINUX
xio_client_LDADD += -ldl
endif

noinst_PROGRAMS += \
	xio_server \
	xio_client

endif # ENABLE_XIO
endif # ENABLE_SERVER

EXTRA_DIST += test/messenger/CMakeLists.txt
