libmsg_la_SOURCES = \
	msg/Message.cc \
	msg/Messenger.cc \
	msg/msg_types.cc

noinst_HEADERS += \
	msg/Connection.h \
	msg/Dispatcher.h \
	msg/Message.h \
	msg/Messenger.h \
	msg/SimplePolicyMessenger.h \
	msg/msg_types.h

libmsg_la_SOURCES += \
	msg/simple/Accepter.cc \
	msg/simple/DispatchQueue.cc \
	msg/simple/Pipe.cc \
	msg/simple/PipeConnection.cc \
	msg/simple/SimpleMessenger.cc \
	msg/async/AsyncConnection.cc \
	msg/async/AsyncMessenger.cc \
	msg/async/Event.cc \
	msg/async/net_handler.cc \
	msg/async/EventSelect.cc

if LINUX
libmsg_la_SOURCES += msg/async/EventEpoll.cc
endif

if DARWIN
libmsg_la_SOURCES += msg/async/EventKqueue.cc
endif

if FREEBSD
libmsg_la_SOURCES += msg/async/EventKqueue.cc
endif


noinst_HEADERS += \
	msg/simple/Accepter.h \
	msg/simple/DispatchQueue.h \
	msg/simple/Pipe.h \
	msg/simple/PipeConnection.h \
	msg/simple/SimpleMessenger.h \
	msg/async/AsyncConnection.h \
	msg/async/AsyncMessenger.h \
	msg/async/Event.h \
	msg/async/EventEpoll.h \
	msg/async/EventSelect.h \
	msg/async/net_handler.h

if LINUX
libmsg_la_SOURCES += msg/async/EventEpoll.h
endif

if DARWIN
libmsg_la_SOURCES += msg/async/EventKqueue.h
endif

if FREEBSD
libmsg_la_SOURCES += msg/async/EventKqueue.h
endif

if ENABLE_XIO
libmsg_la_SOURCES += \
	msg/xio/QueueStrategy.cc \
	msg/xio/XioConnection.cc \
	msg/xio/XioMessenger.cc \
	msg/xio/XioMsg.cc \
	msg/xio/XioPortal.cc \
	msg/xio/XioPool.cc

noinst_HEADERS += \
	msg/xio/DispatchStrategy.h \
	msg/xio/FastStrategy.h \
	msg/xio/QueueStrategy.h \
	msg/xio/XioConnection.h \
	msg/xio/XioInSeq.h \
	msg/xio/XioMessenger.h \
	msg/xio/XioMsg.h \
	msg/xio/XioPool.h \
	msg/xio/XioPortal.h \
	msg/xio/XioSubmit.h
endif

noinst_LTLIBRARIES += libmsg.la
