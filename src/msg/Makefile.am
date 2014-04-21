libmsg_la_SOURCES = \
	msg/Accepter.cc \
	msg/DispatchQueue.cc \
	msg/Message.cc \
	msg/Messenger.cc \
	msg/Pipe.cc \
	msg/PipeConnection.cc \
	msg/SimpleMessenger.cc \
	msg/msg_types.cc

noinst_HEADERS += \
	msg/Accepter.h \
	msg/Connection.h \
	msg/DispatchQueue.h \
	msg/Dispatcher.h \
	msg/Message.h \
	msg/Messenger.h \
	msg/Pipe.h \
	msg/SimpleMessenger.h \
	msg/msg_types.h

if ENABLE_XIO
libmsg_la_SOURCES += \
	msg/QueueStrategy.cc \
	msg/XioConnection.cc \
	msg/XioMessenger.cc \
	msg/XioMsg.cc \
	msg/XioPortal.cc

noinst_HEADERS += \
	msg/DispatchStrategy.h \
	msg/FastStrategy.h \
	msg/QueueStrategy.h \
	msg/SimplePolicyMessenger.h \
	msg/XioConnection.h \
	msg/XioMessenger.h \
	msg/XioMsg.h \
	msg/XioPool.h \
	msg/XioPortal.h
endif

noinst_LTLIBRARIES += libmsg.la
