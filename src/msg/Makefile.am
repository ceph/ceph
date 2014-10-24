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

# simple
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
	msg/async/EventEpoll.cc

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
	msg/async/net_handler.h


noinst_LTLIBRARIES += libmsg.la


