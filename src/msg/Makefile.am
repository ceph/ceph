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

if HAVE_DPDK
libmsg_la_CXXFLAGS += -I ${with_dpdk}/include -fPIC
libmsg_la_LIBADD += ${DPDK_LIBS}
libmsg_la_SOURCES += \
	msg/async/dpdk/ARP.cc \
	msg/async/dpdk/DPDK.cc \
	msg/async/dpdk/dpdk_rte.cc \
	msg/async/dpdk/DPDKStack.cc \
	msg/async/dpdk/EventDPDK.cc \
	msg/async/dpdk/IP.cc \
	msg/async/dpdk/Packet.cc \
	msg/async/dpdk/TCP.cc

noinst_HEADERS += \
	msg/async/dpdk/ARP.h \
	msg/async/dpdk/array_map.h \
	msg/async/dpdk/byteorder.h \
	msg/async/dpdk/circular_buffer.h \
	msg/async/dpdk/const.h \
	msg/async/dpdk/deleter.h \
	msg/async/dpdk/DPDK.h \
	msg/async/dpdk/dpdk_rte.h \
	msg/async/dpdk/DPDKStack.h \
	msg/async/dpdk/ethernet.h \
	msg/async/dpdk/EventDPDK.h \
	msg/async/dpdk/GenericSocket.h \
	msg/async/dpdk/IP.h \
	msg/async/dpdk/IPChecksum.h \
	msg/async/dpdk/Packet.h \
	msg/async/dpdk/PacketUtil.h \
	msg/async/dpdk/queue.h \
	msg/async/dpdk/shared_ptr.h \
	msg/async/dpdk/stream.h \
	msg/async/dpdk/TCP.h \
	msg/async/dpdk/toeplitz.h \
	msg/async/dpdk/transfer.h \
	msg/async/dpdk/unaligned.h \
	msg/async/dpdk/UserspaceEvent.h
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
