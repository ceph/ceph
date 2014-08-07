# librbd_replay_la exists only to help with unit tests
librbd_replay_la_SOURCES = rbd_replay/actions.cc \
	rbd_replay/Deser.cc \
	rbd_replay/ImageNameMap.cc \
	rbd_replay/PendingIO.cc \
	rbd_replay/rbd_loc.cc \
	rbd_replay/Replayer.cc
librbd_replay_la_LIBADD = $(LIBRBD) \
	$(LIBRADOS) \
	$(CEPH_GLOBAL)
noinst_LTLIBRARIES += librbd_replay.la
noinst_HEADERS += rbd_replay/BoundedBuffer.hpp \
	rbd_replay/actions.hpp \
	rbd_replay/Deser.hpp \
	rbd_replay/ImageNameMap.hpp \
	rbd_replay/PendingIO.hpp \
	rbd_replay/rbd_loc.hpp \
	rbd_replay/rbd_replay_debug.hpp \
	rbd_replay/Replayer.hpp

rbd_replay_SOURCES = rbd_replay/rbd-replay.cc
rbd_replay_LDADD = $(LIBRBD) \
	$(LIBRADOS) \
	$(CEPH_GLOBAL) \
	librbd_replay.la

if LINUX
bin_PROGRAMS += rbd-replay
endif #LINUX
