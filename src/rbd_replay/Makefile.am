rbd_replay_SOURCES = rbd_replay/rbd-replay.cc \
	rbd_replay/actions.cc \
	rbd_replay/Deser.cc \
	rbd_replay/ImageNameMap.cc \
	rbd_replay/NameMap.cc \
	rbd_replay/PendingIO.cc \
	rbd_replay/Replayer.cc
rbd_replay_LDADD = $(LIBRBD) \
	$(LIBRADOS) \
	$(CEPH_GLOBAL) \
	-lboost_regex
noinst_HEADERS += rbd_replay/BoundedBuffer.hpp \
	rbd_replay/actions.hpp \
	rbd_replay/Deser.hpp \
	rbd_replay/ImageNameMap.hpp \
	rbd_replay/NameMap.hpp \
	rbd_replay/PendingIO.hpp \
	rbd_replay/rbd_replay_debug.hpp \
	rbd_replay/Replayer.hpp
if LINUX
bin_PROGRAMS += rbd-replay
endif #LINUX
