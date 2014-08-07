# librbd_replay_la exists only to help with unit tests
librbd_replay_la_SOURCES = rbd_replay/actions.cc \
	rbd_replay/Deser.cc \
	rbd_replay/ImageNameMap.cc \
	rbd_replay/PendingIO.cc \
	rbd_replay/rbd_loc.cc \
	rbd_replay/Replayer.cc \
	rbd_replay/Ser.cc
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
	rbd_replay/Replayer.hpp \
	rbd_replay/Ser.hpp


rbd_replay_SOURCES = rbd_replay/rbd-replay.cc
rbd_replay_LDADD = $(LIBRBD) \
	$(LIBRADOS) \
	$(CEPH_GLOBAL) \
	librbd_replay.la

if LINUX
bin_PROGRAMS += rbd-replay
endif #LINUX

# TODO: See if we need any new dependencies
rbd_replay_prep_SOURCES = rbd_replay/rbd-replay-prep.cc
rbd_replay_prep_LDADD = $(LIBRBD) \
	$(LIBRADOS) \
	$(CEPH_GLOBAL) \
	librbd_replay.la \
	-lbabeltrace \
	-lbabeltrace-ctf \
	-lboost_date_time
bin_PROGRAMS += rbd-replay-prep
