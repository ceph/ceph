if ENABLE_CLIENT
if WITH_RADOS
if WITH_RBD

librbd_replay_types_la_SOURCES = \
	rbd_replay/ActionTypes.cc
noinst_HEADERS += \
        rbd_replay/ActionTypes.h
noinst_LTLIBRARIES += librbd_replay_types.la
DENCODER_DEPS += librbd_replay_types.la

# librbd_replay_la exists only to help with unit tests
librbd_replay_la_SOURCES = \
	rbd_replay/actions.cc \
	rbd_replay/BufferReader.cc \
	rbd_replay/ImageNameMap.cc \
	rbd_replay/PendingIO.cc \
	rbd_replay/rbd_loc.cc \
	rbd_replay/Replayer.cc
librbd_replay_la_LIBADD = \
	$(LIBRBD) \
	$(LIBRADOS) \
	$(CEPH_GLOBAL)
noinst_LTLIBRARIES += librbd_replay.la
noinst_HEADERS += \
	rbd_replay/actions.hpp \
	rbd_replay/BoundedBuffer.hpp \
	rbd_replay/BufferReader.h \
	rbd_replay/ImageNameMap.hpp \
	rbd_replay/ios.hpp \
	rbd_replay/PendingIO.hpp \
	rbd_replay/rbd_loc.hpp \
	rbd_replay/rbd_replay_debug.hpp \
	rbd_replay/Replayer.hpp

rbd_replay_SOURCES = \
	rbd_replay/rbd-replay.cc
rbd_replay_LDADD = \
	librbd_replay.la \
	librbd_replay_types.la \
	$(LIBRBD) \
	$(LIBRADOS) \
	$(CEPH_GLOBAL) \
	$(LIBCOMMON)

if LINUX
bin_PROGRAMS += rbd-replay
endif #LINUX

librbd_replay_ios_la_SOURCES = rbd_replay/ios.cc
librbd_replay_ios_la_LIBADD = $(LIBRBD) \
	$(LIBRADOS) \
	$(CEPH_GLOBAL) \
	librbd_replay.la
noinst_LTLIBRARIES += librbd_replay_ios.la

rbd_replay_prep_SOURCES = \
	rbd_replay/rbd-replay-prep.cc
rbd_replay_prep_LDADD = \
	librbd_replay.la \
	librbd_replay_ios.la \
	librbd_replay_types.la \
	$(LIBRBD) \
	$(LIBRADOS) \
	$(CEPH_GLOBAL) \
	$(LIBCOMMON) \
	-lbabeltrace \
	-lbabeltrace-ctf \
	-lboost_date_time
if WITH_BABELTRACE
bin_PROGRAMS += rbd-replay-prep
endif

endif # WITH_RBD
endif # WITH_RADOS
endif # ENABLE_CLIENT

EXTRA_DIST += rbd_replay/CMakeLists.txt
