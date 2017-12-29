set(librbd_replay_types_srcs
    ActionTypes.cc)
add_library(rbd_replay_types STATIC ${librbd_replay_types_srcs})

set(librbd_replay_srcs
    actions.cc
    BufferReader.cc
    ImageNameMap.cc
    PendingIO.cc
    rbd_loc.cc
    Replayer.cc)
add_library(rbd_replay STATIC ${librbd_replay_srcs})
target_link_libraries(rbd_replay PRIVATE librbd librados global)

add_executable(rbd-replay
  rbd-replay.cc)
target_link_libraries(rbd-replay
  librbd librados global rbd_replay rbd_replay_types ceph-common)
install(TARGETS rbd-replay DESTINATION bin)

set(librbd_replay_ios_srcs
    ios.cc)
add_library(rbd_replay_ios STATIC ${librbd_replay_ios_srcs})
target_link_libraries(rbd_replay_ios librbd librados global)

if(HAVE_BABELTRACE)
  add_executable(rbd-replay-prep
    rbd-replay-prep.cc)
  target_link_libraries(rbd-replay-prep
    rbd_replay
    rbd_replay_ios
    rbd_replay_types
    librbd
    librados
    ceph-common
    global
    babeltrace
    babeltrace-ctf
    Boost::date_time
    )
  install(TARGETS rbd-replay-prep DESTINATION bin)
endif(HAVE_BABELTRACE)

