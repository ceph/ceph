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
target_link_libraries(rbd_replay LINK_PRIVATE librbd librados global)

add_executable(rbd-replay
  rbd-replay.cc
  $<TARGET_OBJECTS:parse_secret_objs>
  )
target_link_libraries(rbd-replay librbd librados global rbd_replay rbd_replay_types common keyutils)
install(TARGETS rbd-replay DESTINATION bin)

set(librbd_replay_ios_srcs
    ios.cc)
add_library(rbd_replay_ios STATIC ${librbd_replay_ios_srcs})
target_link_libraries(rbd_replay_ios librbd librados global)

if(${WITH_BABELTRACE})
  set(rbd_replay_prep_srcs
      rbd-replay-prep.cc)
  add_executable(rbd-replay-prep
    ${rbd_replay_prep_srcs}
    $<TARGET_OBJECTS:parse_secret_objs>
    )
  target_link_libraries(rbd-replay-prep
    rbd_replay
    rbd_replay_ios
    rbd_replay_types
    librbd
    librados
    common
    keyutils
    global
    babeltrace
    babeltrace-ctf
    udev
    ${Boost_DATE_TIME_LIBRARY}
    )
  install(TARGETS rbd-replay-prep DESTINATION bin)
endif(${WITH_BABELTRACE})

install(TARGETS rbd_replay rbd_replay_ios DESTINATION lib)

