add_executable(rbd-fuse
  rbd-fuse.cc)
target_link_libraries(rbd-fuse
  librbd librados common ${FUSE_LIBRARIES})
install(TARGETS rbd-fuse DESTINATION bin)
