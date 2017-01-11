add_executable(rbd-fuse
  rbd-fuse.cc)
target_link_libraries(rbd-fuse
  ceph-common librbd librados ${FUSE_LIBRARIES})
install(TARGETS rbd-fuse DESTINATION bin)
