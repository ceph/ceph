set(mount_ceph_srcs
  mount.ceph.c conf.cc)
add_executable(mount.ceph ${mount_ceph_srcs}
  $<TARGET_OBJECTS:parse_secret_objs>
  $<TARGET_OBJECTS:common_mountcephfs_objs>)
target_link_libraries(mount.ceph keyutils::keyutils ${CAPNG_LIBRARIES} global ceph-common)
install(TARGETS mount.ceph DESTINATION ${CMAKE_INSTALL_SBINDIR})
