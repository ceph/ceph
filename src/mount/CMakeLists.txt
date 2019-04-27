set(mount_ceph_srcs
  mount.ceph.c)
add_executable(mount.ceph ${mount_ceph_srcs}
  $<TARGET_OBJECTS:parse_secret_objs>
  $<TARGET_OBJECTS:common_mountcephfs_objs>)
set_target_properties(mount.ceph PROPERTIES
  INSTALL_RPATH "")
target_link_libraries(mount.ceph keyutils::keyutils)
install(TARGETS mount.ceph DESTINATION ${CMAKE_INSTALL_SBINDIR})
