add_custom_target(
  ceph.pp ALL
  COMMAND ln -sf ${CMAKE_CURRENT_SOURCE_DIR}/ceph.* .
  COMMAND make -f ${SELINUX_MAKEFILE} ceph.pp
  DEPENDS ceph.te ceph.fc ceph.if)

set(datadir ${CMAKE_INSTALL_PREFIX}/share/selinux)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/ceph.pp
  DESTINATION ${datadir}/packages)
install(FILES ceph.if
  DESTINATION ${datadir}/devel/include/contrib)
