set(CMAKE_INSTALL_SYSTEMD_SERVICEDIR "${CMAKE_INSTALL_LIBEXECDIR}/systemd/system"
  CACHE PATH "Location for systemd service files")
set(CEPH_SYSTEMD_ENV_DIR "/etc/sysconfig"
  CACHE PATH "Location for systemd service environmental variable settings files")
set(SYSTEMD_ENV_FILE "${CEPH_SYSTEMD_ENV_DIR}/ceph")
foreach(service
    ceph-crash
    ceph-fuse@
    ceph-mds@
    ceph-mgr@
    ceph-mon@
    ceph-osd@
    ceph-radosgw@
    ceph-rbd-mirror@
    ceph-immutable-object-cache@
    rbdmap)
  configure_file(
    ${service}.service.in
    ${service}.service
    @ONLY)
  install(
    FILES ${CMAKE_CURRENT_BINARY_DIR}/${service}.service
    DESTINATION ${CMAKE_INSTALL_SYSTEMD_SERVICEDIR})
endforeach()

install(FILES
  ceph.target
  ceph-fuse.target
  ceph-osd.target
  ceph-mgr.target
  ceph-mon.target
  ceph-mds.target
  ceph-radosgw.target
  ceph-rbd-mirror.target
  ceph-immutable-object-cache.target
  ceph-volume@.service
  DESTINATION ${CMAKE_INSTALL_SYSTEMD_SERVICEDIR})
