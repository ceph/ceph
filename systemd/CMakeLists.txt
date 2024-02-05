find_package(PkgConfig QUIET REQUIRED)
pkg_get_variable(SYSTEMD_SYSTEM_UNIT_DIR "systemd" "systemd_system_unit_dir")
if(NOT SYSTEMD_SYSTEM_UNIT_DIR)
  pkg_get_variable(SYSTEMD_SYSTEM_UNIT_DIR "systemd" "systemdsystemunitdir")
  if(NOT SYSTEMD_SYSTEM_UNIT_DIR)
    set(SYSTEMD_SYSTEM_UNIT_DIR "${CMAKE_INSTALL_LIBDIR}/systemd/system")
  endif()
endif()
set(SYSTEMD_SYSTEM_UNIT_DIR "${SYSTEMD_SYSTEM_UNIT_DIR}"
  CACHE PATH "Location for systemd system unit files")

set(CEPH_SYSTEMD_ENV_DIR "/etc/sysconfig"
  CACHE PATH "Location for systemd service environmental variable settings files")
set(SYSTEMD_ENV_FILE "${CEPH_SYSTEMD_ENV_DIR}/ceph")
foreach(service
    ceph-crash
    ceph-exporter
    ceph-fuse@
    ceph-mds@
    ceph-mgr@
    ceph-mon@
    ceph-osd@
    ceph-radosgw@
    ceph-rbd-mirror@
    ceph-immutable-object-cache@
    cephfs-mirror@
    rbdmap)
  configure_file(
    ${service}.service.in
    ${service}.service
    @ONLY)
  install(
    FILES ${CMAKE_CURRENT_BINARY_DIR}/${service}.service
    DESTINATION ${SYSTEMD_SYSTEM_UNIT_DIR})
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
  cephfs-mirror.target
  DESTINATION ${SYSTEMD_SYSTEM_UNIT_DIR})
