include(GNUInstallDirs)

configure_file(${CMAKE_SOURCE_DIR}/systemd/ceph-osd@.service.in
  ${CMAKE_CURRENT_BINARY_DIR}/ceph-osd@.service @ONLY)

install(FILES
  ${CMAKE_BINARY_DIR}/systemd/ceph-osd@.service
  DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/systemd/system)

install(FILES
  ceph.target
  ceph-fuse.target
  ceph-osd.target
  ceph-mgr.target
  ceph-mon.target
  ceph-mds.target
  ceph-radosgw.target
  ceph-rbd-mirror.target
  ceph-fuse@.service
  ceph-mds@.service
  ceph-mgr@.service
  ceph-mon@.service
  ceph-radosgw@.service
  ceph-rbd-mirror@.service
  ceph-disk@.service
  rbdmap.service
  DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/systemd/system)
