install(FILES
  ceph.target
  ceph-osd.target
  ceph-mgr.target
  ceph-mon.target
  ceph-mds.target
  ceph-radosgw.target
  ceph-rbd-mirror.target
  ceph-mds@.service
  ceph-mgr@.service
  ceph-mon@.service
  ceph-create-keys@.service
  ceph-osd@.service
  ceph-radosgw@.service
  ceph-rbd-mirror@.service
  ceph-disk@.service
  rbdmap.service
  DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/systemd/system)
