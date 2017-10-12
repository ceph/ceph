set(client_srcs
  ceph-syn.rst
  ceph-conf.rst
  ceph.rst
  ceph-authtool.rst
  ceph-kvstore-tool.rst
  rados.rst
  ceph-post-file.rst
  ceph-dencoder.rst)

set(server_srcs
  ceph-deploy.rst
  crushtool.rst
  ceph-run.rst
  mount.ceph.rst
  ceph-create-keys.rst
  ceph-rest-api.rst)
if(WITH_TESTS)
list(APPEND server_srcs
  ceph-debugpack.rst)
endif(WITH_TESTS)

set(osd_srcs
  ceph-clsinfo.rst
  ceph-detect-init.rst
  ceph-disk.rst
  ceph-volume.rst
  ceph-volume-systemd.rst
  ceph-osd.rst
  osdmaptool.rst
  ceph-bluestore-tool.rst)

set(mon_srcs
  ceph-mon.rst
  monmaptool.rst)

list(APPEND man_srcs
  ${client_srcs}
  ${server_srcs}
  ${osd_srcs}
  ${mon_srcs}
  ceph-mds.rst
  librados-config.rst)

if(HAVE_LIBFUSE)
  list(APPEND man_srcs
    ceph-fuse.rst
    rbd-fuse.rst)
endif()

if(WITH_RADOSGW)
  list(APPEND man_srcs
	radosgw.rst
	radosgw-admin.rst)
endif()

if(WITH_RBD)
  list(APPEND man_srcs
	ceph-rbdnamer.rst
	rbd-mirror.rst
	rbd-replay-many.rst
	rbd-replay-prep.rst
	rbd-replay.rst
	rbdmap.rst
	rbd.rst)
  if(LINUX)
    list(APPEND man_srcs rbd-nbd.rst)
  endif()
  if(FREEBSD)
    list(APPEND man_srcs rbd-ggate.rst)
  endif()
endif()

foreach(man ${man_srcs})
  list(APPEND sphinx_input ${CMAKE_CURRENT_SOURCE_DIR}/${man})
  # mount.ceph.rst => mount if we use
  # get_filename_component(cmd ${man} NAME_WE)
  string(REGEX REPLACE ".rst$" "" cmd ${man})
  list(APPEND sphinx_output ${sphinx_output_dir}/${cmd}.8)
  install(FILES ${sphinx_output_dir}/${cmd}.8
    DESTINATION ${CEPH_MAN_DIR}/man8)
endforeach()

set(sphinx_input ${sphinx_input} PARENT_SCOPE)
set(sphinx_output ${sphinx_output} PARENT_SCOPE)
