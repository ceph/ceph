list(APPEND man_srcs
  ceph-osd.rst
  ceph-mds.rst
  ceph-mon.rst
  ceph-syn.rst
  crushtool.rst
  osdmaptool.rst
  monmaptool.rst
  ceph-conf.rst
  ceph-run.rst
  ceph.rst
  mount.ceph.rst
  ceph-create-keys.rst
  radosgw.rst
  radosgw-admin.rst
  ceph-authtool.rst
  rados.rst
  librados-config.rst
  ceph-clsinfo.rst
  ceph-debugpack.rst
  cephfs.rst
  ceph-dencoder.rst
  ceph-rest-api.rst
  ceph-post-file.rst)

if(HAVE_LIBFUSE)
  list(APPEND man_srcs
    ceph-fuse.rst
    rbd-fuse.rst)
endif()

if(WITH_RBD)
  list(APPEND man_srcs
	ceph-rbdnamer.rst
	rbd-nbd.rst
	rbd-replay-prep.rst
	rbd-replay.rst
	rbdmap.rst
	rbd.rst)
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
