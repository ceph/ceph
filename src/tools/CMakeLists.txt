add_executable(ceph-client-debug ceph-client-debug.cc)
target_link_libraries(ceph-client-debug cephfs librados global common)
install(TARGETS ceph-client-debug DESTINATION bin)

add_executable(ceph-kvstore-tool ceph_kvstore_tool.cc)
target_link_libraries(ceph-kvstore-tool os global ${UNITTEST_CXX_FLAGS})
install(TARGETS ceph-kvstore-tool DESTINATION bin)

set(ceph_conf_srcs ceph_conf.cc)
add_executable(ceph-conf ${ceph_conf_srcs})
target_link_libraries(ceph-conf global)
install(TARGETS ceph-conf DESTINATION bin)

set(monmaptool_srcs monmaptool.cc)
add_executable(monmaptool ${monmaptool_srcs})
target_link_libraries(monmaptool global)
install(TARGETS monmaptool DESTINATION bin)

set(osdomaptool_srcs osdmaptool.cc)
add_executable(osdmaptool ${osdomaptool_srcs})
target_link_libraries(osdmaptool global)
install(TARGETS osdmaptool DESTINATION bin)

set(ceph_psim_srcs psim.cc)
add_executable(ceph_psim ${ceph_psim_srcs})
target_link_libraries(ceph_psim global)
install(TARGETS ceph_psim DESTINATION bin)

set(ceph_authtool_srcs ceph_authtool.cc)
add_executable(ceph-authtool ${ceph_authtool_srcs})
target_link_libraries(ceph-authtool global ${EXTRALIBS} ${CRYPTO_LIBS})
install(TARGETS ceph-authtool DESTINATION bin)

if(WITH_RBD)
  add_subdirectory(rbd)
  add_subdirectory(rbd_mirror)
  add_subdirectory(rbd_nbd)
endif(WITH_RBD)
