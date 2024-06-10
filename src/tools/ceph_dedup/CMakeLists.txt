if(WITH_TESTS)
set(ceph_dedup_tool_srcs ceph_dedup_tool.cc common.cc)
add_executable(ceph-dedup-tool ${ceph_dedup_tool_srcs})
target_link_libraries(ceph-dedup-tool
  librados
  global
  cls_cas_client
  cls_cas_internal)
install(TARGETS ceph-dedup-tool 
        DESTINATION ${CMAKE_INSTALL_BINDIR})

set(ceph_dedup_daemon_srcs
  ceph_dedup_daemon.cc
  common.cc)
add_executable(ceph-dedup-daemon
  ${ceph_dedup_daemon_srcs})
target_link_libraries(ceph-dedup-daemon
  librados
  global
  cls_cas_client
  cls_cas_internal)
install(TARGETS ceph-dedup-daemon
        DESTINATION ${CMAKE_INSTALL_BINDIR})
endif(WITH_TESTS)
