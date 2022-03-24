set(exporter_srcs
  ${CMAKE_SOURCE_DIR}/src/ceph_exporter.cc
  DaemonMetricCollector.cc
  http_server.cc
  )
add_executable(ceph-exporter ${exporter_srcs})
target_link_libraries(ceph-exporter
  global-static ceph-common)
install(TARGETS ceph-exporter DESTINATION bin)
