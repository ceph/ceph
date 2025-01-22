set(exporter_srcs
  ceph_exporter.cc
  DaemonMetricCollector.cc
  web_server.cc
  util.cc
  )
add_executable(ceph-exporter ${exporter_srcs})
target_link_libraries(ceph-exporter
  global-static
  ceph-common
  OpenSSL::SSL)
install(TARGETS ceph-exporter DESTINATION bin)
