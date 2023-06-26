add_executable(unittest_exporter
  test_exporter.cc
  "${CMAKE_SOURCE_DIR}/src/exporter/util.cc"
  "${CMAKE_SOURCE_DIR}/src/exporter/DaemonMetricCollector.cc"
  )

target_link_libraries(unittest_exporter
  global
  ${UNITTEST_LIBS}
  )
add_ceph_unittest(unittest_exporter)

