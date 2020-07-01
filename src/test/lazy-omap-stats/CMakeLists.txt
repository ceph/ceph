# Lazy omap stat collection tests

add_executable(ceph_test_lazy_omap_stats
  main.cc
  lazy_omap_stats_test.cc)
target_link_libraries(ceph_test_lazy_omap_stats
  librados ceph-common ${UNITTEST_LIBS} Boost::system)
install(TARGETS
  ceph_test_lazy_omap_stats
  DESTINATION ${CMAKE_INSTALL_BINDIR})
