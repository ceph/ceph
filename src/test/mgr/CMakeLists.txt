# unittest_mgr_mgrcap
add_executable(unittest_mgr_mgrcap
  test_mgrcap.cc
  $<TARGET_OBJECTS:mgr_cap_obj>)
add_ceph_unittest(unittest_mgr_mgrcap)
target_link_libraries(unittest_mgr_mgrcap global)

# unittest_mgr_ttlcache
add_executable(unittest_mgr_ttlcache test_ttlcache.cc)
add_ceph_unittest(unittest_mgr_ttlcache)
target_link_libraries(unittest_mgr_ttlcache
  Python3::Python ${CMAKE_DL_LIBS} ${GSSAPI_LIBRARIES})

#scripts
if(WITH_MGR_DASHBOARD_FRONTEND)
  if(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm|ARM")
    add_ceph_test(mgr-dashboard-frontend-unittests ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/run-frontend-unittests.sh)
  endif(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm|ARM")

  add_ceph_test(mgr-dashboard-smoke.sh ${CMAKE_CURRENT_SOURCE_DIR}/mgr-dashboard-smoke.sh)
endif(WITH_MGR_DASHBOARD_FRONTEND)
