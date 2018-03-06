#scripts
add_ceph_test(mgr-dashboard-smoke.sh ${CMAKE_CURRENT_SOURCE_DIR}/mgr-dashboard-smoke.sh)

if(WITH_MGR_DASHBOARD_V2_FRONTEND AND NOT CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm|ARM")
add_ceph_test(mgr-dashboard_v2-frontend-unittests ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard_v2/run-frontend-unittests.sh)
endif(WITH_MGR_DASHBOARD_V2_FRONTEND AND NOT CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm|ARM")

add_ceph_test(mgr-dashboard_v2-smoke.sh ${CMAKE_CURRENT_SOURCE_DIR}/mgr-dashboard_v2-smoke.sh)

