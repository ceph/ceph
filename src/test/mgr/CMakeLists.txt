# unittest_mgr_clusterstate
add_executable(unittest_mgr_clusterstate
  test_clusterstate.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/ClusterState.cc
)
add_ceph_unittest(unittest_mgr_clusterstate)
target_link_libraries(unittest_mgr_clusterstate
  ceph-common
  global
  Python3::Python
)

# unittest_mgr_daemonkey
add_executable(unittest_mgr_daemonkey
  test_daemonkey.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonKey.cc
)
add_ceph_unittest(unittest_mgr_daemonkey)
target_link_libraries(unittest_mgr_daemonkey
  ceph-common
)

# unittest_mgr_daemonstate
add_executable(unittest_mgr_daemonstate
  test_daemonstate.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonState.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonKey.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonPerfCounters.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/PerfCounterInstance.cc
)
add_ceph_unittest(unittest_mgr_daemonstate)
target_link_libraries(unittest_mgr_daemonstate
  ceph-common
  Python3::Python
)

# unittest_mgr_mgrcap
add_executable(unittest_mgr_mgrcap
  test_mgrcap.cc
  $<TARGET_OBJECTS:mgr_cap_obj>)
add_ceph_unittest(unittest_mgr_mgrcap)
target_link_libraries(unittest_mgr_mgrcap global)

# unittest_mgr_pyformatter
add_executable(unittest_mgr_pyformatter
  test_pyformatter.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/PyFormatter.cc
)
add_ceph_unittest(unittest_mgr_pyformatter)
target_link_libraries(unittest_mgr_pyformatter
  ceph-common
  Python3::Python
)

# unittest_mgr_pyutil
add_executable(unittest_mgr_pyutil
  test_pyutil.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/PyUtil.cc
)
add_ceph_unittest(unittest_mgr_pyutil)
target_link_libraries(unittest_mgr_pyutil
  ceph-common
  Python3::Python
)

# unittest_mgr_servicemap
add_executable(unittest_mgr_servicemap
  test_servicemap.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/ServiceMap.cc
)
add_ceph_unittest(unittest_mgr_servicemap)
target_link_libraries(unittest_mgr_servicemap
  ceph-common
)

# unittest_mgr_map_cache
add_executable(unittest_mgr_map_cache
  test_mgrmapcache.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/MgrMapCache.cc)
add_ceph_unittest(unittest_mgr_map_cache)
target_link_libraries(unittest_mgr_map_cache ceph-common
  Python3::Python ${CMAKE_DL_LIBS} ${GSSAPI_LIBRARIES})

# unittest_mgr_pymodule
#
# Scaffolding for building PyModule against its full link closure is
# cherry-picked from ceph/ceph PR #71237 (pending its merge) as a temporary
# measure; reconcile this block with that PR's CMake changes once it lands.
set(unittest_mgr_pymodule_srcs
  ${CMAKE_SOURCE_DIR}/src/mgr/ActivePyModule.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/ActivePyModules.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/BaseMgrModule.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/BaseMgrStandbyModule.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/ClusterState.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonHealthMetricCollector.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonHealthMetric.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonKey.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonPerfCounters.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonServer.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonState.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/Gil.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/MDSPerfMetricCollector.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/MDSPerfMetricTypes.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/MetricCollector.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/MgrMapCache.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/MgrOpRequest.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/OSDPerfMetricCollector.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/OSDPerfMetricTypes.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/PerfCounterInstance.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/PyFormatter.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/PyModule.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/PyModuleRegistry.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/PyModuleRunner.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/PyOSDMap.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/PyUtil.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/ServiceMap.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/StandbyPyModules.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/ThreadMonitor.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/mgr_commands.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/mgr_perf_counters.cc
)

add_library(unittest_mgr_pymodule_obj OBJECT ${unittest_mgr_pymodule_srcs})
target_include_directories(unittest_mgr_pymodule_obj PRIVATE
  $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:Python3::Python,INTERFACE_INCLUDE_DIRECTORIES>)
target_compile_definitions(unittest_mgr_pymodule_obj PRIVATE UNIT_TESTS_BUILT)
target_link_libraries(unittest_mgr_pymodule_obj legacy-option-headers)
if(WITH_LTTNG)
  add_dependencies(unittest_mgr_pymodule_obj mgr_op_tp)
endif()

add_executable(unittest_mgr_pymodule
  test_pymodule.cc
  $<TARGET_OBJECTS:unittest_mgr_pymodule_obj>
  $<TARGET_OBJECTS:mgr_cap_obj>
  ${CMAKE_SOURCE_DIR}/src/mgr/Mgr.cc
)
add_ceph_unittest(unittest_mgr_pymodule)
target_compile_definitions(unittest_mgr_pymodule PRIVATE UNIT_TESTS_BUILT)
target_include_directories(unittest_mgr_pymodule PRIVATE
  $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
if(WITH_LTTNG)
  add_dependencies(unittest_mgr_pymodule mgr_op_tp)
endif()
target_link_libraries(unittest_mgr_pymodule
  ceph-common
  osdc
  mon
  global
  Boost::python${MGR_PYTHON_VERSION_MAJOR}${MGR_PYTHON_VERSION_MINOR}
  Python3::Python
  ${CMAKE_DL_LIBS}
  $<$<BOOL:${WITH_LIBCEPHSQLITE}>:cephsqlite>
  $<$<BOOL:${WITH_LIBCEPHSQLITE}>:SQLite3::SQLite3>
)

#scripts
if(WITH_MGR_DASHBOARD_FRONTEND)
  if(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm|ARM")
    add_ceph_test(mgr-dashboard-frontend-unittests ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard/run-frontend-unittests.sh)
  endif(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm|ARM")

  add_ceph_test(mgr-dashboard-smoke.sh ${CMAKE_CURRENT_SOURCE_DIR}/mgr-dashboard-smoke.sh)
endif(WITH_MGR_DASHBOARD_FRONTEND)
