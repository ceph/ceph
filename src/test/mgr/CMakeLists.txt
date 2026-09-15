set(unittest_mgr_base_srcs
  ${CMAKE_SOURCE_DIR}/src/mgr/ClusterState.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonKey.cc
)

set(unittest_mgr_daemonstate_srcs
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonState.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonPerfCounters.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/PerfCounterInstance.cc
)

set(unittest_mgr_python_helpers_srcs
  ${CMAKE_SOURCE_DIR}/src/mgr/PyFormatter.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/PyUtil.cc
)

set(unittest_mgr_servicemap_srcs
  ${CMAKE_SOURCE_DIR}/src/mgr/ServiceMap.cc
)

set(unittest_mgr_map_cache_srcs
  ${CMAKE_SOURCE_DIR}/src/mgr/MgrMapCache.cc
)

set(unittest_mgr_perfcounterinstance_srcs
  ${CMAKE_SOURCE_DIR}/src/mgr/PerfCounterInstance.cc
)

set(unittest_mgr_metriccollector_srcs
  ${CMAKE_SOURCE_DIR}/src/mgr/MetricCollector.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/OSDPerfMetricCollector.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/OSDPerfMetricTypes.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/MDSPerfMetricCollector.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/MDSPerfMetricTypes.cc
)

add_library(unittest_mgr_base_obj OBJECT ${unittest_mgr_base_srcs})
target_link_libraries(unittest_mgr_base_obj legacy-option-headers)

add_library(unittest_mgr_daemonstate_obj OBJECT ${unittest_mgr_daemonstate_srcs})
target_link_libraries(unittest_mgr_daemonstate_obj legacy-option-headers)

add_library(unittest_mgr_python_helpers_obj OBJECT ${unittest_mgr_python_helpers_srcs})
target_include_directories(unittest_mgr_python_helpers_obj PRIVATE
  $<TARGET_PROPERTY:Python3::Python,INTERFACE_INCLUDE_DIRECTORIES>)
target_link_libraries(unittest_mgr_python_helpers_obj legacy-option-headers)

add_library(unittest_mgr_servicemap_obj OBJECT ${unittest_mgr_servicemap_srcs})
target_link_libraries(unittest_mgr_servicemap_obj legacy-option-headers)

add_library(unittest_mgr_map_cache_obj OBJECT ${unittest_mgr_map_cache_srcs})
target_include_directories(unittest_mgr_map_cache_obj PRIVATE
  $<TARGET_PROPERTY:Python3::Python,INTERFACE_INCLUDE_DIRECTORIES>)
target_link_libraries(unittest_mgr_map_cache_obj legacy-option-headers)

add_library(unittest_mgr_perfcounterinstance_obj OBJECT ${unittest_mgr_perfcounterinstance_srcs})
target_link_libraries(unittest_mgr_perfcounterinstance_obj legacy-option-headers)

add_library(unittest_mgr_metriccollector_obj OBJECT ${unittest_mgr_metriccollector_srcs})
target_link_libraries(unittest_mgr_metriccollector_obj legacy-option-headers)

# unittest_mgr_clusterstate
add_executable(unittest_mgr_clusterstate
  test_clusterstate.cc
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
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
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
)
add_ceph_unittest(unittest_mgr_daemonkey)
target_link_libraries(unittest_mgr_daemonkey
  ceph-common
)

# unittest_mgr_daemonstate
add_executable(unittest_mgr_daemonstate
  test_daemonstate.cc
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
  $<TARGET_OBJECTS:unittest_mgr_daemonstate_obj>
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
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
  $<TARGET_OBJECTS:unittest_mgr_python_helpers_obj>
)
add_ceph_unittest(unittest_mgr_pyformatter)
target_link_libraries(unittest_mgr_pyformatter
  ceph-common
  Python3::Python
)

# unittest_mgr_pyutil
add_executable(unittest_mgr_pyutil
  test_pyutil.cc
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
  $<TARGET_OBJECTS:unittest_mgr_python_helpers_obj>
)
add_ceph_unittest(unittest_mgr_pyutil)
target_link_libraries(unittest_mgr_pyutil
  ceph-common
  Python3::Python
)

# unittest_mgr_servicemap
add_executable(unittest_mgr_servicemap
  test_servicemap.cc
  $<TARGET_OBJECTS:unittest_mgr_servicemap_obj>
)
add_ceph_unittest(unittest_mgr_servicemap)
target_link_libraries(unittest_mgr_servicemap
  ceph-common
)

# unittest_mgr_map_cache
add_executable(unittest_mgr_map_cache
  test_mgrmapcache.cc
  $<TARGET_OBJECTS:unittest_mgr_map_cache_obj>)
add_ceph_unittest(unittest_mgr_map_cache)
target_link_libraries(unittest_mgr_map_cache ceph-common
  Python3::Python ${CMAKE_DL_LIBS} ${GSSAPI_LIBRARIES})

# unittest_mgr_daemonperfcounters
add_executable(unittest_mgr_daemonperfcounters
  test_daemonperfcounters.cc
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
  $<TARGET_OBJECTS:unittest_mgr_daemonstate_obj>
)
add_ceph_unittest(unittest_mgr_daemonperfcounters)
target_link_libraries(unittest_mgr_daemonperfcounters
  ceph-common
  Python3::Python
)

# unittest_mgr_perfcounterinstance
add_executable(unittest_mgr_perfcounterinstance
  test_perfcounterinstance.cc
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
  $<TARGET_OBJECTS:unittest_mgr_perfcounterinstance_obj>
)
add_ceph_unittest(unittest_mgr_perfcounterinstance)
target_link_libraries(unittest_mgr_perfcounterinstance
  ceph-common
  Python3::Python
)

# unittest_mgr_metriccollector
add_executable(unittest_mgr_metriccollector
  test_metriccollector.cc
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
  $<TARGET_OBJECTS:unittest_mgr_metriccollector_obj>
)
add_ceph_unittest(unittest_mgr_metriccollector)
target_link_libraries(unittest_mgr_metriccollector
  ceph-common
  global
  Python3::Python
)

# unittest_mgr_osdperfmetriccollector
add_executable(unittest_mgr_osdperfmetriccollector
  test_osdperfmetriccollector.cc
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
  $<TARGET_OBJECTS:unittest_mgr_metriccollector_obj>
)
add_ceph_unittest(unittest_mgr_osdperfmetriccollector)
target_link_libraries(unittest_mgr_osdperfmetriccollector
  ceph-common
  global
  Python3::Python
)

# unittest_mgr_mdsperfmetriccollector
add_executable(unittest_mgr_mdsperfmetriccollector
  test_mdsperfmetriccollector.cc
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
  $<TARGET_OBJECTS:unittest_mgr_metriccollector_obj>
)
add_ceph_unittest(unittest_mgr_mdsperfmetriccollector)
target_link_libraries(unittest_mgr_mdsperfmetriccollector
  ceph-common
  global
  Python3::Python
)

# unittest_mgr_daemonhealthmetriccollector
add_executable(unittest_mgr_daemonhealthmetriccollector
  test_daemonhealthmetriccollector.cc
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
  $<TARGET_OBJECTS:unittest_mgr_metriccollector_obj>
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonHealthMetricCollector.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/DaemonHealthMetric.cc
)
add_ceph_unittest(unittest_mgr_daemonhealthmetriccollector)
target_link_libraries(unittest_mgr_daemonhealthmetriccollector
  ceph-common
  global
  Python3::Python
)

set(unittest_mgr_common_srcs
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

add_library(unittest_mgr_common_obj OBJECT ${unittest_mgr_common_srcs})
target_include_directories(unittest_mgr_common_obj PRIVATE
  $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:Python3::Python,INTERFACE_INCLUDE_DIRECTORIES>)
target_compile_definitions(unittest_mgr_common_obj PRIVATE UNIT_TESTS_BUILT)
target_link_libraries(unittest_mgr_common_obj legacy-option-headers)
if(WITH_LTTNG)
  add_dependencies(unittest_mgr_common_obj mgr_op_tp)
endif()

set(unittest_mgr_common_objs
  $<TARGET_OBJECTS:unittest_mgr_common_obj>
  $<TARGET_OBJECTS:mgr_cap_obj>)

set(unittest_mgr_common_with_mgr_objs
  ${unittest_mgr_common_objs}
  ${CMAKE_SOURCE_DIR}/src/mgr/Mgr.cc)

# unittest_mgr_pyosdmap
add_executable(unittest_mgr_pyosdmap
  test_pyosdmap.cc
  ${unittest_mgr_common_with_mgr_objs}
)
add_ceph_unittest(unittest_mgr_pyosdmap)
target_compile_definitions(unittest_mgr_pyosdmap PRIVATE UNIT_TESTS_BUILT)
target_include_directories(unittest_mgr_pyosdmap PRIVATE
  $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
if(WITH_LTTNG)
  add_dependencies(unittest_mgr_pyosdmap mgr_op_tp)
endif()
target_link_libraries(unittest_mgr_pyosdmap
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

# unittest_mgr_mgrclient
add_executable(unittest_mgr_mgrclient
  test_mgrclient.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/MgrClient.cc
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
)
add_ceph_unittest(unittest_mgr_mgrclient)
target_link_libraries(unittest_mgr_mgrclient
  ceph-common
  global
  Python3::Python
)

# unittest_mgr_threadmonitor
add_executable(unittest_mgr_threadmonitor
  test_threadmonitor.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/ThreadMonitor.cc
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
)
add_ceph_unittest(unittest_mgr_threadmonitor)
target_link_libraries(unittest_mgr_threadmonitor
  ceph-common
  global
  Python3::Python
)

# unittest_mgr_mgroprequest
add_executable(unittest_mgr_mgroprequest
  test_mgroprequest.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/MgrOpRequest.cc
  $<TARGET_OBJECTS:unittest_mgr_base_obj>
)
add_ceph_unittest(unittest_mgr_mgroprequest)
if(WITH_LTTNG)
  add_dependencies(unittest_mgr_mgroprequest mgr_op_tp)
endif()
target_link_libraries(unittest_mgr_mgroprequest
  ceph-common
  global
  Python3::Python
)

# unittest_mgr_pymodulerunner
add_executable(unittest_mgr_pymodulerunner
  test_pymodulerunner.cc
  ${unittest_mgr_common_with_mgr_objs}
)
add_ceph_unittest(unittest_mgr_pymodulerunner)
target_compile_definitions(unittest_mgr_pymodulerunner PRIVATE UNIT_TESTS_BUILT)
target_include_directories(unittest_mgr_pymodulerunner PRIVATE
  $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
if(WITH_LTTNG)
  add_dependencies(unittest_mgr_pymodulerunner mgr_op_tp)
endif()
target_link_libraries(unittest_mgr_pymodulerunner
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

# unittest_mgr_pymoduleregistry
add_executable(unittest_mgr_pymoduleregistry
  test_pymoduleregistry.cc
  ${unittest_mgr_common_with_mgr_objs}
)
add_ceph_unittest(unittest_mgr_pymoduleregistry)
target_compile_definitions(unittest_mgr_pymoduleregistry PRIVATE UNIT_TESTS_BUILT)
target_include_directories(unittest_mgr_pymoduleregistry PRIVATE
  $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
if(WITH_LTTNG)
  add_dependencies(unittest_mgr_pymoduleregistry mgr_op_tp)
endif()
target_link_libraries(unittest_mgr_pymoduleregistry
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

# unittest_mgr_standbypymodules
add_executable(unittest_mgr_standbypymodules
  test_standbypymodules.cc
  ${unittest_mgr_common_with_mgr_objs}
)
add_ceph_unittest(unittest_mgr_standbypymodules)
target_compile_definitions(unittest_mgr_standbypymodules PRIVATE UNIT_TESTS_BUILT)
target_include_directories(unittest_mgr_standbypymodules PRIVATE
  $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
if(WITH_LTTNG)
  add_dependencies(unittest_mgr_standbypymodules mgr_op_tp)
endif()
target_link_libraries(unittest_mgr_standbypymodules
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

# unittest_mgr_daemonserver
add_executable(unittest_mgr_daemonserver
  test_daemonserver.cc
  ${unittest_mgr_common_with_mgr_objs}
)
add_ceph_unittest(unittest_mgr_daemonserver)
target_compile_definitions(unittest_mgr_daemonserver PRIVATE UNIT_TESTS_BUILT)
target_include_directories(unittest_mgr_daemonserver PRIVATE
  $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
if(WITH_LTTNG)
  add_dependencies(unittest_mgr_daemonserver mgr_op_tp)
endif()
target_link_libraries(unittest_mgr_daemonserver
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

# unittest_mgr_mgrstandby
add_executable(unittest_mgr_mgrstandby
  test_mgrstandby.cc
  ${unittest_mgr_common_with_mgr_objs}
  ${CMAKE_SOURCE_DIR}/src/mgr/MgrStandby.cc
)
add_ceph_unittest(unittest_mgr_mgrstandby)
target_compile_definitions(unittest_mgr_mgrstandby PRIVATE UNIT_TESTS_BUILT)
target_include_directories(unittest_mgr_mgrstandby PRIVATE
  $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
if(WITH_LTTNG)
  add_dependencies(unittest_mgr_mgrstandby mgr_op_tp)
endif()
target_link_libraries(unittest_mgr_mgrstandby
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

# unittest_mgr_mgr
add_executable(unittest_mgr_mgr
  test_mgr.cc
  ${unittest_mgr_common_with_mgr_objs}
)
add_ceph_unittest(unittest_mgr_mgr)
target_compile_definitions(unittest_mgr_mgr PRIVATE UNIT_TESTS_BUILT)
target_include_directories(unittest_mgr_mgr PRIVATE
  $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
if(WITH_LTTNG)
  add_dependencies(unittest_mgr_mgr mgr_op_tp)
endif()
target_link_libraries(unittest_mgr_mgr
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
