add_library(mgr_cap_obj OBJECT
  MgrCap.cc)

if(WITH_MGR)
  set(mgr_srcs
    ${CMAKE_SOURCE_DIR}/src/ceph_mgr.cc
    ${CMAKE_SOURCE_DIR}/src/mon/PGMap.cc
    ${CMAKE_SOURCE_DIR}/src/mon/ConfigMap.cc
    ActivePyModule.cc
    ActivePyModules.cc
    BaseMgrModule.cc
    BaseMgrStandbyModule.cc
    ClusterState.cc
    DaemonHealthMetricCollector.cc
    DaemonKey.cc
    DaemonServer.cc
    DaemonState.cc
    Gil.cc
    Mgr.cc
    mgr_perf_counters.cc
    MgrStandby.cc
    MetricCollector.cc
    OSDPerfMetricTypes.cc
    OSDPerfMetricCollector.cc
    MDSPerfMetricTypes.cc
    MDSPerfMetricCollector.cc
    PyFormatter.cc
    PyUtil.cc
    PyModule.cc
    PyModuleRegistry.cc
    PyModuleRunner.cc
    PyOSDMap.cc
    StandbyPyModules.cc
    mgr_commands.cc
    MgrOpRequest.cc
    ${CMAKE_SOURCE_DIR}/src/common/TrackedOp.cc
    $<TARGET_OBJECTS:mgr_cap_obj>)
  add_executable(ceph-mgr ${mgr_srcs})
  target_compile_definitions(ceph-mgr PRIVATE PY_SSIZE_T_CLEAN)
  if(WITH_LIBCEPHSQLITE)
    target_link_libraries(ceph-mgr cephsqlite SQLite3::SQLite3)
  endif()
  target_include_directories(ceph-mgr PRIVATE
    $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
  target_link_libraries(ceph-mgr
    osdc client heap_profiler
    global-static ceph-common
    Boost::python${MGR_PYTHON_VERSION_MAJOR}${MGR_PYTHON_VERSION_MINOR}
    Python3::Python
    ${ALLOC_LIBS}
    ${CMAKE_DL_LIBS}
    ${GSSAPI_LIBRARIES})
  set_target_properties(ceph-mgr PROPERTIES
    POSITION_INDEPENDENT_CODE ${EXE_LINKER_USE_PIE})
  if(WITH_LTTNG)
    add_dependencies(ceph-mgr mgr_op_tp)
  endif()
  install(TARGETS ceph-mgr DESTINATION bin)
endif()
