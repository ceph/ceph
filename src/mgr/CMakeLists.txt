add_library(mgr_cap_obj OBJECT
  MgrCap.cc)

if(WITH_MGR)
  set(mgr_srcs
    ${CMAKE_SOURCE_DIR}/src/ceph_mgr.cc
    ${CMAKE_SOURCE_DIR}/src/mon/PGMap.cc
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
    $<TARGET_OBJECTS:mgr_cap_obj>)
  add_executable(ceph-mgr ${mgr_srcs})
  target_link_libraries(ceph-mgr
    osdc client heap_profiler
    global-static ceph-common
    Boost::python${MGR_PYTHON_VERSION_MAJOR}${MGR_PYTHON_VERSION_MINOR}
    Python3::Python ${CMAKE_DL_LIBS} ${GSSAPI_LIBRARIES})
  set_target_properties(ceph-mgr PROPERTIES
    POSITION_INDEPENDENT_CODE ${EXE_LINKER_USE_PIE})
  install(TARGETS ceph-mgr DESTINATION bin)
endif()
