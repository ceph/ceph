set(lib_mon_srcs
  ${CMAKE_SOURCE_DIR}/src/auth/cephx/CephxKeyServer.cc
  ${CMAKE_SOURCE_DIR}/src/auth/cephx/CephxServiceHandler.cc
  ${CMAKE_SOURCE_DIR}/src/auth/AuthServiceHandler.cc
  ${osd_mon_files}
  Paxos.cc
  PaxosService.cc
  OSDMonitor.cc
  MDSMonitor.cc
  FSCommands.cc
  MgrMonitor.cc
  MgrStatMonitor.cc
  Monitor.cc
  MonmapMonitor.cc
  LogMonitor.cc
  AuthMonitor.cc
  Elector.cc
  HealthMonitor.cc
  OldHealthMonitor.cc
  DataHealthService.cc
  PGMonitor.cc
  PGMap.cc
  ConfigKeyService.cc)
add_library(mon STATIC
  ${lib_mon_srcs}
  $<TARGET_OBJECTS:kv_objs>
  $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(mon ${ALLOC_LIBS})
