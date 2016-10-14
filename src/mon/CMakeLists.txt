set(osd_mon_files
  Monitor.cc)
add_library(osd_mon_objs OBJECT ${osd_mon_files})

set(lib_mon_srcs
  ${CMAKE_SOURCE_DIR}/src/auth/cephx/CephxKeyServer.cc
  ${CMAKE_SOURCE_DIR}/src/auth/cephx/CephxServiceHandler.cc
  ${CMAKE_SOURCE_DIR}/src/auth/AuthServiceHandler.cc
  ${osd_mon_files}
  Paxos.cc
  PaxosService.cc
  OSDMonitor.cc
  MDSMonitor.cc
  MgrMonitor.cc
  MonmapMonitor.cc
  LogMonitor.cc
  AuthMonitor.cc
  Elector.cc
  HealthMonitor.cc
  DataHealthService.cc
  PGMonitor.cc
  PGMap.cc
  ConfigKeyService.cc)
add_library(mon STATIC ${lib_mon_srcs} $<TARGET_OBJECTS:mon_common_objs>
  $<TARGET_OBJECTS:kv_objs> $<TARGET_OBJECTS:osd_mon_objs>
  $<TARGET_OBJECTS:common_util_obj> $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(mon ${ALLOC_LIBS})
