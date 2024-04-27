set(lib_mon_srcs
  ${CMAKE_SOURCE_DIR}/src/auth/cephx/CephxKeyServer.cc
  ${CMAKE_SOURCE_DIR}/src/auth/cephx/CephxServiceHandler.cc
  ${CMAKE_SOURCE_DIR}/src/auth/AuthServiceHandler.cc
  Paxos.cc
  PaxosService.cc
  OSDMonitor.cc
  MDSMonitor.cc
  CommandHandler.cc
  FSCommands.cc
  MgrMonitor.cc
  MgrStatMonitor.cc
  Monitor.cc
  MonmapMonitor.cc
  LogMonitor.cc
  AuthMonitor.cc
  ConfigMap.cc
  ConfigMonitor.cc
  Elector.cc
  ElectionLogic.cc
  ConnectionTracker.cc
  HealthMonitor.cc
  KVMonitor.cc
  ../mds/MDSAuthCaps.cc
  ../mgr/mgr_commands.cc
  ../osd/OSDCap.cc
  ../mgr/MgrCap.cc)

if(TARGET mgr_cap_obj)
  list(APPEND lib_mon_srcs $<TARGET_OBJECTS:mgr_cap_obj>)
endif()

if(HAVE_GSSAPI)
  list(APPEND lib_mon_srcs
    ${CMAKE_SOURCE_DIR}/src/auth/krb/KrbServiceHandler.cpp)
endif()

add_library(mon STATIC
  ${lib_mon_srcs})
target_link_libraries(mon
  legacy-option-headers
  kv
  heap_profiler
  ${FMT_LIB})
if(WITH_JAEGER)
  target_link_libraries(mon jaeger_base)
endif()
