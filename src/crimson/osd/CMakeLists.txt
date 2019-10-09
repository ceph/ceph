add_executable(crimson-osd
  chained_dispatchers.cc
  ec_backend.cc
  heartbeat.cc
  main.cc
  osd.cc
  osd_meta.cc
  pg.cc
  pg_backend.cc
  pg_meta.cc
  replicated_backend.cc
  shard_services.cc
  object_context.cc
  ops_executer.cc
  osd_operation.cc
  osd_operations/client_request.cc
  osd_operations/compound_peering_request.cc
  osd_operations/peering_event.cc
  osd_operations/pg_advance_map.cc
  osd_operations/replicated_request.cc
  osdmap_gate.cc
  pg_map.cc
  objclass.cc
  ${PROJECT_SOURCE_DIR}/src/objclass/class_api.cc
  ${PROJECT_SOURCE_DIR}/src/osd/ClassHandler.cc
  ${PROJECT_SOURCE_DIR}/src/osd/osd_op_util.cc
  ${PROJECT_SOURCE_DIR}/src/osd/PeeringState.cc
  ${PROJECT_SOURCE_DIR}/src/osd/PGPeeringEvent.cc
  ${PROJECT_SOURCE_DIR}/src/osd/PGStateUtils.cc
  ${PROJECT_SOURCE_DIR}/src/osd/MissingLoc.cc
  ${PROJECT_SOURCE_DIR}/src/osd/PGLog.cc
  ${PROJECT_SOURCE_DIR}/src/osd/osd_perf_counters.cc
  )
target_link_libraries(crimson-osd
  crimson-common crimson-os crimson fmt::fmt)
set_target_properties(crimson-osd PROPERTIES
  POSITION_INDEPENDENT_CODE ${EXE_LINKER_USE_PIE})
install(TARGETS crimson-osd DESTINATION bin)
if(WITH_TESTS)
  add_dependencies(tests crimson-osd)
endif()
