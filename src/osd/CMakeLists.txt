set(osdc_osd_srcs
  ${CMAKE_SOURCE_DIR}/src/osdc/Objecter.cc
  ${CMAKE_SOURCE_DIR}/src/osdc/Striper.cc)

if(WITH_OSD_INSTRUMENT_FUNCTIONS AND CMAKE_CXX_COMPILER_ID STREQUAL GNU)
  set(GCC_C_FLAGS "-finstrument-functions")
  set(GCC_C_FLAGS "${GCC_C_FLAGS} -finstrument-functions-exclude-function-list=_mm_loadu_si128,_mm_cmpeq_epi32,_mm_movemask_epi8")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GCC_C_FLAGS}")
  set(osd_cyg_functions_src ${CMAKE_SOURCE_DIR}/src/tracing/cyg_profile_functions.c)
endif()

set(osd_srcs
  OSD.cc
  Watch.cc
  ClassHandler.cc
  PG.cc
  PGLog.cc
  PrimaryLogPG.cc
  ReplicatedBackend.cc
  ECBackend.cc
  ECTransaction.cc
  PGBackend.cc
  OSDCap.cc
  Watch.cc
  ClassHandler.cc
  Session.cc
  SnapMapper.cc
  ScrubStore.cc
  osd_types.cc
  ECUtil.cc
  ExtentCache.cc
  mClockOpClassSupport.cc
  mClockOpClassQueue.cc
  mClockClientQueue.cc
  OpQueueItem.cc
  PeeringState.cc
  PGStateUtils.cc
  MissingLoc.cc
  osd_perf_counters.cc
  ${CMAKE_SOURCE_DIR}/src/common/TrackedOp.cc
  ${CMAKE_SOURCE_DIR}/src/mgr/OSDPerfMetricTypes.cc
  ${osd_cyg_functions_src}
  ${osdc_osd_srcs})
if(HAS_VTA)
  set_source_files_properties(osdcap.cc
    PROPERTIES COMPILE_FLAGS -fno-var-tracking-assignments)
endif()
add_library(osd STATIC ${osd_srcs}
  $<TARGET_OBJECTS:global_common_objs>)
target_link_libraries(osd
  PUBLIC dmclock::dmclock
  PRIVATE os heap_profiler cpu_profiler ${CMAKE_DL_LIBS})
if(WITH_LTTNG)
  add_dependencies(osd osd-tp pg-tp)
endif()
if(WITH_EVENTTRACE)
  add_dependencies(osd eventtrace_tp)
endif()
if(WITH_OSD_INSTRUMENT_FUNCTIONS)
  add_dependencies(osd cyg_profile_tp)
endif()

# libcls_* are runtime dependencies
add_dependencies(osd cls_journal cls_hello cls_lock cls_log cls_numops
  cls_refcount cls_timeindex cls_user cls_version cls_cas)
if(WITH_CEPHFS)
  add_dependencies(osd cls_cephfs)
endif()
if(WITH_RBD)
  add_dependencies(osd cls_rbd)
endif()
if(WITH_RADOSGW)
  add_dependencies(osd cls_otp cls_rgw cls_queue cls_rgw_gc)
endif()
