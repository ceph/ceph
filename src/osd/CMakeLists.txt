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
  ${CMAKE_SOURCE_DIR}/src/common/TrackedOp.cc
  ${osd_cyg_functions_src}
  ${osdc_osd_srcs})
if(HAS_VTA)
  set_source_files_properties(osdcap.cc
    PROPERTIES COMPILE_FLAGS -fno-var-tracking-assignments)
endif()
add_library(osd STATIC ${osd_srcs}
  $<TARGET_OBJECTS:cls_references_objs>
  $<TARGET_OBJECTS:global_common_objs>
  $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(osd ${LEVELDB_LIBRARIES} dmclock ${CMAKE_DL_LIBS} ${ALLOC_LIBS})
if(WITH_LTTNG)
  add_dependencies(osd osd-tp pg-tp)
endif()
if(WITH_LTTNG AND WITH_EVENTTRACE)
  add_dependencies(osd eventtrace_tp)
endif()
if(WITH_OSD_INSTRUMENT_FUNCTIONS)
  add_dependencies(osd cyg_profile_tp)
endif()
