# we are in "src/tracing", so create the output dir manually.
# the source files include the tracing headers like
# #include "tracing/oprequest.h". so better put them into
# ${PROJECT_BINARY_DIR}/include, where acconfig.h is also located
set(working_dir ${CMAKE_BINARY_DIR}/include)
set(header_dir ${working_dir}/tracing)
file(MAKE_DIRECTORY ${header_dir})

add_custom_target(tracepoint_libraries)

file(GLOB tps "*.tp")
foreach(tp ${tps})
  get_filename_component(name ${tp} NAME_WE)
  set(header ${header_dir}/${name}.h)
  add_custom_command(
    OUTPUT ${header}
    COMMAND ${LTTNG_GEN_TP} ${tp} -o tracing/${name}.h
    DEPENDS ${tp}
    WORKING_DIRECTORY ${working_dir}
    COMMENT "generating ${header}")
  add_custom_target(
    ${name}-tp
    DEPENDS ${header})
endforeach()

function(add_tracing_library name tracings version)
  foreach(tp_file ${tracings})
    get_filename_component(tp ${tp_file} NAME_WE)
    list(APPEND hdrs
      ${header_dir}/${tp}.h)
    list(APPEND tpfiles ${tp}.c)
  endforeach()
  add_library(${name} SHARED ${hdrs} ${tpfiles})
  target_link_libraries(${name} ${LTTNGUST_LIBRARIES} ${CMAKE_DL_LIBS})
  string(REGEX MATCH "^[0-9]+" soversion ${version})
  set_target_properties(${name} PROPERTIES
    OUTPUT_NAME ${name}
    VERSION ${version}
    SOVERSION ${soversion}
    INSTALL_RPATH "")
  add_dependencies(tracepoint_libraries ${name})
endfunction()

set(osd_traces oprequest.tp osd.tp pg.tp)
add_tracing_library(osd_tp "${osd_traces}" 1.0.0)
add_tracing_library(rados_tp librados.tp 2.0.0)
add_tracing_library(os_tp objectstore.tp 1.0.0)
add_tracing_library(bluestore_tp bluestore.tp 1.0.0)
add_tracing_library(rgw_op_tp rgw_op.tp 2.0.0)
add_tracing_library(rgw_rados_tp rgw_rados.tp 2.0.0)
add_tracing_library(mgr_op_tp mgroprequest.tp 1.0.0)

install(TARGETS rados_tp osd_tp os_tp rgw_rados_tp rgw_op_tp mgr_op_tp DESTINATION ${CMAKE_INSTALL_LIBDIR})
if(WITH_RBD)
  add_tracing_library(rbd_tp librbd.tp 1.0.0)
  install(TARGETS rbd_tp DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif(WITH_RBD)
if(WITH_OSD_INSTRUMENT_FUNCTIONS)
  add_tracing_library(cyg_profile_tp cyg_profile.tp 1.0.0)
  install(TARGETS cyg_profile_tp DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

if(WITH_EVENTTRACE)
  add_tracing_library(eventtrace_tp eventtrace.tp 1.0.0)
  install(TARGETS eventtrace_tp DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

