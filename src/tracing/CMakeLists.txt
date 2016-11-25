# we are in "src/tracing", so create the output dir manually.
# the source files include the tracing headers like
# #include "tracing/oprequest.h". so better put them into
# ${PROJECT_BINARY_DIR}/include, where acconfig.h is also located
set(working_dir ${CMAKE_BINARY_DIR}/include)
set(header_dir ${working_dir}/tracing)
file(MAKE_DIRECTORY ${header_dir})

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
  endforeach()
  add_library(${name} SHARED ${hdrs} ${tp}.c)
  target_link_libraries(${name} ${LTTNGUST_LIBRARIES} ${CMAKE_DL_LIBS})
  string(REGEX MATCH "^[0-9]+" soversion ${version})
  set_target_properties(${name} PROPERTIES
    OUTPUT_NAME ${name}
    VERSION ${version}
    SOVERSION ${soversion})
endfunction()

set(osd_traces oprequest.tp osd.tp pg.tp)
add_tracing_library(osd_tp "${osd_traces}" 1.0.0)
add_tracing_library(rados_tp librados.tp 2.0.0)
add_tracing_library(rbd_tp librbd.tp 1.0.0)
add_tracing_library(os_tp objectstore.tp 1.0.0)

install(TARGETS rados_tp osd_tp rbd_tp os_tp DESTINATION ${CMAKE_INSTALL_LIBDIR})

