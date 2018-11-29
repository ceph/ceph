# we are in "src/tracing", so create the output dir manually.
# the source files include the tracing headers like
# #include "tracing/oprequest.h". so better put them into
# ${PROJECT_BINARY_DIR}/include, where acconfig.h is also located
set(working_dir ${CMAKE_BINARY_DIR}/include)
set(header_dir ${working_dir}/tracing)
set(tracing_build_dir ${CMAKE_BINARY_DIR}/tracing)
file(MAKE_DIRECTORY ${header_dir})
file(MAKE_DIRECTORY ${tracing_build_dir})

add_custom_target(tracepoint_libraries)

# TODO this should be done on all source code automatically instead of
# indivudal files
execute_process(
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BlueStore.cc -t tp --outdir ${CMAKE_BINARY_DIR})
execute_process(
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BitmapAllocator.cc -t tp --outdir ${CMAKE_BINARY_DIR})
execute_process(
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BitmapFreelistManager.cc -t tp --outdir ${CMAKE_BINARY_DIR})
execute_process(
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/msg/async/AsyncConnection.cc -t tp --outdir ${CMAKE_BINARY_DIR})
execute_process(
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/msg/DispatchQueue.cc -t tp --outdir ${CMAKE_BINARY_DIR})
if(WITH_LTTNG_LOGGING)
  execute_process(
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BlueStore.cc -t h --outdir ${CMAKE_BINARY_DIR})
  execute_process(
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BitmapAllocator.cc -t h --outdir ${CMAKE_BINARY_DIR})
  execute_process(
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BitmapFreelistManager.cc -t h --outdir ${CMAKE_BINARY_DIR})
  execute_process(
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/msg/async/AsyncConnection.cc -t h --outdir ${CMAKE_BINARY_DIR})
  execute_process(
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/msg/DispatchQueue.cc -t h --outdir ${CMAKE_BINARY_DIR})
else()
  execute_process(
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BlueStore.cc -t h --outdir ${CMAKE_BINARY_DIR} --disabled)
  execute_process(
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BitmapAllocator.cc -t h --outdir ${CMAKE_BINARY_DIR} --disabled)
  execute_process(
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BitmapFreelistManager.cc -t h --outdir ${CMAKE_BINARY_DIR} --disabled)
  execute_process(
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/msg/async/AsyncConnection.cc -t h --outdir ${CMAKE_BINARY_DIR} --disabled)
  execute_process(
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/msg/DispatchQueue.cc -t h --outdir ${CMAKE_BINARY_DIR} --disabled)
endif()
execute_process(
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BlueStore.cc -t c --outdir ${CMAKE_BINARY_DIR})
execute_process(
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BitmapAllocator.cc -t c --outdir ${CMAKE_BINARY_DIR})
execute_process(
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BitmapFreelistManager.cc -t c --outdir ${CMAKE_BINARY_DIR})
execute_process(
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/msg/async/AsyncConnection.cc -t c --outdir ${CMAKE_BINARY_DIR})
execute_process(
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/msg/DispatchQueue.cc -t c --outdir ${CMAKE_BINARY_DIR})

file(GLOB all_files "${tracing_build_dir}/*")
file(GLOB generated_tps "${tracing_build_dir}/*.tp")
file(GLOB tps "*.tp")

#set(generated_tps ${working_dir}/tracing/bluestore_gc.tp;${working_dir}/tracing/bluestore_lru_cache.tp;${working_dir}/tracing/bluestore_twoqcache.tp;${working_dir}/tracing/bluestore.tp;${working_dir}/tracing/bluestore_blob.tp;${working_dir}/tracing/bitmapallocator.tp;${working_dir}/tracing/bmap_freelist_manager.tp;${working_dir}/tracing/asyncconnection.tp)

foreach(tp ${tps} ${generated_tps})
  get_filename_component(name ${tp} NAME_WE)
  set(header ${header_dir}/${name}.h)
  execute_process(COMMAND ${LTTNG_GEN_TP} ${tp} -o tracing/${name}.h)
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

#add_custom_command(
#  OUTPUT ${CMAKE_BINARY_DIR}/include/tracing/bluestore_impl.h
#  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tracetool/parsetool.py -f ${CMAKE_SOURCE_DIR}/src/os/bluestore/BlueStore.cc -t h --outdir ${CMAKE_BINARY_DIR}
#  DEPENDS ${CMAKE_SOURCE_DIR}/src/os/bluestore/BlueStore.cc
#  COMMENT "Running parsetool.py")
#add_custom_target(
#  bluestore-parsetool
#  DEPENDS ${CMAKE_BINARY_DIR}/include/tracing/bluestore_impl.h)

function(add_tracing_library name tracings version)
  foreach(tp_file ${tracings})
    get_filename_component(tp ${tp_file} NAME_WE)
    list(APPEND hdrs
      ${header_dir}/${tp}.h)
    # TODO: remove this condition when we move to tracetool for existing tracepoints
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${tp}.c")
      list(APPEND tpfiles ${tp}.c)
    else()
      list(APPEND tpfiles ${tracing_build_dir}/${tp}.c)
    endif()
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

set(osd_traces oprequest.tp osd.tp)
add_tracing_library(osd_tp "${osd_traces}" 1.0.0)
add_tracing_library(rados_tp librados.tp 3.0.0)
add_tracing_library(os_tp objectstore.tp 1.0.0)
add_tracing_library(rgw_op_tp rgw_op.tp 1.0.0)
add_tracing_library(rgw_rados_tp rgw_rados.tp 1.0.0)

if(WITH_LTTNG_LOGGING)
  add_tracing_library(ceph_logging_tp ceph_logging.tp 1.0.0)
#  Uncommenting this line makes it so that build/include/tracing/ceph_logging.h
#  isn't generated anymore..
#  target_link_libraries(ceph_logging_tp fmt::fmt)

  foreach(tp ${generated_tps})
    get_filename_component(name ${tp} NAME)
    set(logging_tps "${name};${logging_tps}")
  endforeach()

  add_tracing_library(logging_tp "${logging_tps}" 1.0.0)
  install(TARGETS rados_tp osd_tp logging_tp ceph_logging_tp os_tp rgw_rados_tp rgw_op_tp DESTINATION ${CMAKE_INSTALL_LIBDIR})
else()
  install(TARGETS rados_tp osd_tp os_tp rgw_rados_tp rgw_op_tp DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

if(WITH_RBD)
  add_tracing_library(rbd_tp librbd.tp 1.0.0)
  install(TARGETS rbd_tp DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif(WITH_RBD)
if(WITH_OSD_INSTRUMENT_FUNCTIONS)
  add_tracing_library(cyg_profile_tp cyg_profile.tp 1.0.0)
  install(TARGETS cyg_profile_tp DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
if(WITH_LTTNG AND WITH_EVENTTRACE)
  add_tracing_library(eventtrace_tp eventtrace.tp 1.0.0)
  install(TARGETS eventtrace_tp DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

