set(libglobal_srcs
  global_init.cc
  pidfile.cc
  signal_handler.cc)
set(global_common_files
  global_context.cc)
add_library(global_common_objs OBJECT ${global_common_files})
add_library(global STATIC ${libglobal_srcs}
  $<TARGET_OBJECTS:global_common_objs>)
target_link_libraries(global common ${DPDK_LIBRARIES} ${EXTRALIBS})
