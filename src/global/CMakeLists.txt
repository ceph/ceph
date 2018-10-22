set(libglobal_srcs
  global_init.cc
  pidfile.cc
  signal_handler.cc)
set(global_common_files
  global_context.cc)
add_library(global_common_objs OBJECT ${global_common_files})
add_library(libglobal_objs OBJECT ${libglobal_srcs})

add_library(global-static STATIC
  $<TARGET_OBJECTS:libglobal_objs>
  $<TARGET_OBJECTS:global_common_objs>)
target_link_libraries(global-static common)

add_library(global STATIC
  $<TARGET_OBJECTS:libglobal_objs>
  $<TARGET_OBJECTS:global_common_objs>)
target_link_libraries(global ceph-common ${EXTRALIBS})
