set(libglobal_srcs
  global_init.cc
  pidfile.cc)
if (WIN32)
  list(APPEND libglobal_srcs signal_handler_win32.cc)
else()
  list(APPEND libglobal_srcs signal_handler.cc)
endif()

add_library(libglobal_objs OBJECT ${libglobal_srcs})
target_link_libraries(libglobal_objs legacy-option-headers)

add_library(global-static STATIC
  $<TARGET_OBJECTS:libglobal_objs>)
target_link_libraries(global-static common)

add_library(global STATIC
  $<TARGET_OBJECTS:libglobal_objs>)
target_link_libraries(global ceph-common ${EXTRALIBS})
