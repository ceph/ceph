add_custom_target(lttng_gen_tp
  ALL
  COMMAND 
  lttng-gen-tp tracing/librados.tp -o tracing/librados.h && 
  lttng-gen-tp tracing/librbd.tp -o tracing/librbd.h && 
  lttng-gen-tp tracing/objectstore.tp -o tracing/objectstore.h && 
  lttng-gen-tp tracing/oprequest.tp -o tracing/oprequest.h &&
  lttng-gen-tp tracing/osd.tp -o tracing/osd.h &&
  lttng-gen-tp tracing/pg.tp -o tracing/pg.h
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src
  COMMENT "lttng-gen-tp generated .h files")

add_library(libosd_tp STATIC
  oprequest.c
  osd.c
  pg.c)
target_link_libraries(libosd_tp lttng-ust dl)
target_include_directories(libosd_tp PUBLIC ${CMAKE_SOURCE_DIR}/src/tracing)
add_dependencies(libosd_tp lttng_gen_tp)
set_target_properties(libosd_tp PROPERTIES OUTPUT_NAME osd_tp VERSION 1.0.0 SOVERSION 1)

add_library(librados_tp STATIC librados.c)
target_link_libraries(librados_tp lttng-ust dl)
target_include_directories(librados_tp PUBLIC ${CMAKE_SOURCE_DIR}/src/tracing)
add_dependencies(librados_tp lttng_gen_tp)
set_target_properties(librados_tp PROPERTIES OUTPUT_NAME rados_tp VERSION 2.0.0 SOVERSION 2)

add_library(librbd_tp STATIC librbd.c)
target_link_libraries(librbd_tp lttng-ust dl)
target_include_directories(librbd_tp PUBLIC ${CMAKE_SOURCE_DIR}/src/tracing)
add_dependencies(librbd_tp lttng_gen_tp)
set_target_properties(librbd_tp PROPERTIES OUTPUT_NAME rbd_tp VERSION 1.0.0 SOVERSION 1)

add_library(libos_tp STATIC objectstore.c)
target_link_libraries(libos_tp lttng-ust dl)
target_include_directories(libos_tp PUBLIC ${CMAKE_SOURCE_DIR}/src/tracing)
add_dependencies(libos_tp lttng_gen_tp)
set_target_properties(libos_tp PROPERTIES OUTPUT_NAME os_tp VERSION 1.0.0 SOVERSION 1)

install(TARGETS librados_tp libosd_tp librbd_tp libos_tp DESTINATION lib)
