add_custom_target(lttng_gen_tp
  ALL
  COMMAND 
  lttng-gen-tp tracing/librados.tp -o tracing/librados.h -o tracing/librados.c && 
  lttng-gen-tp tracing/librbd.tp -o tracing/librbd.h -o tracing/librbd.c && 
  lttng-gen-tp tracing/objectstore.tp -o tracing/objectstore.h -o tracing/objectstore.c && 
  lttng-gen-tp tracing/oprequest.tp -o tracing/oprequest.h -o tracing/oprequest.c &&
  lttng-gen-tp tracing/osd.tp -o tracing/osd.h -o tracing/osd.c &&
  lttng-gen-tp tracing/pg.tp -o tracing/pg.h -o tracing/pg.c
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src
  COMMENT "lttng-gen-tp generated .c and .h files")

add_library(libosd_tp 
  oprequest.c
  osd.c
  pg.c)
target_link_libraries(libosd_tp lttng-ust dl)
target_include_directories(libosd_tp PUBLIC ${CMAKE_SOURCE_DIR}/src/tracing)
add_dependencies(libosd_tp lttng_gen_tp)
set_target_properties(libosd_tp PROPERTIES OUTPUT_NAME osd_tp VERSION 1.0.0 SOVERSION 1)

add_library(librados_tp librados.c)
target_link_libraries(librados_tp lttng-ust dl)
target_include_directories(librados_tp PUBLIC ${CMAKE_SOURCE_DIR}/src/tracing)
add_dependencies(librados_tp lttng_gen_tp)
set_target_properties(librados_tp PROPERTIES OUTPUT_NAME rados_tp VERSION 2.0.0 SOVERSION 2)

add_library(librbd_tp librbd.c)
target_link_libraries(librbd_tp lttng-ust dl)
target_include_directories(librbd_tp PUBLIC ${CMAKE_SOURCE_DIR}/src/tracing)
add_dependencies(librbd_tp lttng_gen_tp)
set_target_properties(librbd_tp PROPERTIES OUTPUT_NAME rbd_tp VERSION 1.0.0 SOVERSION 1)

add_library(libos_tp objectstore.c)
target_link_libraries(libos_tp lttng-ust dl)
target_include_directories(libos_tp PUBLIC ${CMAKE_SOURCE_DIR}/src/tracing)
add_dependencies(libos_tp lttng_gen_tp)
set_target_properties(libos_tp PROPERTIES OUTPUT_NAME os_tp VERSION 1.0.0 SOVERSION 1)
