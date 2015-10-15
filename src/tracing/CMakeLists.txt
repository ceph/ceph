add_custom_command(OUTPUT 
  ${CMAKE_SOURCE_DIR}/src/tracing/librados.c
  ${CMAKE_SOURCE_DIR}/src/tracing/osd.c
  ${CMAKE_SOURCE_DIR}/src/tracing/pg.c
  ${CMAKE_SOURCE_DIR}/src/tracing/oprequest.c
  ${CMAKE_SOURCE_DIR}/src/tracing/librbd.c
  ${CMAKE_SOURCE_DIR}/src/tracing/objectstore.c
  COMMAND 
  lttng-gen-tp tracing/librados.tp -o tracing/librados.h -o tracing/librados.c && 
  lttng-gen-tp tracing/librbd.tp -o tracing/librbd.h -o tracing/librbd.c && 
  lttng-gen-tp tracing/objectstore.tp -o tracing/objectstore.h -o tracing/objectstore.c && 
  lttng-gen-tp tracing/objectstore.tp -o tracing/objectstore.h -o tracing/objectstore.c && 
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
target_compile_definitions(libosd_tp PUBLIC "-DTRACEPOINT_PROBE_DYNAMIC_LINKAGE")

add_library(librados_tp librados.c)
target_link_libraries(librados_tp lttng-ust dl)
target_compile_definitions(librados_tp PUBLIC "-DTRACEPOINT_PROBE_DYNAMIC_LINKAGE")

add_library(librbd_tp librbd.c)
target_link_libraries(librbd_tp lttng-ust dl)
target_compile_definitions(librbd_tp PUBLIC "-DTRACEPOINT_PROBE_DYNAMIC_LINKAGE")

add_library(libos_tp objectstore.c)
target_link_libraries(libos_tp lttng-ust dl)
target_compile_definitions(libos_tp PUBLIC "-DTRACEPOINT_PROBE_DYNAMIC_LINKAGE")
