add_library(os STATIC
  ObjectStore.cc
  Transaction.cc)

target_link_libraries(os
  PRIVATE
    legacy-option-headers
    ${FMT_LIB})

add_subdirectory(memstore)
target_link_libraries(os
  PRIVATE memstore)

add_subdirectory(kstore)
target_link_libraries(os
  PRIVATE
    kstore)

if(WITH_BLUESTORE)
  add_subdirectory(bluestore)
  target_link_libraries(os
    PRIVATE bluestore)
endif()

if(WITH_FUSE)
  add_library(fusestore
    FuseStore.cc)
  target_link_libraries(fusestore
    PRIVATE FUSE::FUSE)
  target_link_libraries(os
    PRIVATE fusestore)
endif()

if(WITH_LTTNG)
  add_dependencies(os objectstore-tp)
endif()

if(WITH_JAEGER)
  add_dependencies(os jaeger_base)
  target_link_libraries(os
    PRIVATE jaeger_base)
endif()

if(WITH_BLUESTORE)
  add_executable(ceph-bluestore-tool
    bluestore/bluestore_tool.cc)
  target_link_libraries(ceph-bluestore-tool
    global kv os)
  install(TARGETS ceph-bluestore-tool
    DESTINATION bin)
endif()
