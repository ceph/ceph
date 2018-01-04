add_library(librados_objs OBJECT
  IoCtxImpl.cc
  RadosXattrIter.cc
  RadosClient.cc)
add_library(librados_api_obj OBJECT
  librados.cc)
if(WITH_LTTNG)
  add_dependencies(librados_api_obj librados-tp)
endif()
if(ENABLE_SHARED)
  add_library(librados ${CEPH_SHARED}
    $<TARGET_OBJECTS:librados_api_obj>
    $<TARGET_OBJECTS:librados_objs>
    $<TARGET_OBJECTS:common_buffer_obj>)
  set_target_properties(librados PROPERTIES
    OUTPUT_NAME rados
    VERSION 2.0.0
    SOVERSION 2
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON)
  if(NOT APPLE)
    set_property(TARGET librados APPEND_STRING PROPERTY
      LINK_FLAGS " -Wl,--exclude-libs,ALL")
  endif()
else(ENABLE_SHARED)
  add_library(librados STATIC
    $<TARGET_OBJECTS:librados_api_obj>
    $<TARGET_OBJECTS:librados_objs>)
endif(ENABLE_SHARED)
target_link_libraries(librados PRIVATE
  osdc ceph-common cls_lock_client
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})
if(WITH_STATIC_LIBSTDCXX)
  target_link_libraries(librados
    INTERFACE "-static-libstdc++ -static-libgcc")
endif()
target_link_libraries(librados ${rados_libs})
install(TARGETS librados DESTINATION ${CMAKE_INSTALL_LIBDIR})

if(WITH_EMBEDDED)
  add_library(cephd_rados STATIC
    $<TARGET_OBJECTS:librados_api_obj>
    $<TARGET_OBJECTS:librados_objs>)
endif()
if(WITH_LTTNG AND WITH_EVENTTRACE)
  add_dependencies(librados_api_obj eventtrace_tp)
endif()
