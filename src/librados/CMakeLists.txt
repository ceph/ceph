add_library(librados_impl STATIC
  IoCtxImpl.cc
  RadosXattrIter.cc
  RadosClient.cc
  librados_util.cc
  librados_tp.cc
  $<TARGET_OBJECTS:common_buffer_obj>)

add_library(rados_c_api OBJECT
  librados_c.cc)
add_library(rados_cxx_api OBJECT
  librados_cxx.cc)
add_library(rados_cxx STATIC
  $<TARGET_OBJECTS:rados_cxx_api>)
if(WITH_LTTNG)
  add_dependencies(librados_impl librados-tp)
  add_dependencies(rados_c_api librados-tp)
  add_dependencies(rados_cxx_api librados-tp)
endif()

# C API
add_library(librados ${CEPH_SHARED}
  $<TARGET_OBJECTS:rados_c_api>)
if(ENABLE_SHARED)
  set_target_properties(librados PROPERTIES
    OUTPUT_NAME rados
    VERSION 3.0.0
    SOVERSION 3
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON)
  if(NOT APPLE)
    set_property(TARGET librados APPEND_STRING PROPERTY
      LINK_FLAGS " -Wl,--exclude-libs,ALL")
  endif()
  if(WITH_STATIC_LIBSTDCXX)
    set_property(TARGET librados APPEND_STRING PROPERTY
      LINK_FLAGS " -static-libstdc++ -static-libgcc")
  endif()
endif()

target_link_libraries(librados PRIVATE
  rados_cxx librados_impl
  osdc ceph-common cls_lock_client
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS} ${GSSAPI_LIBRARIES})
target_link_libraries(librados ${rados_libs})
install(TARGETS librados DESTINATION ${CMAKE_INSTALL_LIBDIR})

# C++ API
add_library(librados-cxx ${CEPH_SHARED}
  $<TARGET_OBJECTS:rados_cxx_api>)
if(ENABLE_SHARED)
  set_target_properties(librados-cxx PROPERTIES
    OUTPUT_NAME radospp
    VERSION 1.0.0
    SOVERSION 1
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON)
  if(NOT APPLE)
    set_property(TARGET librados-cxx APPEND_STRING PROPERTY
      LINK_FLAGS " -Wl,--exclude-libs,ALL")
  endif()
endif(ENABLE_SHARED)
target_link_libraries(librados-cxx
  PUBLIC
    librados
  PRIVATE
    librados_impl cls_lock_client ceph-common)
install(TARGETS librados-cxx DESTINATION ${CMAKE_INSTALL_LIBDIR})

if(WITH_LTTNG AND WITH_EVENTTRACE)
  add_dependencies(librados_api_obj eventtrace_tp)
endif()
