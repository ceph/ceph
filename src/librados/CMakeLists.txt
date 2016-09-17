add_library(librados_objs OBJECT
  IoCtxImpl.cc
  RadosXattrIter.cc
  RadosClient.cc)
add_library(librados_api_obj OBJECT
  librados.cc)
add_library(librados ${CEPH_SHARED}
  $<TARGET_OBJECTS:librados_api_obj>
  $<TARGET_OBJECTS:librados_objs>
  $<TARGET_OBJECTS:common_buffer_obj>)
add_dependencies(librados osdc)
if(WITH_LTTNG)
  add_dependencies(librados_api_obj librados-tp)
endif()
# LINK_PRIVATE instead of PRIVATE is used to backward compatibility with cmake 2.8.11
target_link_libraries(librados LINK_PRIVATE osdc common cls_lock_client
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})
if(ENABLE_SHARED)
  set_target_properties(librados PROPERTIES
    OUTPUT_NAME rados
    VERSION 2.0.0
    SOVERSION 2
    # use COMPILE_FLAGS for the backward compatibility with cmake 2.8.11, should have been:
    #  CXX_VISIBILITY_PRESET hidden
    #  VISIBILITY_INLINES_HIDDEN ON
    COMPILE_FLAGS "-fvisibility=hidden -fvisibility-inlines-hidden"
    LINK_FLAGS "-Wl,--exclude-libs,ALL")
endif(ENABLE_SHARED)
install(TARGETS librados DESTINATION ${CMAKE_INSTALL_LIBDIR})
