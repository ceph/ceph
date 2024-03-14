add_library(neorados_objs OBJECT
  RADOSImpl.cc)
target_compile_definitions(neorados_objs PRIVATE
  $<TARGET_PROPERTY:${FMT_LIB},INTERFACE_COMPILE_DEFINITIONS>)
add_library(neorados_api_obj OBJECT
  RADOS.cc)
target_compile_definitions(neorados_api_obj PRIVATE
  $<TARGET_PROPERTY:${FMT_LIB},INTERFACE_COMPILE_DEFINITIONS>)

add_library(libneorados STATIC
  $<TARGET_OBJECTS:neorados_api_obj>
  $<TARGET_OBJECTS:neorados_objs>)
target_link_libraries(libneorados PRIVATE
  osdc ceph-common cls_lock_client ${FMT_LIB}
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})

# if(ENABLE_SHARED)
#   add_library(libneorados ${CEPH_SHARED}
#     $<TARGET_OBJECTS:neorados_api_obj>
#     $<TARGET_OBJECTS:neorados_objs>
#     $<TARGET_OBJECTS:common_buffer_obj>)
#   set_target_properties(libneorados PROPERTIES
#     OUTPUT_NAME RADOS
#     VERSION 0.0.1
#     SOVERSION 1
#     CXX_VISIBILITY_PRESET hidden
#     VISIBILITY_INLINES_HIDDEN ON)
#   if(NOT APPLE)
#     set_property(TARGET libneorados APPEND_STRING PROPERTY
#       LINK_FLAGS " -Wl,--exclude-libs,ALL")
#   endif()
# else(ENABLE_SHARED)
#   add_library(libneorados STATIC
#     $<TARGET_OBJECTS:neorados_api_obj>
#     $<TARGET_OBJECTS:neorados_objs>)
# endif(ENABLE_SHARED)
# target_link_libraries(libneorados PRIVATE
#   osdc ceph-common cls_lock_client
#   ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})
# target_link_libraries(libneorados ${rados_libs})
# install(TARGETS libneorados DESTINATION ${CMAKE_INSTALL_LIBDIR})
