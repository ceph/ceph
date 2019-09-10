add_library(RADOS_objs OBJECT
  RADOSImpl.cc)
add_library(RADOS_api_obj OBJECT
  RADOS.cc)

add_library(libRADOS STATIC
  $<TARGET_OBJECTS:RADOS_api_obj>
  $<TARGET_OBJECTS:RADOS_objs>)
target_link_libraries(libRADOS PRIVATE
  osdc ceph-common cls_lock_client fmt::fmt
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})

# if(ENABLE_SHARED)
#   add_library(libRADOS ${CEPH_SHARED}
#     $<TARGET_OBJECTS:RADOS_api_obj>
#     $<TARGET_OBJECTS:RADOS_objs>
#     $<TARGET_OBJECTS:common_buffer_obj>)
#   set_target_properties(libRADOS PROPERTIES
#     OUTPUT_NAME RADOS
#     VERSION 0.0.1
#     SOVERSION 1
#     CXX_VISIBILITY_PRESET hidden
#     VISIBILITY_INLINES_HIDDEN ON)
#   if(NOT APPLE)
#     set_property(TARGET libRADOS APPEND_STRING PROPERTY
#       LINK_FLAGS " -Wl,--exclude-libs,ALL")
#   endif()
# else(ENABLE_SHARED)
#   add_library(libRADOS STATIC
#     $<TARGET_OBJECTS:RADOS_api_obj>
#     $<TARGET_OBJECTS:RADOS_objs>)
# endif(ENABLE_SHARED)
# target_link_libraries(libRADOS PRIVATE
#   osdc ceph-common cls_lock_client
#   ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})
# target_link_libraries(libRADOS ${rados_libs})
# install(TARGETS libRADOS DESTINATION ${CMAKE_INSTALL_LIBDIR})

add_library(RADOS_cls_version OBJECT cls/version.cc)
add_library(RADOS_cls_rgw OBJECT cls/rgw.cc)
