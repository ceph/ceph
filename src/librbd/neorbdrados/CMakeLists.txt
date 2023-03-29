add_library(neorbdrados_objs OBJECT
  RADOSImpl.cc)
target_compile_definitions(neorbdrados_objs PRIVATE
  $<TARGET_PROPERTY:fmt::fmt,INTERFACE_COMPILE_DEFINITIONS>)
add_library(neorbdrados_api_obj OBJECT
  RADOS.cc)
target_compile_definitions(neorbdrados_api_obj PRIVATE
  $<TARGET_PROPERTY:fmt::fmt,INTERFACE_COMPILE_DEFINITIONS>)

add_library(libneorbdrados STATIC
  $<TARGET_OBJECTS:neorbdrados_api_obj>
  $<TARGET_OBJECTS:neorbdrados_objs>)
target_link_libraries(libneorbdrados PRIVATE
  osdc ceph-common cls_lock_client fmt::fmt
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})
