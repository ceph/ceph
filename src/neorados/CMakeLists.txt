add_library(neorados_objs OBJECT
  RADOSImpl.cc)
target_compile_definitions(neorados_objs PRIVATE
  $<TARGET_PROPERTY:fmt::fmt,INTERFACE_COMPILE_DEFINITIONS>)
add_library(neorados_api_obj OBJECT
  RADOS.cc)
target_compile_definitions(neorados_api_obj PRIVATE
  $<TARGET_PROPERTY:fmt::fmt,INTERFACE_COMPILE_DEFINITIONS>)

add_library(libneorados STATIC
  $<TARGET_OBJECTS:neorados_api_obj>
  $<TARGET_OBJECTS:neorados_objs>)
target_link_libraries(libneorados PRIVATE
  osdc ceph-common cls_lock_client fmt::fmt
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})

add_library(neorados_cls_version STATIC cls/version.cc)
add_library(neorados_cls_log STATIC cls/log.cc)
