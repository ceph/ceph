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
