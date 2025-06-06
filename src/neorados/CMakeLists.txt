add_library(neorados_objs OBJECT
  RADOSImpl.cc)
add_library(neorados_api_obj OBJECT
  RADOS.cc)
add_library(libneorados STATIC
  $<TARGET_OBJECTS:neorados_api_obj>
  $<TARGET_OBJECTS:neorados_objs>)
target_link_libraries(libneorados PRIVATE
  osdc ceph-common cls_lock_client ${FMT_LIB}
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})
