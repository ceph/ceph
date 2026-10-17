add_library(neorados_objs OBJECT
  RADOSImpl.cc)
target_link_libraries(neorados_objs
  PUBLIC legacy-option-headers
  PRIVATE ceph-json-headers)
add_library(neorados_api_obj OBJECT
  RADOS.cc)
target_link_libraries(neorados_api_obj
  PUBLIC legacy-option-headers
  PRIVATE ceph-json-headers)

add_library(libneorados STATIC
  $<TARGET_OBJECTS:neorados_api_obj>
  $<TARGET_OBJECTS:neorados_objs>)
target_link_libraries(libneorados
  PUBLIC ceph-json-headers
  PRIVATE
    osdc ceph-common cls_lock_client
    ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})
