add_library(neorados_objs OBJECT
  RADOSImpl.cc)
add_library(neorados_api_obj OBJECT
  RADOS.cc)

add_library(libneorados STATIC
  $<TARGET_OBJECTS:neorados_api_obj>
  $<TARGET_OBJECTS:neorados_objs>)
target_link_libraries(libneorados PRIVATE
  osdc ceph-common cls_lock_client fmt::fmt
  ${BLKID_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})

add_library(neorados_cls_fifo STATIC cls/fifo.cc)
add_library(neorados_cls_version STATIC cls/version.cc)
