set(kvs_srcs cls_kvs.cc)
add_library(cls_kvs SHARED ${kvs_srcs})
set_target_properties(cls_kvs PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_kvs DESTINATION ${CMAKE_INSTALL_LIBDIR}/rados-classes)
