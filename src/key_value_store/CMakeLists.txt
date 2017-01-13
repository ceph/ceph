set(kvs_srcs cls_kvs.cc)
add_library(cls_kvs SHARED ${kvs_srcs})
set_target_properties(cls_kvs PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH "")
install(TARGETS cls_kvs DESTINATION ${CMAKE_INSTALL_LIBDIR}/rados-classes)

if(WITH_EMBEDDED)
  add_library(cephd_cls_kvs STATIC ${kvs_srcs})
  set_target_properties(cephd_cls_kvs PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
