add_library(cls_cmpxattr SHARED server.cc)
set_target_properties(cls_cmpxattr PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_cmpxattr DESTINATION ${cls_dir})

add_library(cls_cmpxattr_client STATIC client.cc)
