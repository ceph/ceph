add_library(cls_cmpomap SHARED server.cc)
set_target_properties(cls_cmpomap PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_cmpomap DESTINATION ${cls_dir})

add_library(cls_cmpomap_client STATIC client.cc)
