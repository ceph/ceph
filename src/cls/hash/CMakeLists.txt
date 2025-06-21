add_library(cls_hash SHARED server.cc)
target_link_libraries(cls_hash blake3)
set_target_properties(cls_hash PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)

install(TARGETS cls_hash DESTINATION ${cls_dir})

add_library(cls_hash_client STATIC client.cc)
