add_library(cls_blake3 SHARED server.cc)
target_link_libraries(cls_blake3 blake3)
set_target_properties(cls_blake3 PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)

install(TARGETS cls_blake3 DESTINATION ${cls_dir})

add_library(cls_blake3_client STATIC client.cc)
