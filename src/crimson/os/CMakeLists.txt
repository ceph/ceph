add_library(crimson-os
  cyan_store.cc
  cyan_collection.cc
  cyan_object.cc
  ${PROJECT_SOURCE_DIR}/src/os/Transaction.cc)
target_link_libraries(crimson-os
  crimson)
