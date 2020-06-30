add_library(crimson-cyanstore
  cyan_store.cc
  cyan_collection.cc
  cyan_object.cc)
target_link_libraries(crimson-cyanstore
  crimson)
