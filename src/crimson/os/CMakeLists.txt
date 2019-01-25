add_library(crimson-os
  cyan_store.cc)
target_link_libraries(crimson-os
  fmt::fmt crimson-common)
