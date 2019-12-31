add_library(crimson-os
  futurized_store.cc
  ${PROJECT_SOURCE_DIR}/src/os/Transaction.cc)
add_subdirectory(cyanstore)
target_link_libraries(crimson-os
  crimson-cyanstore
  crimson)
