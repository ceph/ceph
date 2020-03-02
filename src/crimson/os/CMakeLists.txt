add_library(crimson-os
  futurized_store.cc
  ${PROJECT_SOURCE_DIR}/src/os/Transaction.cc)
add_subdirectory(cyanstore)

if(WITH_BLUESTORE)
  add_subdirectory(alienstore)
endif()

target_link_libraries(crimson-os
  crimson-cyanstore
  crimson-alienstore
  crimson)
