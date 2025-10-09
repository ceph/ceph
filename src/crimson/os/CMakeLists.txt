add_library(crimson-os STATIC
  futurized_store.cc
  ${PROJECT_SOURCE_DIR}/src/os/Transaction.cc)
add_subdirectory(cyanstore)

if(WITH_BLUESTORE)
  add_subdirectory(alienstore)
  set(alienstore_lib crimson-alienstore)
endif()

add_subdirectory(seastore)
target_link_libraries(crimson-os
  crimson-cyanstore
  ${alienstore_lib}
  crimson-seastore
  crimson)
