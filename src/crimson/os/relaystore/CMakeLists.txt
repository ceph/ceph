add_library(crimson-relaystore STATIC
  relay_store.cc)
target_link_libraries(crimson-relaystore
  crimson
  crimson-os)
