add_executable(crimson-osd-client
  crimson_osd_client.cc)

target_link_libraries(crimson-osd-client
  crimson
  crimson-common
  crimson::cflags
  seastar
  ${CMAKE_THREAD_LIBS_INIT})

install(TARGETS crimson-osd-client DESTINATION bin)

# Simple client for testing
add_executable(simple-client
  simple_client.cc)

target_link_libraries(simple-client
  crimson
  crimson-common
  crimson::cflags
  seastar
  ${CMAKE_THREAD_LIBS_INIT})

install(TARGETS simple-client DESTINATION bin)
