cmake_minimum_required(VERSION 2.8)

# include_directories($ENV{NATIVE_CLIENT_ROOT} ${COCL2_DIR})

set(cxx_srcs dm_clock_srv.cc test_server.cc test_client.cc test.cc)

set_source_files_properties(${cxx_srcs}
  PROPERTIES
  COMPILE_FLAGS "${my_compile_flags} -std=gnu++11 -pthread")

add_executable(sim ${cxx_srcs})

target_link_libraries(sim PUBLIC pthread)

# file(COPY sim DESTINATION .)

add_custom_target(runsim
  DEPENDS sim
  COMMAND $<TARGET_FILE:sim>
  COMMAND printf '\\a\\a\\a')
