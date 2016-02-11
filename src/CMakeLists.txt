cmake_minimum_required(VERSION 2.8)

set(local_flags)

set(core_srcs dmclock_server.cc dmclock_util.cc crimson/run_every.cc)
set(test_srcs test_server.cc test_client.cc test.cc)

set_source_files_properties(${core_srcs} ${test_srcs}
  PROPERTIES
  COMPILE_FLAGS "${my_compile_flags} -std=gnu++11 -pthread ${local_flags}"
  )

add_executable(sim ${core_srcs} ${test_srcs})

target_link_libraries(sim PUBLIC pthread)

# file(COPY sim DESTINATION .)

add_custom_target(runsim
  DEPENDS sim
  COMMAND $<TARGET_FILE:sim>
  COMMAND printf '\\a\\a\\a')
