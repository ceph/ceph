cmake_minimum_required(VERSION 2.8)

include_directories(${GTEST_INCLUDE_DIR})
include_directories(../src/crimson/src)

# set(local_flags "-g -O0")

set(core_srcs ../src/crimson/src/run_every.cc ../src/test_dmclock.cc)
set(test_srcs
  test_test_client.cc
  test_dmclock_server.cc test_dmclock_client.cc)

set_source_files_properties(${core_srcs} ${test_srcs}
  PROPERTIES
  COMPILE_FLAGS "${my_compile_flags} -std=gnu++11 ${local_flags}"
  )

add_executable(tests EXCLUDE_FROM_ALL ${core_srcs} ${test_srcs})

target_link_libraries(tests
  PUBLIC pthread ${GTEST_LIBRARY} ${GTEST_MAIN_LIBRARY})

# for every argument, adds a test with that name, using it as a gtest filter
function(make_tests)
  foreach(targ ${ARGN})
    add_test(NAME ${targ} COMMAND tests --gtest_filter=${targ}.*)
  endforeach()
endfunction()

make_tests(dmclock_server dmclock_server_pull dmclock_client test_client)

add_dependencies(check tests)
