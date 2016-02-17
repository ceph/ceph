cmake_minimum_required(VERSION 2.8)

include_directories(${GTEST_INCLUDE_DIR})

set(local_flags "-g -O0")

set(core_srcs ../src/test_client.cc ../src/crimson/run_every.cc)
set(test_srcs test_test_client.cc test_server_client_map.cc)

set_source_files_properties(${core_srcs} ${test_srcs}
  PROPERTIES
  COMPILE_FLAGS "${my_compile_flags} -std=gnu++11 ${local_flags}"
  )

add_executable(client_tests EXCLUDE_FROM_ALL ${core_srcs} ${test_srcs})

target_link_libraries(client_tests
  PUBLIC pthread ${GTEST_LIBRARY} ${GTEST_MAIN_LIBRARY})

add_test(
  NAME unitTests
  COMMAND client_tests)

add_dependencies(check client_tests)
