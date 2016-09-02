# TO REMOVE include_directories(${GTEST_INCLUDE_DIR})
include_directories(../src)
include_directories(../support/src)
include_directories(../sim/src)
include_directories(${BOOST_INCLUDE_DIR})

set(support_srcs ../sim/src/test_dmclock.cc)
set(test_srcs
  test_test_client.cc
  test_dmclock_server.cc
  test_dmclock_client.cc
  )

set_source_files_properties(${core_srcs} ${test_srcs}
  PROPERTIES
  COMPILE_FLAGS "${local_flags}"
  )

add_executable(dmclock-tests EXCLUDE_FROM_ALL ${test_srcs} ${support_srcs})

if (TARGET gtest AND TARGET gtest_main)
  add_dependencies(dmclock-tests gtest gtest_main)
  target_link_libraries(dmclock-tests
    LINK_PRIVATE $<TARGET_FILE:dmclock>
    pthread
    $<TARGET_FILE:gtest>
    $<TARGET_FILE:gtest_main>)
else()
  target_link_libraries(dmclock-tests
    LINK_PRIVATE $<TARGET_FILE:dmclock> pthread ${GTEST_LIBRARY} ${GTEST_MAIN_LIBRARY})
endif()
  

add_dependencies(dmclock-tests dmclock data-struct-tests)

# for every argument, adds a test with that name, using it as a gtest filter
function(dmclock_make_tests)
  foreach(targ ${ARGN})
    add_test(NAME ${targ} COMMAND dmclock-tests --gtest_filter=${targ}.*)
  endforeach()
endfunction()

dmclock_make_tests(dmclock_server dmclock_server_pull dmclock_client test_client)

add_dependencies(dmclock-check dmclock-tests)
