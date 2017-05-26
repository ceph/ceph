include_directories(../src)
include_directories(../support/src)
include_directories(../sim/src)

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

add_executable(dmclock-tests ${test_srcs} ${support_srcs})

if (TARGET gtest AND TARGET gtest_main)
  add_dependencies(dmclock-tests gtest gtest_main)
  target_link_libraries(dmclock-tests
    LINK_PRIVATE $<TARGET_FILE:dmclock>
    pthread
    $<TARGET_FILE:gtest>
    $<TARGET_FILE:gtest_main>)
else()
  target_link_libraries(dmclock-tests
    LINK_PRIVATE $<TARGET_FILE:dmclock> pthread ${GTEST_LIBRARIES} ${GTEST_MAIN_LIBRARIES})
endif()
  
add_dependencies(dmclock-tests dmclock)
