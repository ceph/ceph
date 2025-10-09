include(CheckIncludeFileCXX)
check_include_file_cxx("sys/prctl.h" HAVE_SYS_PRCTL_H)

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
if(HAVE_SYS_PRCTL_H)
  target_compile_definitions(dmclock-tests PRIVATE "HAVE_SYS_PRCTL_H")
endif()
target_include_directories(dmclock-tests PRIVATE
  ../sim/src ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(dmclock-tests PRIVATE SYSTEM
  "${GTEST_INCLUDE_DIRS}")

target_link_libraries(dmclock-tests LINK_PRIVATE
  dmclock
  Threads::Threads
  GTest::GTest
  GTest::Main)

add_test(NAME dmclock-tests
  COMMAND $<TARGET_FILE:dmclock-tests>)
add_test(NAME dmclock-data-struct-tests
  COMMAND $<TARGET_FILE:dmclock-data-struct-tests>)
