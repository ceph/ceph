cmake_minimum_required(VERSION 2.8.11)

# add_subdirectory(support/src)
add_subdirectory(src)
add_subdirectory(sim)

enable_testing()
add_subdirectory(test)
add_subdirectory(support/test)
add_test(NAME dmclock-tests
  COMMAND $<TARGET_FILE:dmclock-tests>)
add_test(NAME dmclock-data-struct-tests
  COMMAND $<TARGET_FILE:dmclock-data-struct-tests>)
