cmake_minimum_required(VERSION 2.8)

include_directories(${GTEST_INCLUDE_DIR})
include_directories(../src)

set(local_flags "-std=gnu++11 -Wall -pthread")

set(srcs
  test_intrusive_heap.cc
  test_ind_intru_heap.cc)

set_source_files_properties(${srcs}
  PROPERTIES
  COMPILE_FLAGS "${my_compile_flags} ${local_flags}"
  )

add_executable(test_intru_heap test_intrusive_heap.cc)
add_executable(test_ind_intru_heap test_ind_intru_heap.cc)

# target_link_libraries(sim PUBLIC pthread)

add_custom_target(runheaptests
  DEPENDS test_intru_heap test_ind_intru_heap
  COMMAND $<TARGET_FILE:test_intru_heap>
  COMMAND $<TARGET_FILE:test_ind_intru_heap>
  COMMAND printf '\\a\\a\\a')
