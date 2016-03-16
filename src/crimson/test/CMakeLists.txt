cmake_minimum_required(VERSION 2.8)

include_directories(${GTEST_INCLUDE_DIR})
include_directories(../src)

set(local_flags "-std=gnu++11 -Wall -pthread")

set(local_flags "${local_flags} -g -O0")

set(srcs
  test_intrusive_heap.cc
  test_ind_intru_heap.cc)

set(test_srcs test_indirect_intrusive_heap.cc)

set_source_files_properties(${srcs} ${test_src}
  PROPERTIES
  COMPILE_FLAGS "${my_compile_flags} ${local_flags}"
  )

add_executable(crim_tests EXCLUDE_FROM_ALL ${test_srcs})
add_executable(test_intru_heap test_intrusive_heap.cc)
add_executable(test_ind_intru_heap test_ind_intru_heap.cc)

target_link_libraries(crim_tests
  PUBLIC pthread ${GTEST_LIBRARY} ${GTEST_MAIN_LIBRARY})

# for every argument, adds a test with that name, using it as a gtest filter
function(make_tests)
  foreach(targ ${ARGN})
    add_test(NAME ${targ} COMMAND crim_tests --gtest_filter=${targ}.*)
  endforeach()
endfunction()

make_tests(ind_intru_heap)

add_dependencies(check crim_tests)




# target_link_libraries(sim PUBLIC pthread)

# add_custom_target(crimtests
#   DEPENDS test_intru_heap test_ind_intru_heap
#   COMMAND $<TARGET_FILE:test_intru_heap>
#   COMMAND $<TARGET_FILE:test_ind_intru_heap>
#   COMMAND printf '\\a\\a\\a')
