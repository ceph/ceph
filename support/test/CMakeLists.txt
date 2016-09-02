include_directories(${GTEST_INCLUDE_DIR})
include_directories(../src)

set(local_flags "-Wall -pthread")

# get ready to remove this once gtest tests are made
set(srcs
  test_intrusive_heap.cc)
add_executable(test_intru_heap test_intrusive_heap.cc)

set(test_srcs test_indirect_intrusive_heap.cc)

set_source_files_properties(${srcs} ${test_srcs}
  PROPERTIES
  COMPILE_FLAGS "${local_flags}"
  )

add_executable(data-struct-tests EXCLUDE_FROM_ALL ${test_srcs})

set_target_properties(data-struct-tests
  PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ../../test)

target_link_libraries(data-struct-tests
  LINK_PRIVATE ${GTEST_LIBRARY} ${GTEST_MAIN_LIBRARY} pthread)

# for every argument, adds a test with that name, using it as a gtest filter
function(make_tests)
  foreach(targ ${ARGN})
    add_test(NAME ${targ} COMMAND data-struct-tests --gtest_filter=${targ}.*)
  endforeach()
endfunction()

make_tests(ind_intru_heap)
