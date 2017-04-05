include_directories(${GTEST_INCLUDE_DIR})
include_directories(../src)

set(local_flags "-Wall -pthread")

# dmclock does not use intrusive heap (but it does use indirect
# intrusive heap), so we won't use this code
if(false)
  set(srcs
    test_intrusive_heap.cc)
  add_executable(test_intru_heap test_intrusive_heap.cc)
  set_source_files_properties(${srcs}
    PROPERTIES
    COMPILE_FLAGS "${local_flags}")
endif(false)

set(test_srcs test_indirect_intrusive_heap.cc)

set_source_files_properties(${test_srcs}
  PROPERTIES
  COMPILE_FLAGS "${local_flags}"
  )

add_executable(dmclock-data-struct-tests EXCLUDE_FROM_ALL ${test_srcs})

target_link_libraries(dmclock-data-struct-tests
  LINK_PRIVATE gtest gtest_main pthread)

# for every argument, adds a test with that name, using it as a gtest filter
function(make_tests)
  foreach(targ ${ARGN})
    add_test(NAME ${targ}
      COMMAND dmclock-data-struct-tests --gtest_filter=${targ}.*)
  endforeach()
endfunction()

make_tests(ind_intru_heap)
