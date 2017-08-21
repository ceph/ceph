enable_testing()

set(UNITTEST_CXX_FLAGS "-I${CMAKE_SOURCE_DIR} -I${CMAKE_SOURCE_DIR}/googletest/googlemock/include -I${CMAKE_BINARY_DIR}/googletest/googlemock/include -I${CMAKE_SOURCE_DIR}/googletest/googletest/include -I${CMAKE_BINARY_DIR}/googletest/googletest/include -fno-strict-aliasing --std=c++11")

add_executable(unittest_crush_wrapper CrushWrapper.cc)
set_target_properties(unittest_crush_wrapper PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(unittest_crush_wrapper crushcpp gtest gtest_main)
add_test(crush_wrapper unittest_crush_wrapper)

add_executable(unittest_crush_compiler CrushCompiler.cc)
set_target_properties(unittest_crush_compiler PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(unittest_crush_compiler crushcpp gtest gtest_main)
add_test(crush_compiler unittest_crush_compiler)

