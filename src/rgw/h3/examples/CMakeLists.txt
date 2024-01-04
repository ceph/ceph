find_package(Boost 1.79 REQUIRED COMPONENTS program_options)

add_executable(h3_zero h3_zero.cc)
target_link_libraries(h3_zero PRIVATE rgw_h3_quiche Boost::program_options)
