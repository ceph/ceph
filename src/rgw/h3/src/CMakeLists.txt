set(rgw_h3_quiche_srcs
  address_validation.cc
  config.cc
  connection.cc
  error.cc
  listener.cc
  ssl.cc
)
add_library(rgw_h3_quiche SHARED ${rgw_h3_quiche_srcs})
target_link_libraries(rgw_h3_quiche PUBLIC
  rgw_h3_quiche_headers Boost::system Quiche::Quiche OpenSSL::SSL)
set_target_properties(rgw_h3_quiche PROPERTIES VERSION 1.0.0 SOVERSION 1
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/h3")
