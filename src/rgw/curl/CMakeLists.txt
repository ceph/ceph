add_library(rgw_curl STATIC client.cc)
target_link_libraries(rgw_curl PUBLIC ${CURL_LIBRARIES})
target_include_directories(rgw_curl PUBLIC ..)
#target_compile_definitions(rgw_curl PUBLIC BOOST_ASIO_ENABLE_HANDLER_TRACKING)
