add_library(json_spirit STATIC
  json_spirit_reader.cpp
  json_spirit_writer.cpp)
target_link_libraries(json_spirit common_utf8 Boost::thread)
