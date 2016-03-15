add_library(json_spirit
  json_spirit_reader.cpp
  json_spirit_writer.cpp)
target_link_libraries(json_spirit ${Boost_LIBRARIES})
