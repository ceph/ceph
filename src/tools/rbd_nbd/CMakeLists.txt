add_executable(rbd-nbd rbd-nbd.cc
  $<TARGET_OBJECTS:parse_secret_objs>)
target_link_libraries(rbd-nbd librbd librados global keyutils
  ${Boost_REGEX_LIBRARY})
install(TARGETS rbd-nbd DESTINATION bin)
