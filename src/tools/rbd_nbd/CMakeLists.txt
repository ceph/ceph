add_executable(rbd-nbd rbd-nbd.cc)
target_link_libraries(rbd-nbd librbd librados-cxx global)
install(TARGETS rbd-nbd DESTINATION bin)
