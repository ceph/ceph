add_executable(rbd-nbd rbd-nbd.cc)
target_link_libraries(rbd-nbd librbd librados global)
install(TARGETS rbd-nbd DESTINATION bin)
