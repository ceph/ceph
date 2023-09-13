find_package(nl REQUIRED genl)
add_executable(rbd-nbd rbd-nbd.cc)
target_link_libraries(rbd-nbd librbd librados global nl::genl)
install(TARGETS rbd-nbd DESTINATION bin)
