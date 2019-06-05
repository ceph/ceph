add_executable(rbd-nbd rbd-nbd.cc)
target_include_directories(rbd-nbd PUBLIC ${GENL_INCLUDE_DIR})
target_link_libraries(rbd-nbd librbd librados global ${GENL_LIBRARIES})
install(TARGETS rbd-nbd DESTINATION bin)
