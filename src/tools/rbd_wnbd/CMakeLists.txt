add_executable(rbd-wnbd wnbd_handler.cc rbd_wnbd.cc)
set_target_properties(
    rbd-wnbd PROPERTIES COMPILE_FLAGS
    "-fpermissive -I${WNBD_INCLUDE_DIRS}")
target_link_libraries(
    rbd-wnbd setupapi rpcrt4
    ${WNBD_LIBRARIES}
    ${Boost_FILESYSTEM_LIBRARY}
    librbd librados global)
install(TARGETS rbd-wnbd DESTINATION bin)
