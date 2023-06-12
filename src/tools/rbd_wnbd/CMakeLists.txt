add_executable(
    rbd-wnbd
    rbd_mapping.cc rbd_mapping_config.cc
    rbd_wnbd.cc wnbd_handler.cc wnbd_wmi.cc
    ../../common/win32/code_page.rc)
set_target_properties(
    rbd-wnbd PROPERTIES COMPILE_FLAGS
    "-fpermissive -I${WNBD_INCLUDE_DIRS}")
target_link_libraries(
    rbd-wnbd setupapi rpcrt4
    wbemuuid oleaut32
    ${WNBD_LIBRARIES}
    ${Boost_FILESYSTEM_LIBRARY}
    librbd librados global)
install(TARGETS rbd-wnbd DESTINATION bin)
