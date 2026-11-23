# RGW on IBM Storage Scale deployment tool.  Installed as an admin
# command alongside the rgw-standalone binaries it drives.
if(WITH_RADOSGW_STANDALONE)
  install(PROGRAMS rgw/scale/rgw-on-scale.sh
    DESTINATION ${CMAKE_INSTALL_SBINDIR}
    RENAME rgw-on-scale)
endif()

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
    IMMEDIATE @ONLY)

add_custom_target(uninstall
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
