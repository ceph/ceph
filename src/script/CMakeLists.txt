option(WITH_SUBMAN "install subscription manager cron job" OFF)

if(WITH_SUBMAN)
  install(PROGRAMS
    ${CMAKE_CURRENT_SOURCE_DIR}/subman
    DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cron.hourly)
endif()

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
    IMMEDIATE @ONLY)

add_custom_target(uninstall
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
