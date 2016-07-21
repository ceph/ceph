option(WITH_SUBMAN "install subscription manager cron job" OFF)

if(WITH_SUBMAN)
  install(PROGRAMS
    ${CMAKE_CURRENT_SOURCE_DIR}/subman
    DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cron.hourly)
endif()
