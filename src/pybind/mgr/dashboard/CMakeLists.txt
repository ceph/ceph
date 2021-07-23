install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DESTINATION ${CEPH_INSTALL_DATADIR}/mgr
  ${mgr_module_install_excludes}
  PATTERN "frontend/*" EXCLUDE
  PATTERN ".*" EXCLUDE)

if(WITH_MGR_DASHBOARD_FRONTEND)
  # build from source
  add_subdirectory(frontend)
  if(WITH_TESTS)
    include(AddCephTest)
    add_tox_test(mgr-dashboard TOX_ENVS py3 lint check openapi-check)
  endif()
else()
  # prebuilt
  install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/frontend/dist
    DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/frontend)
  install(FILES frontend/package.json
    DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/frontend)
endif()
