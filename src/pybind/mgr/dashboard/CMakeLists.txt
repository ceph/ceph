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
    add_tox_test(mgr-dashboard
      TOX_ENVS grafonnet-check)
    set(ver 0.1.0)
    set(name grafonnet-lib)
    include(ExternalProject)
    ExternalProject_Add(${name}
      URL https://github.com/grafana/${name}/archive/v${ver}/${name}-${ver}.tar.gz
      URL_MD5 0798752ed40864fa8b3db40a3c970642
      BUILD_COMMAND ""
      CONFIGURE_COMMAND ""
      INSTALL_COMMAND "")
    add_dependencies(tests
      ${name})
    ExternalProject_Get_Property(${name} SOURCE_DIR)
    set_property(
      TEST run-tox-mgr-dashboard
      APPEND
      PROPERTY ENVIRONMENT
      GRAFONNET_PATH=${SOURCE_DIR}/grafonnet)
  endif()
else()
  # prebuilt
  install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/frontend/dist
    DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/frontend)
  install(FILES frontend/package.json
    DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/frontend)
endif()
