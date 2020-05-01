if(WITH_MGR_DASHBOARD_FRONTEND)
  add_subdirectory(dashboard)
endif()
if(WITH_MGR_ROOK_CLIENT)
  add_subdirectory(rook)
endif()
if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(mgr ${CMAKE_CURRENT_SOURCE_DIR} TOX_ENVS mypy)
endif()

# Location needs to match default setting for mgr_module_path, currently:
# OPTION(mgr_module_path, OPT_STR, CEPH_PKGLIBDIR "/mgr")
install(DIRECTORY
  ${CMAKE_CURRENT_SOURCE_DIR}
  DESTINATION ${CEPH_INSTALL_DATADIR}
  REGEX "CMakeLists.txt" EXCLUDE
  REGEX "\\.gitignore" EXCLUDE
  REGEX ".*\\.pyi" EXCLUDE
  REGEX "hello/.*" EXCLUDE
  REGEX "tests/.*" EXCLUDE
  REGEX "rook/rook-client-python.*" EXCLUDE
  REGEX "osd_perf_query/.*" EXCLUDE
  REGEX "tox.ini" EXCLUDE
  REGEX "requirements.txt" EXCLUDE)
