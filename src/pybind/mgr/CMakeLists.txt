add_subdirectory(dashboard)
add_subdirectory(insights)
add_subdirectory(ansible)
add_subdirectory(orchestrator_cli)

# Location needs to match default setting for mgr_module_path, currently:
# OPTION(mgr_module_path, OPT_STR, CEPH_PKGLIBDIR "/mgr")
install(DIRECTORY
  ${CMAKE_CURRENT_SOURCE_DIR}
  DESTINATION ${CEPH_INSTALL_DATADIR}
  REGEX "CMakeLists.txt" EXCLUDE
  REGEX "\\.gitignore" EXCLUDE
  REGEX "hello/.*" EXCLUDE
  REGEX "osd_perf_query/.*" EXCLUDE)
