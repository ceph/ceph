set(mgr_module_install_excludes
  PATTERN "CMakeLists.txt" EXCLUDE
  PATTERN ".gitignore" EXCLUDE
  PATTERN "tox.ini" EXCLUDE
  PATTERN "requirements*.txt" EXCLUDE
  PATTERN "constraints*.txt" EXCLUDE
  PATTERN "tests/*" EXCLUDE)

add_subdirectory(dashboard)

if(WITH_MGR_ROOK_CLIENT)
  add_subdirectory(rook)
endif()
if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(mgr ${CMAKE_CURRENT_SOURCE_DIR} TOX_ENVS py3 py37 mypy flake8 jinjalint nooptional)
endif()

# Location needs to match default setting for mgr_module_path, currently:
# OPTION(mgr_module_path, OPT_STR, CEPH_INSTALL_DATADIR "/mgr")
set(mgr_modules
  alerts
  balancer
  cephadm
  crash
  # dashboard (optional)
  devicehealth
  diskprediction_local
  # hello is an example for developers, not for user
  influx
  insights
  iostat
  k8sevents
  localpool
  mds_autoscaler
  mirroring
  nfs
  orchestrator
  osd_perf_query
  osd_support
  pg_autoscaler
  progress
  prometheus
  rbd_support
  restful
  rgw
  # rook (optional)
  selftest
  smb
  snap_schedule
  stats
  status
  telegraf
  telemetry
  # tests (for testing purpose only)
  test_orchestrator
  volumes
  zabbix)

install(DIRECTORY ${mgr_modules}
  DESTINATION ${CEPH_INSTALL_DATADIR}/mgr
  ${mgr_module_install_excludes})
install(FILES mgr_module.py mgr_util.py object_format.py
  DESTINATION ${CEPH_INSTALL_DATADIR}/mgr)
