add_library(ec_consistency STATIC
  ECReader.cc
  ECEncoder.cc
  ECEncoderSwitch.cc
  Pool.cc
  ConsistencyChecker.cc
  RadosCommands.cc
  ${PROJECT_SOURCE_DIR}/src/osd/ECUtilL.cc
  ${PROJECT_SOURCE_DIR}/src/osd/ECUtil.cc
)

target_link_libraries(ec_consistency
  librados
  global
  json_structures
)

add_executable(ceph_ec_consistency_checker
  ${CMAKE_CURRENT_SOURCE_DIR}/ceph_ec_consistency_checker.cc
  ${PROJECT_SOURCE_DIR}/src/osd/ECUtilL.cc
  ${PROJECT_SOURCE_DIR}/src/osd/ECUtil.cc)
target_link_libraries(ceph_ec_consistency_checker
  librados global ec_consistency)
install(TARGETS
ceph_ec_consistency_checker
  DESTINATION ${CMAKE_INSTALL_BINDIR})
