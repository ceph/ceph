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
    add_tox_test(mgr-dashboard-py3 TOX_ENVS py3)
    add_tox_test(mgr-dashboard-lint TOX_ENVS lint)
    add_tox_test(mgr-dashboard-check TOX_ENVS check)
    add_tox_test(mgr-dashboard-openapi TOX_ENVS openapi-check)
  endif()

  # protobuffer files generation
  add_custom_command(
    OUTPUT "${CMAKE_CURRENT_SOURCE_DIR}/services/proto/gateway_pb2.py" "${CMAKE_CURRENT_SOURCE_DIR}/services/proto/gateway_pb2_grpc.py"
    COMMAND ${Python3_EXECUTABLE} -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. dashboard/services/proto/gateway.proto
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/..
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/services/proto/gateway.proto
    COMMENT "proto generation for grpc deps")
  add_custom_target(mgr-dashboard-services-deps
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/services/proto/gateway_pb2.py ${CMAKE_CURRENT_SOURCE_DIR}/services/proto/gateway_pb2_grpc.py
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  add_custom_target(default
    ALL
    DEPENDS mgr-dashboard-services-deps)

else()
  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/frontend/dist)
    # prebuilt
    install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/frontend/dist
      DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/frontend)
    install(FILES frontend/package.json
      DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/frontend)
  endif()

endif()
