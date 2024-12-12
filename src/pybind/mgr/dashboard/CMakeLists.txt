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

  # Required for building the dashboard grpc protos at build time
  set(CEPH_GRPC_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/ceph-grpc-virtualenv)

  add_custom_command(
    OUTPUT ${CEPH_GRPC_VIRTUALENV}/lib/python*/site-packages/grpc_tools
    COMMAND ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh --python=${Python3_EXECUTABLE} ${CEPH_GRPC_VIRTUALENV}
    COMMAND ${CEPH_GRPC_VIRTUALENV}/bin/pip install grpcio-tools
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "grpcio-tools is being installed")

  # protobuffer files generation
  add_custom_command(
    OUTPUT "${CMAKE_CURRENT_SOURCE_DIR}/services/proto/gateway_pb2.py" "${CMAKE_CURRENT_SOURCE_DIR}/services/proto/gateway_pb2_grpc.py"
    COMMAND . ${CEPH_GRPC_VIRTUALENV}/bin/activate && ${CEPH_GRPC_VIRTUALENV}/bin/python -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. dashboard/services/proto/gateway.proto && deactivate
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/..
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/services/proto/gateway.proto ${CEPH_GRPC_VIRTUALENV}/lib/python*/site-packages/grpc_tools
    COMMENT "proto generation for grpc deps")
  add_custom_target(mgr-dashboard-services-deps
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/services/proto/gateway_pb2.py ${CMAKE_CURRENT_SOURCE_DIR}/services/proto/gateway_pb2_grpc.py
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_custom_target(mgr-dashboard-services-build
    ALL
    DEPENDS mgr-dashboard-services-deps)

  add_dependencies(tests mgr-dashboard-services-build)

else()
  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/frontend/dist)
    # prebuilt
    install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/frontend/dist
      DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/frontend)
    install(FILES frontend/package.json
      DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/frontend)
  endif()
endif()
