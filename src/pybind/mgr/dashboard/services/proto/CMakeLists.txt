# This CMake file is primarily responsible for generating/building the dashboard NVME grpc protos at build time.
# This is done so these files can be dynamically generated and make it to the build instead of being hardcoded in the repository
# to a given version of the protoc generator.

# All three versions of the grpc_tools module (ubuntu22.04, centos9, latest) have "quirks" when asked to handle the existing NVME proto file.

# The first if(NOT LIBPROTOC_VERSION) block below explains those differences by the flags that it sets which the generation block immediately after uses.

# This will generate the correct protoc versions based on the given OS system python3 grpc module.

# Failing to find any module version it will just not generate anything but still continue the cmake build.

# For any of the most recent gprc_tools it should just work provided there are no major differences in behaviour.

# This implementation should also be able to cope with repo updates to the `src/nvmeof/gateway` module.



#####################################
# Notes about python package loading:
#####################################

# For`gateway_pb2_grpc.py` to be successfully imported by python
# These files must be created from the dashboard parent directory
# so that `gateway_pb2_grpc.py` is written with an import statement that matches the module location. e.g. import dashboard.services.proto.gateway_pb2

# WORKING_DIRECTORY: /ceph/src/pybind/mgr/dashboard/services/proto/../../..
# /usr/bin/python3 -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. dashboard/services/proto/gateway.proto

# view from python:

# <module 'dashboard.services.orchestrator' from '/ceph/src/pybind/mgr/dashboard/services/orchestrator.py'>,
# <module 'dashboard.services.nvmeof_conf' from '/ceph/src/pybind/mgr/dashboard/services/nvmeof_conf.py'>,
# <module 'dashboard.services.nvmeof_cli' from '/ceph/src/pybind/mgr/dashboard/services/nvmeof_cli.py'>,
# <module 'dashboard.services.nvmeof_client' from '/ceph/src/pybind/mgr/dashboard/services/nvmeof_client.py'>,
# <module 'dashboard.services.proto' (namespace) from ['/ceph/src/pybind/mgr/dashboard/services/proto']>,
# <module 'dashboard.services.proto.gateway_pb2' from '/ceph/src/pybind/mgr/dashboard/services/proto/gateway_pb2.py'>,
# <module 'dashboard.services.proto.gateway_pb2_grpc' from '/ceph/src/pybind/mgr/dashboard/services/proto/gateway_pb2_grpc.py'>]

# The grpc_tool generates an import statement for the gateway_pb2_grpc.py to the gateway_pb2.py based on the location of the file.
# This import statement is static and not relative and you have no control over it other than by modifying their path at generation time from the tool.

# By explicitly setting sys.path in the consuming code with an extra directory pointing at these files,
# it would be possible to generate the files from/to anywhere as modules that did not depend on the dashboard module namespace.


# Setting paths for required files and locations
set(CEPH_DASHBOARD_GATEWAY_PROTO_FILE dashboard/services/proto/gateway.proto)
set(CEPH_DASHBOARD_PARENT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../..")
set(CEPH_NVME_GATEWAY_PROTO_FILE ${CEPH_DASHBOARD_PARENT_DIR}/../../../src/nvmeof/gateway/control/proto/gateway.proto)

# Current versions of the grpc_tools.protoc module per OS
set(UBUNTU_22_04_GRPC_TOOLS_VERSION "3.5.1")
set(CENTOS_9_GRPC_TOOLS_VERSION "3.14.0")
set(FEDORA_40_GRPC_TOOLS_VERSION "3.19.6")

# Find the installed version of the grpc_tools module
execute_process(
  COMMAND ${Python3_EXECUTABLE} -m grpc_tools.protoc --version
  COMMAND awk "{ print $2 }"
  OUTPUT_VARIABLE LIBPROTOC_VERSION
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Set defaults for variables
set(GRPC_TOOLS_EXPERIMENTAL_FLAG "")
set(GRPC_TOOLS_NVME_PREPARE_SOURCE_CMD cp ${CEPH_NVME_GATEWAY_PROTO_FILE} ${CEPH_DASHBOARD_GATEWAY_PROTO_FILE})
set(GRPC_TOOLS_NVME_PREPARE_SOURCE_MSG "copy verbatim source gateway.proto file")

# Evaluate existing version and set toggles
if(NOT LIBPROTOC_VERSION)
  message(DEBUG "Unknown system. libprotoc '${LIBPROTOC_VERSION}' NOT FOUND")

elseif(LIBPROTOC_VERSION VERSION_LESS_EQUAL UBUNTU_22_04_GRPC_TOOLS_VERSION)
  message(DEBUG "System likely to be Ubuntu 22.04 or earlier. libprotoc ${LIBPROTOC_VERSION}")
  set(GRPC_TOOLS_NVME_PREPARE_SOURCE_CMD sed 's|optional ||g' ${CEPH_NVME_GATEWAY_PROTO_FILE} > ${CEPH_DASHBOARD_GATEWAY_PROTO_FILE})
  set(GRPC_TOOLS_NVME_PREPARE_SOURCE_MSG "copy amended source gateway.proto file")

elseif(LIBPROTOC_VERSION VERSION_LESS_EQUAL CENTOS_9_GRPC_TOOLS_VERSION)
  message(DEBUG "System likely to be CentOS 9 or earlier. libprotoc ${LIBPROTOC_VERSION}")
  set(GRPC_TOOLS_EXPERIMENTAL_FLAG "--experimental_allow_proto3_optional")

elseif(LIBPROTOC_VERSION VERSION_LESS FEDORA_40_GRPC_TOOLS_VERSION)
  message(DEBUG "Unknown system. libprotoc ${LIBPROTOC_VERSION}")

elseif(LIBPROTOC_VERSION VERSION_GREATER_EQUAL FEDORA_40_GRPC_TOOLS_VERSION)
  message(DEBUG "Unknown system. Newer libprotoc ${LIBPROTOC_VERSION}")
endif()

# Protobuffer files generation
if (LIBPROTOC_VERSION)

  add_custom_command(
    OUTPUT "${CEPH_DASHBOARD_PARENT_DIR}/${CEPH_DASHBOARD_GATEWAY_PROTO_FILE}"
    COMMAND ${GRPC_TOOLS_NVME_PREPARE_SOURCE_CMD}
    WORKING_DIRECTORY ${CEPH_DASHBOARD_PARENT_DIR}
    DEPENDS ${CEPH_NVME_GATEWAY_PROTO_FILE}
    COMMENT ${GRPC_TOOLS_NVME_PREPARE_SOURCE_MSG})

  add_custom_command(
    OUTPUT "${CMAKE_CURRENT_SOURCE_DIR}/gateway_pb2.py" "${CMAKE_CURRENT_SOURCE_DIR}/gateway_pb2_grpc.py"
    COMMAND ${Python3_EXECUTABLE} -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. ${CEPH_DASHBOARD_GATEWAY_PROTO_FILE} ${GRPC_TOOLS_EXPERIMENTAL_FLAG}
    WORKING_DIRECTORY ${CEPH_DASHBOARD_PARENT_DIR}
    DEPENDS ${CEPH_DASHBOARD_PARENT_DIR}/${CEPH_DASHBOARD_GATEWAY_PROTO_FILE}
    COMMENT "proto generation for grpc deps")

  add_custom_target(mgr-dashboard-services-proto
    ALL
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/gateway_pb2.py ${CMAKE_CURRENT_SOURCE_DIR}/gateway_pb2_grpc.py
    WORKING_DIRECTORY ${CEPH_DASHBOARD_PARENT_DIR})

  add_dependencies(tests mgr-dashboard-services-proto)

endif()

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DESTINATION ${CEPH_INSTALL_DATADIR}/mgr/dashboard/services
  PATTERN ".*" EXCLUDE)
