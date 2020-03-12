add_custom_command(
  OUTPUT rook_client/__init__.py
  COMMAND ./generate_rook_ceph_client.sh
  DEPENDS rook_cluster.py
  COMMENT "generating mgr/rook/rook_client"
  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/src/pybind/mgr/rook")

add_custom_target(mgr-rook-client
  ALL
  DEPENDS rook_client/__init__.py
  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/src/pybind/mgr/rook"
)

add_dependencies(ceph-mgr mgr-rook-client)
