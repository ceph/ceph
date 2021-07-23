include(ExternalProject)

ExternalProject_Add(mgr-rook-client
  SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rook-client-python/rook_client"
  # use INSTALL_DIR for destination dir
  INSTALL_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rook_client"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>
  COMMAND       ${CMAKE_COMMAND} -E copy_directory <SOURCE_DIR>/ceph <INSTALL_DIR>/ceph
  COMMAND       ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/__init__.py <INSTALL_DIR>
  COMMAND       ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/_helper.py <INSTALL_DIR>
  BUILD_BYPRODUCTS "<INSTALL_DIR>/__init__.py"
  INSTALL_COMMAND "")

add_dependencies(ceph-mgr mgr-rook-client)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DESTINATION ${CEPH_INSTALL_DATADIR}/mgr
  ${mgr_module_install_excludes}
  REGEX "rook-client-python.*" EXCLUDE)
