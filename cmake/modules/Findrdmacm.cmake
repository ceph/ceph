# - Find rdma cm
# Find the rdma cm library and includes
#
# RDMACM_INCLUDE_DIR - where to find cma.h, etc.
# RDMACM_LIBRARIES - List of libraries when using rdmacm.
# RDMACM_FOUND - True if rdmacm found.

find_path(RDMACM_INCLUDE_DIR rdma/rdma_cma.h)
find_library(RDMACM_LIBRARIES rdmacm)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(rdmacm DEFAULT_MSG RDMACM_LIBRARIES RDMACM_INCLUDE_DIR)

if(RDMACM_FOUND)
  if(NOT TARGET RDMA::RDMAcm)
    add_library(RDMA::RDMAcm UNKNOWN IMPORTED)
  endif()
  set_target_properties(RDMA::RDMAcm PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${RDMACM_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${RDMACM_LIBRARIES}")
endif()

mark_as_advanced(
  RDMACM_LIBRARIES
)
