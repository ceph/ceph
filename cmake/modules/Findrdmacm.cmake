# - Find rdma cm
# Find the rdma cm library and includes
#
# RDMACM_INCLUDE_DIR - where to find cma.h, etc.
# RDMACM_LIBRARIES - List of libraries when using rdmacm.
# RDMACM_FOUND - True if rdmacm found.

find_path(RDMACM_INCLUDE_DIR rdma/rdma_cma.h)
find_library(RDMACM_LIBRARIES rdmacm)

# handle the QUIETLY and REQUIRED arguments and set UUID_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(rdmacm DEFAULT_MSG RDMACM_LIBRARIES RDMACM_INCLUDE_DIR)

mark_as_advanced(
  RDMACM_LIBRARIES
)
