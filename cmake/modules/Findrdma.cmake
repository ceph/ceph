# - Find rdma
# Find the rdma library and includes
#
# RDMA_INCLUDE_DIR - where to find ibverbs.h, etc.
# RDMA_LIBRARIES - List of libraries when using ibverbs.
# RDMA_FOUND - True if ibverbs found.

find_path(RDMA_INCLUDE_DIR infiniband/verbs.h)

set(RDMA_NAMES ${RDMA_NAMES} ibverbs)
find_library(RDMA_LIBRARY NAMES ${RDMA_NAMES})

if (RDMA_INCLUDE_DIR AND RDMA_LIBRARY)
  set(RDMA_FOUND TRUE)
  set(RDMA_LIBRARIES ${RDMA_LIBRARY})
else ()
  set(RDMA_FOUND FALSE)
  set( RDMA_LIBRARIES )
endif ()

if (RDMA_FOUND)
  message(STATUS "Found libibverbs: ${RDMA_LIBRARY}")
else ()
  message(STATUS "Not Found libibverbs: ${RDMA_LIBRARY}")
  if (RDMA_FIND_REQUIRED)
    message(STATUS "Looked for libibverbs named ${RDMA_NAMES}.")
    message(FATAL_ERROR "Could NOT find libibverbs")
  endif ()
endif ()

# handle the QUIETLY and REQUIRED arguments and set UUID_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ibverbs DEFAULT_MSG RDMA_LIBRARIES RDMA_INCLUDE_DIR)

mark_as_advanced(
  RDMA_LIBRARY
)
