# - Find rpma
#
# RPMA_INCLUDE_DIR - Where to find librpma.h
# RPMA_LIBRARIES - List of libraries when using rpma.
# rpma_FOUND - True if rpma found.

find_path(RPMA_INCLUDE_DIR librpma.h)
find_library(RPMA_LIBRARIES rpma)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(rpma
  DEFAULT_MSG RPMA_LIBRARIES RPMA_INCLUDE_DIR)

mark_as_advanced(
  RPMA_INCLUDE_DIR
  RPMA_LIBRARIES)

if(rpma_FOUND AND NOT TARGET rpma::rpma)
  add_library(rpma::rpma UNKNOWN IMPORTED)
  set_target_properties(rpma::rpma PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${RPMA_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${RPMA_LIBRARIES}")
endif()
