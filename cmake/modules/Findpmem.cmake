# - Find pmem
#
# PMEM_INCLUDE_DIR - Where to find libpmem.h
# PMEM_LIBRARIES - List of libraries when using pmdk.
# pmem_FOUND - True if pmem found.

find_path(PMEM_INCLUDE_DIR libpmem.h)
find_library(PMEM_LIBRARIES pmem)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(pmem
  DEFAULT_MSG PMEM_LIBRARIES PMEM_INCLUDE_DIR)

mark_as_advanced(
  PMEM_INCLUDE_DIR
  PMEM_LIBRARIES)

if(pmem_FOUND AND NOT TARGET pmem::pmem)
  add_library(pmem::pmem UNKNOWN IMPORTED)
  set_target_properties(pmem::pmem PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${PMEM_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${PMEM_LIBRARIES}")
endif()
