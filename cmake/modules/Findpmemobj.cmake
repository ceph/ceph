# - Find pmemobj
#
# PMEMOBJ_INCLUDE_DIR - Where to find libpmemobj.h
# PMEMOBJ_LIBRARIES - List of libraries when using pmdk obj.
# pmemobj_FOUND - True if pmemobj found.

find_path(PMEMOBJ_INCLUDE_DIR libpmemobj.h)
find_library(PMEMOBJ_LIBRARIES pmemobj)

find_package_handle_standard_args(pmemobj
  DEFAULT_MSG PMEMOBJ_LIBRARIES PMEMOBJ_INCLUDE_DIR)

mark_as_advanced(
  PMEMOBJ_INCLUDE_DIR
  PMEMOBJ_LIBRARIES)

if(pmemobj_FOUND AND NOT TARGET pmem::pmemobj)
  add_library(pmem::pmemobj UNKNOWN IMPORTED)
  set_target_properties(pmem::pmemobj PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${PMEMOBJ_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${PMEMOBJ_LIBRARIES}")
endif()
