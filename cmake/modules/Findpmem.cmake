# Try to find libpmem
#
# Once done, this will define
#
# PMEM_FOUND
# PMEM_INCLUDE_DIR
# PMEM_LIBRARY

find_path(PMEM_INCLUDE_DIR NAMES libpmem.h)
find_library(PMEM_LIBRARY NAMES pmem)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(pmem DEFAULT_MSG PMEM_LIBRARY PMEM_INCLUDE_DIR)

mark_as_advanced(PMEM_INCLUDE_DIR PMEM_LIBRARY)
