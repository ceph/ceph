# - Find libnuma
# Find the numa library and includes
#
# NUMA_INCLUDE_DIR - where to find numa.h, etc.
# NUMA_LIBRARIES - List of libraries when using numa.
# NUMA_FOUND - True if numa found.

find_path(NUMA_INCLUDE_DIR numa.h)
find_library(NUMA_LIBRARIES numa)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(numa DEFAULT_MSG NUMA_LIBRARIES NUMA_INCLUDE_DIR)

mark_as_advanced(
  NUMA_LIBRARIES
  NUMA_INCLUDE_DIR)
