# - Find Tcmalloc
# Find the native Tcmalloc includes and library
#
# Tcmalloc_INCLUDE_DIR - where to find Tcmalloc.h, etc.
# Tcmalloc_LIBRARIES - List of libraries when using Tcmalloc.
# Tcmalloc_FOUND - True if Tcmalloc found.

find_path(Tcmalloc_INCLUDE_DIR google/tcmalloc.h PATHS
  /usr/include
  /opt/local/include
  /usr/local/include)

find_library(Tcmalloc_LIBRARY
  NAMES tcmalloc_minimal tcmalloc
  PATHS /lib /usr/lib /usr/local/lib /opt/local/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Tcmalloc
  FOUND_VAR Tcmalloc_FOUND
  REQUIRED_VARS Tcmalloc_INCLUDE_DIR Tcmalloc_LIBRARY)

if(Tcmalloc_FOUND)
  set(Tcmalloc_LIBRARIES ${Tcmalloc_LIBRARY})
endif()

mark_as_advanced(
  Tcmalloc_LIBRARY
  Tcmalloc_INCLUDE_DIR)
