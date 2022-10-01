# - Find qatzip
# Find the qatzip compression library and includes
#
# qatzip_INCLUDE_DIR - where to find qatzip.h, etc.
# qatzip_LIBRARIES - List of libraries when using qatzip.
# qatzip_FOUND - True if qatzip found.

find_path(qatzip_INCLUDE_DIR NAMES qatzip.h)
find_library(qatzip_LIBRARIES NAMES qatzip HINTS /usr/local/lib64/)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(qatzip DEFAULT_MSG qatzip_LIBRARIES qatzip_INCLUDE_DIR)

mark_as_advanced(
  qatzip_LIBRARIES
  qatzip_INCLUDE_DIR)

if(qatzip_FOUND AND NOT TARGET qatzip::qatzip)
  add_library(qatzip::qatzip SHARED IMPORTED)
  set_target_properties(qatzip::qatzip PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${qatzip_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${qatzip_LIBRARIES}")
endif()
