# - Find QATzip
# Find the QATzip compression library and includes
#
# QATzip_INCLUDE_DIR - where to find QATzip.h, etc.
# QATzip_LIBRARIES - List of libraries when using QATzip.
# QATzip_FOUND - True if QATzip found.

find_package(PkgConfig QUIET)
pkg_search_module(PC_QATzip qatzip QUIET)

find_path(QATzip_INCLUDE_DIR
  NAMES qatzip.h
  HINTS ${PC_QATzip_INCLUDE_DIRS})

find_library(QATzip_LIBRARIES
  NAMES qatzip
  HINTS ${PC_QATzip_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(QATzip DEFAULT_MSG QATzip_LIBRARIES QATzip_INCLUDE_DIR)

mark_as_advanced(
  QATzip_LIBRARIES
  QATzip_INCLUDE_DIR)

if(QATzip_FOUND AND NOT TARGET QAT::zip)
  add_library(QAT::zip SHARED IMPORTED)
  set_target_properties(QAT::zip PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${QATzip_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${QATzip_LIBRARIES}")
endif()
