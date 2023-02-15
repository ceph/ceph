# - Find libmotr
# Find the motr and motrhl libraries and includes
#
# motr_INCLUDE_DIR - where to find motr.hpp etc.
# motr_LIBRARIES - List of libraries when using motr.
# motr_FOUND - True if motr found.

find_package(PkgConfig QUIET REQUIRED)
pkg_search_module(PC_motr QUIET motr)

find_path(motr_INCLUDE_DIR
  NAMES motr/config.h
  HINTS ${PC_motr_INCLUDE_DIRS})
find_library(motr_LIBRARY
  NAMES motr
  HINTS ${PC_motr_LIBRARY_DIRS})
find_library(motr_helpers_LIBRARY
  NAMES motr-helpers
  HINTS ${PC_motr_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(motr
  DEFAULT_MSG
  motr_INCLUDE_DIR
  motr_LIBRARY
  motr_helpers_LIBRARY)

mark_as_advanced(
  motr_INCLUDE_DIR
  motr_LIBRARY
  motr_helpers_LIBRARY)

if(motr_FOUND)
  set(motr_LIBRARIES ${motr_LIBRARY} ${motr_helpers_LIBRARY})
  if(NOT (TARGET motr::helpers))
    add_library(motr::helpers UNKNOWN IMPORTED)
    set_target_properties(motr::helpers PROPERTIES
      IMPORTED_LOCATION "${motr_helpers_LIBRARY}")
  endif()
  if(NOT (TARGET motr::motr))
    add_library(motr::motr UNKNOWN IMPORTED)
    set_target_properties(motr::motr PROPERTIES
      INTERFACE_COMPILE_DEFINITIONS "M0_EXTERN=extern;M0_INTERNAL="
      INTERFACE_COMPILE_OPTIONS "-Wno-attributes"
      INTERFACE_INCLUDE_DIRECTORIES "${motr_INCLUDE_DIR}"
      INTERFACE_LINK_LIBRARIES motr::helpers
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${motr_LIBRARY}")
  endif()
endif()
