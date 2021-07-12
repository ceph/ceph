# - Find ucx
# Find the ucx library and includes
#
# UCX_INC_DIR - where to find memory_type.h, etc.
# UCXlib - List of libraries when using ucx.
# UCS_FOUND - True if ucs found.

find_path(UCX_INC_DIR ucs/memory/memory_type.h)

find_library(UCS_LIB ucs)
find_library(UCT_LIB uct)
find_library(UCP_LIB ucp)
find_library(UCM_LIB ucm)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ucx DEFAULT_MSG UCS_LIB UCT_LIB UCP_LIB UCM_LIB UCX_INC_DIR)

if(UCX_FOUND)
  if(NOT TARGET UCX::UCXlibucs)
    add_library(UCX::UCXlibucs UNKNOWN IMPORTED)
  endif()
  set_target_properties(UCX::UCXlibucs PROPERTIES
    IMPORTED_LOCATION "${UCS_LIB}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    INTERFACE_INCLUDE_DIRECTORIES "${UCX_INC_DIR}")

  if(NOT TARGET UCX::UCXlibuct)
    add_library(UCX::UCXlibuct UNKNOWN IMPORTED)
  endif()
  set_target_properties(UCX::UCXlibuct PROPERTIES
    IMPORTED_LOCATION "${UCT_LIB}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    INTERFACE_INCLUDE_DIRECTORIES "${UCX_INC_DIR}")

  if(NOT TARGET UCX::UCXlibucp)
    add_library(UCX::UCXlibucp UNKNOWN IMPORTED)
  endif()
  set_target_properties(UCX::UCXlibucp PROPERTIES
    IMPORTED_LOCATION "${UCP_LIB}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    INTERFACE_INCLUDE_DIRECTORIES "${UCX_INC_DIR}")

  if(NOT TARGET UCX::UCXlibucm)
    add_library(UCX::UCXlibucm UNKNOWN IMPORTED)
  endif()
  set_target_properties(UCX::UCXlibucm PROPERTIES
    IMPORTED_LOCATION "${UCM_LIB}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    INTERFACE_INCLUDE_DIRECTORIES "${UCX_INC_DIR}")
endif()

mark_as_advanced(
  UCX_INC_DIR
  UCS_LIB
  UCT_LIB
  UCP_LIB
  UCM_LIB
)
