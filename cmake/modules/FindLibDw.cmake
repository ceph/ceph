# FindLibDw
# ----------
#
# Find libdw library and headers (part of elfutils)
#
# Result Variables
# ^^^^^^^^^^^^^^^^
#
# This will define the following variables:
#
#  LibDw_FOUND          - True if the system has libdw
#  LibDw_INCLUDE_DIRS   - Include directories needed to use libdw
#  LibDw_LIBRARIES      - Libraries needed to link to libdw
#
# and the following imported targets:
#
#  LibDw::LibDw         - The libdw library

find_path(LibDw_INCLUDE_DIR
  NAMES elfutils/libdw.h
)

find_library(LibDw_LIBRARY
  NAMES dw
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LibDw
  REQUIRED_VARS LibDw_LIBRARY LibDw_INCLUDE_DIR
)

if(LibDw_FOUND)
  set(LibDw_LIBRARIES ${LibDw_LIBRARY})
  set(LibDw_INCLUDE_DIRS ${LibDw_INCLUDE_DIR})

  if(NOT TARGET LibDw::LibDw)
    add_library(LibDw::LibDw UNKNOWN IMPORTED)
    set_target_properties(LibDw::LibDw PROPERTIES
      IMPORTED_LOCATION "${LibDw_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${LibDw_INCLUDE_DIR}"
    )
  endif()
endif()

mark_as_advanced(LibDw_INCLUDE_DIR LibDw_LIBRARY)
