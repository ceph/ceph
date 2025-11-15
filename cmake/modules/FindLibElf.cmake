# FindLibElf
# -----------
#
# Find libelf library and headers
#
# Result Variables
# ^^^^^^^^^^^^^^^^
#
# This will define the following variables:
#
#  LibElf_FOUND          - True if the system has libelf
#  LibElf_INCLUDE_DIRS   - Include directories needed to use libelf
#  LibElf_LIBRARIES      - Libraries needed to link to libelf
#
# and the following imported targets:
#
#  LibElf::LibElf        - The libelf library

find_path(LibElf_INCLUDE_DIR
  NAMES libelf.h
  PATH_SUFFIXES libelf
)

find_library(LibElf_LIBRARY
  NAMES elf
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LibElf
  REQUIRED_VARS LibElf_LIBRARY LibElf_INCLUDE_DIR
)

if(LibElf_FOUND)
  set(LibElf_LIBRARIES ${LibElf_LIBRARY})
  set(LibElf_INCLUDE_DIRS ${LibElf_INCLUDE_DIR})

  if(NOT TARGET LibElf::LibElf)
    add_library(LibElf::LibElf UNKNOWN IMPORTED)
    set_target_properties(LibElf::LibElf PROPERTIES
      IMPORTED_LOCATION "${LibElf_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${LibElf_INCLUDE_DIR}"
    )
  endif()
endif()

mark_as_advanced(LibElf_INCLUDE_DIR LibElf_LIBRARY)
