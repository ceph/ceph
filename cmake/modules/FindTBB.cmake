# Find Intel Threading Building Blocks (TBB) library
#
# This module defines:
# TBB_FOUND - True if TBB is found
# TBB_INCLUDE_DIRS - TBB include directories
# TBB_LIBRARIES - TBB libraries to link against

if(NOT TBB_ROOT_DIR)
  set(TBB_ROOT_DIR $ENV{TBB_ROOT_DIR})
endif()

find_path(TBB_INCLUDE_DIR
  NAMES tbb/concurrent_hash_map.h
  PATHS ${TBB_ROOT_DIR}/include
        /usr/include
        /usr/local/include
)

find_library(TBB_LIBRARY
  NAMES tbb
  PATHS ${TBB_ROOT_DIR}/lib
        ${TBB_ROOT_DIR}/lib64
        /usr/lib
        /usr/lib64
        /usr/local/lib
        /usr/local/lib64
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TBB
  REQUIRED_VARS TBB_LIBRARY TBB_INCLUDE_DIR
)

if(TBB_FOUND)
  set(TBB_INCLUDE_DIRS ${TBB_INCLUDE_DIR})
  set(TBB_LIBRARIES ${TBB_LIBRARY})
  if(NOT TARGET TBB::tbb)
    add_library(TBB::tbb INTERFACE IMPORTED)
    set_target_properties(TBB::tbb PROPERTIES
      INTERFACE_LINK_LIBRARIES "${TBB_LIBRARIES}"
      INTERFACE_INCLUDE_DIRECTORIES "${TBB_INCLUDE_DIRS}"
    )
  endif()
endif()

mark_as_advanced(TBB_INCLUDE_DIR TBB_LIBRARY)
