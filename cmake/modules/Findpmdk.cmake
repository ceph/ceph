# - Find pmdk
#
# pmdk_INCLUDE_DIRS - Where to find pmdk headers
# pmdk_LIBRARIES - List of libraries when using pmdk.
# pmdk_FOUND - True if pmdk found.

find_package(PkgConfig QUIET REQUIRED)

# all pmdk libraries depend on pmem, so always find it
set(pmdk_FIND_COMPONENTS ${pmdk_FIND_COMPONENTS} pmem)
list(REMOVE_DUPLICATES pmdk_FIND_COMPONENTS)

foreach(component ${pmdk_FIND_COMPONENTS})
  set(pmdk_COMPONENTS pmem pmemobj)
  list(FIND pmdk_COMPONENTS "${component}" found)
  if(found EQUAL -1)
    message(FATAL_ERROR "unknown pmdk component: ${component}")
  endif()
  pkg_check_modules(PKG_${component} QUIET "lib${component}")
  if(NOT pmdk_VERSION_STRING OR PKG_${component}_VERSION VERSION_LESS pmdk_VERSION_STRING)
    set(pmdk_VERSION_STRING ${PKG_${component}_VERSION})
  endif()
  find_path(pmdk_${component}_INCLUDE_DIR
    NAMES lib${component}.h
    HINTS ${PKG_${component}_INCLUDE_DIRS})
  find_library(pmdk_${component}_LIBRARY
    NAMES ${component}
    HINTS ${PKG_${component}_LIBRARY_DIRS})
  mark_as_advanced(
    pmdk_${component}_INCLUDE_DIR
    pmdk_${component}_LIBRARY)
  list(APPEND pmdk_INCLUDE_DIRS "pmdk_${component}_INCLUDE_DIR")
  list(APPEND pmdk_LIBRARIES "pmdk_${component}_LIBRARY")
endforeach()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(pmdk
  REQUIRED_VARS pmdk_INCLUDE_DIRS pmdk_LIBRARIES
  VERSION_VAR pmdk_VERSION_STRING)

mark_as_advanced(
  pmdk_INCLUDE_DIRS
  pmdk_LIBRARIES)

if(pmdk_FOUND)
  foreach(component pmem ${pmdk_FIND_COMPONENTS})
    if(NOT TARGET pmdk::${component})
      add_library(pmdk::${component} UNKNOWN IMPORTED)
      set_target_properties(pmdk::${component} PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${pmdk_${component}_INCLUDE_DIR}"
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
        IMPORTED_LOCATION "${pmdk_${component}_LIBRARY}")
      # all pmdk libraries call into pmdk::pmem
      if(NOT component STREQUAL pmem)
        set_target_properties(pmdk::${component} PROPERTIES
          INTERFACE_LINK_LIBRARIES pmdk::pmem)
      endif()
    endif()
  endforeach()
endif()
