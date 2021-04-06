# - Find pmem
#
# pmem_INCLUDE_DIRS - Where to find libpmem headers
# pmem_LIBRARIES - List of libraries when using libpmem.
# pmem_FOUND - True if pmem found.

find_package(PkgConfig QUIET REQUIRED)

# all pmem libraries depend on pmem, so always find it
set(pmem_FIND_COMPONENTS ${pmem_FIND_COMPONENTS} pmem)
list(REMOVE_DUPLICATES pmem_FIND_COMPONENTS)

foreach(component ${pmem_FIND_COMPONENTS})
  set(pmem_COMPONENTS pmem pmemobj)
  list(FIND pmem_COMPONENTS "${component}" found)
  if(found EQUAL -1)
    message(FATAL_ERROR "unknown libpmem component: ${component}")
  endif()
  set(pkg_module_spec "lib${component}")
  if(pmem_FIND_VERSION_EXACT)
    set(pkg_module_spec "lib${component}=${pmem_FIND_VERSION}")
  elseif(pmem_FIND_VERSION)
    set(pkg_module_spec "lib${component}>=${pmem_FIND_VERSION}")
  endif()
  pkg_check_modules(PKG_${component} QUIET ${pkg_module_spec})
  if(NOT pmem_VERSION_STRING OR PKG_${component}_VERSION VERSION_LESS pmem_VERSION_STRING)
    set(pmem_VERSION_STRING ${PKG_${component}_VERSION})
  endif()
  pkg_check_modules(PKG_${component} QUIET lib${component})
  find_path(pmem_${component}_INCLUDE_DIR
    NAMES lib${component}.h
    HINTS ${PKG_${component}_INCLUDE_DIRS})
  find_library(pmem_${component}_LIBRARY
    NAMES ${component}
    HINTS ${PKG_${component}_LIBRARY_DIRS})
  mark_as_advanced(
    pmem_${component}_INCLUDE_DIR
    pmem_${component}_LIBRARY)
  list(APPEND pmem_INCLUDE_DIRS "pmem_${component}_INCLUDE_DIR")
  list(APPEND pmem_LIBRARIES "pmem_${component}_LIBRARY")
endforeach()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(pmem
  REQUIRED_VARS pmem_INCLUDE_DIRS pmem_LIBRARIES
  VERSION_VAR pmem_VERSION_STRING)

mark_as_advanced(
  pmem_INCLUDE_DIRS
  pmem_LIBRARIES)

if(pmem_FOUND)
  foreach(component pmem ${pmem_FIND_COMPONENTS})
    if(NOT TARGET pmem::${component})
      add_library(pmem::${component} UNKNOWN IMPORTED)
      set_target_properties(pmem::${component} PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${pmem_${component}_INCLUDE_DIR}"
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
        IMPORTED_LOCATION "${pmem_${component}_LIBRARY}")
      # all pmem libraries calls into pmem::pmem
      if(NOT component STREQUAL pmem)
        set_target_properties(pmem::${component} PROPERTIES
          INTERFACE_LINK_LIBRARIES pmem::pmem)
      endif()
    endif()
  endforeach()
endif()
