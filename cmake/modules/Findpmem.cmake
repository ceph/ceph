# - Find pmem
#
# pmem_INCLUDE_DIRS - Where to find libpmem headers
# pmem_LIBRARIES - List of libraries when using libpmem.
# pmem_FOUND - True if pmem found.

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
  pkg_check_modules(PKG_pmem QUIET libpmem)
  pkg_check_modules(PKG_pmemobj QUIET libpmemobj)
endif()

foreach(component pmem ${pmem_FIND_COMPONENTS})
  if(component STREQUAL pmem)
    find_path(pmem_${component}_INCLUDE_DIR
      NAMES libpmem.h
      HINTS ${PKG_pmem_INCLUDE_DIRS})
    find_library(pmem_${component}_LIBRARY
      NAMES pmem
      HINTS ${PKG_pmem_LIBRARY_DIRS})
  elseif(component STREQUAL pmemobj)
    find_path(pmem_${component}_INCLUDE_DIR
      NAMES libpmemobj.h
      HINTS ${PKG_pmemobj_INCLUDE_DIRS})
    find_library(pmem_${component}_LIBRARY
      NAMES pmemobj
      HINTS ${PKG_pmemobj_LIBRARY_DIRS}))
  else()
    message(FATAL_ERROR "unknown libpmem component: ${component}")
  endif()
  mark_as_advanced(
    pmem_${component}_INCLUDE_DIR
    pmem_${component}_LIBRARY)
  list(APPEND pmem_INCLUDE_DIRS "pmem_${component}_INCLUDE_DIR")
  list(APPEND pmem_LIBRARIES "pmem_${component}_LIBRARY")
endforeach()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(pmem
  DEFAULT_MSG pmem_INCLUDE_DIRS pmem_LIBRARIES)

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
