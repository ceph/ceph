# - Find pmem
#
# pmem_INCLUDE_DIRS - Where to find libpmem headers
# pmem_LIBRARIES - List of libraries when using libpmem.
# pmem_FOUND - True if pmem found.

foreach(component pmem ${pmem_FIND_COMPONENTS})
  if(component STREQUAL pmem)
    find_path(pmem_${component}_INCLUDE_DIR libpmem.h)
    find_library(pmem_${component}_LIBRARY pmem)
  elseif(component STREQUAL pmemobj)
    find_path(pmem_${component}_INCLUDE_DIR libpmemobj.h)
    find_library(pmem_${component}_LIBRARY pmemobj)
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
