# - Find libnl
# Find the libnl-3 library and includes
#
# nl_INCLUDE_DIR - where to find netlink.h, etc.
# nl_<COMPONENT>_LIBRARY - library when using nl::<COMPONENT>.
# nl_FOUND - True if nl found.

find_path(nl_INCLUDE_DIR
  NAMES
    netlink/netlink.h
  PATH_SUFFIXES
    libnl3)

foreach(component "core" ${nl_FIND_COMPONENTS})
  set(nl_COMPONENTS core cli genl idiag nf route xfrm)
  list(FIND nl_COMPONENTS "${component}" found)
  if(found EQUAL -1)
    message(FATAL_ERROR "unknown libnl-3 component: ${component}")
  endif()
  if(component STREQUAL "core")
    find_library(nl_${component}_LIBRARY nl-3)
  else()
    find_library(nl_${component}_LIBRARY nl-${component}-3)
  endif()
  list(APPEND nl_LIBRARIES "nl_${component}_LIBRARY")
endforeach()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(nl
  DEFAULT_MSG ${nl_LIBRARIES} nl_INCLUDE_DIR)

mark_as_advanced(
  ${nl_LIBRARIES}
  nl_INCLUDE_DIR)

if(nl_FOUND)
  foreach(component "core" ${nl_FIND_COMPONENTS})
    if(NOT TARGET nl::${component})
      add_library(nl::${component} UNKNOWN IMPORTED)
      set_target_properties(nl::${component} PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${nl_INCLUDE_DIR}"
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
        IMPORTED_LOCATION "${nl_${component}_LIBRARY}")
      if(NOT component STREQUAL "core")
        set_target_properties(nl::${component} PROPERTIES
          INTERFACE_LINK_LIBRARIES "${nl_core_LIBRARY}")
      endif()
    endif()
  endforeach()
endif()
