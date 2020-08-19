function (set_library_properties_for_external_project _target _lib _preinstalled _target_include _target_lib)
  if(NOT ${_preinstalled})
    set(_libfullname "${CMAKE_SHARED_LIBRARY_PREFIX}${_lib}${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(_libpath "${CMAKE_BINARY_DIR}/external/lib/${_libfullname}")
    set(_includepath "${CMAKE_BINARY_DIR}/external/include")
    message(STATUS "__libpath " ${_libfullname})
    message(STATUS "CMAKE_BINARY_DIR" ${CMAKE_BINARY_DIR})
    file(MAKE_DIRECTORY "${_includepath}")
    set_target_properties(${_target} PROPERTIES
      INTERFACE_LINK_LIBRARIES "${_libpath}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
      IMPORTED_LOCATION "${_libpath}"
      INTERFACE_INCLUDE_DIRECTORIES "${_includepath}")
    #  set_property(TARGET ${_target} APPEND PROPERTY IMPORTED_LINK_INTERFACE_LIBRARIES "CXX")
    # Manually create the directory, it will be created as part of the build,
    # but this runs in the configuration phase, and CMake generates an error if
    # we add an include directory that does not exist yet.
  else()
    set_target_properties(${_target} PROPERTIES
    INTERFACE_LINK_LIBRARIES ${_target_lib}
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION ${_target_lib}
    INTERFACE_INCLUDE_DIRECTORIES ${_target_include})
  endif()
endfunction ()
