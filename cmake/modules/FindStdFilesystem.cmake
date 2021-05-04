set(_std_filesystem_test_src
  ${CMAKE_CURRENT_LIST_DIR}/FindStdFilesystem_test.cc)

macro(try_std_filesystem_library _library _result _already_included)
  set(_std_filesystem_try_compile_arg
    CXX_STANDARD 17)
  if(NOT _library STREQUAL "")
    list(APPEND _std_filesystem_try_compile_arg
      LINK_LIBRARIES ${_library})
  endif()
  try_compile(_std_filesystem_compiles
    ${CMAKE_CURRENT_BINARY_DIR}
    SOURCES ${_std_filesystem_test_src}
    ${_std_filesystem_try_compile_arg})
  unset(_std_filesystem_try_compile_arg)
  if(_std_filesystem_compiles)
    if(NOT ${_library} STREQUAL "")
      set(${_result} ${_library})
    else()
      set(${_already_included} "included by standard library")
    endif()
  endif()
  unset(_std_filesystem_compiles)
endmacro()

set(_std_filesystem_required_var "StdFilesystem_LIBRARY")
set(_std_filesystem_already_included FALSE)
foreach(library
    ""
    "stdc++fs"
    "c++experimental"
    "c++fs")
  try_std_filesystem_library("${library}" StdFilesystem_LIBRARY _std_filesystem_already_included)
  if(_std_filesystem_already_included)
    set(_std_filesystem_required_var "_std_filesystem_already_included")
    break()
  elseif(StdFilesystem_LIBRARY)
    break()
  endif()
endforeach()

unset(_std_filesystem_test_src)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(StdFilesystem
  FOUND_VAR StdFilesystem_FOUND
  REQUIRED_VARS ${_std_filesystem_required_var})

mark_as_advanced(StdFilesystem_LIBRARY)

if(StdFilesystem_FOUND AND NOT (TARGET StdFilesystem::filesystem))
  add_library(StdFilesystem::filesystem INTERFACE IMPORTED)
  if(StdFilesystem_LIBRARY)
    set_target_properties(StdFilesystem::filesystem PROPERTIES
      INTERFACE_LINK_LIBRARIES ${StdFilesystem_LIBRARY})
  endif()
endif()
