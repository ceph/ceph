set(_std_filesystem_test_src
  ${CMAKE_CURRENT_LIST_DIR}/FindStdFilesystem_test.cc)

macro(try_std_filesystem_library _library _result)
  try_compile(_std_filesystem_compiles
    ${CMAKE_CURRENT_BINARY_DIR}
    SOURCES ${_std_filesystem_test_src}
    CMAKE_FLAGS -DCMAKE_CXX_FLAGS="-std=c++17"
    LINK_LIBRARIES ${_library})
  if(_std_filesystem_compiles)
    set(${_result} ${_library})
  endif()
endmacro()


if(NOT StdFilesystem_LIBRARY)
  try_std_filesystem_library("stdc++fs" StdFilesystem_LIBRARY)
endif()
if(NOT StdFilesystem_LIBRARY)
  try_std_filesystem_library("c++experimental" StdFilesystem_LIBRARY)
endif()
if(NOT StdFilesystem_LIBRARY)
  try_std_filesystem_library("c++fs" StdFilesystem_LIBRARY)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(StdFilesystem
  FOUND_VAR StdFilesystem_FOUND
  REQUIRED_VARS StdFilesystem_LIBRARY)

mark_as_advanced(StdFilesystem_LIBRARY)

if(StdFilesystem_FOUND AND NOT (TARGET StdFilesystem::filesystem))
  add_library(StdFilesystem::filesystem INTERFACE IMPORTED)
  set_target_properties(StdFilesystem::filesystem PROPERTIES
      INTERFACE_LINK_LIBRARIES ${StdFilesystem_LIBRARY})
endif()
