# - Find libdml
# Find the dml and dmlhl libraries and includes
#
# DML_INCLUDE_DIR - where to find dml.hpp etc.
# DML_LIBRARIES - List of libraries when using dml.
# DML_HL_LIBRARIES - List of libraries when using dmlhl.
# DML_FOUND - True if DML found.


find_path(DML_INCLUDE_DIR
  dml/dml.hpp
  PATHS
  /usr/include
  /usr/local/include)

find_library(DML_LIBRARIES NAMES dml libdml PATHS
  /usr/local/
  /usr/local/lib64
  /usr/lib64
  /usr/lib)

find_library(DML_HL_LIBRARIES NAMES dmlhl libdmlhl PATHS
  /usr/local/
  /usr/local/lib64
  /usr/lib64
  /usr/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(dml DEFAULT_MSG
  DML_LIBRARIES
  DML_INCLUDE_DIR
  DML_HL_LIBRARIES)

mark_as_advanced(
  DML_LIBRARIES
  DML_INCLUDE_DIR
  DML_HL_LIBRARIES)

if(DML_FOUND)
  if(NOT (TARGET dml::dml))
    add_library(dml::dml UNKNOWN IMPORTED)
    set_target_properties(dml::dml PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${DML_INCLUDE_DIR}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${DML_LIBRARIES}")
  endif()

  if(NOT (TARGET dml::dmlhl))
    add_library(dml::dmlhl UNKNOWN IMPORTED)
    set_target_properties(dml::dmlhl PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${DML_INCLUDE_DIR}"
      INTERFACE_LINK_LIBRARIES ${CMAKE_DL_LIBS}
      INTERFACE_COMPILE_FEATURES cxx_std_17
      INTERFACE_COMPILE_DEFINITIONS "DML_HW"
      IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
      IMPORTED_LOCATION "${DML_HL_LIBRARIES}")
  endif()
endif()
