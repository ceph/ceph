# This module defines thrift_LIBRARIES, libraries to link thrift_INCLUDE_DIR,
# where to find thrift headers thrift_COMPILER, thrift compiler executable
# thrift_FOUND, If false, do not try to use it.

# prefer the thrift version supplied in thrift_HOME (cmake -Dthrift_HOME then
# environment)
find_path(
  thrift_INCLUDE_DIR
  NAMES thrift/Thrift.h
  HINTS ${thrift_HOME} ENV thrift_HOME /usr/local /opt/local
  PATH_SUFFIXES include)

# prefer the thrift version supplied in thrift_HOME
find_library(
  thrift_LIBRARIES
  NAMES thrift libthrift
  HINTS ${thrift_HOME} ENV thrift_HOME /usr/local /opt/local
  PATH_SUFFIXES lib lib64)

if(thrift_INCLUDE_DIR)
  file(READ "${thrift_INCLUDE_DIR}/thrift/config.h" THRIFT_CONFIG_H_CONTENT)
  string(REGEX MATCH "#define PACKAGE_VERSION \"[0-9.]+\"" THRIFT_VERSION_DEFINITION
	 "${THRIFT_CONFIG_H_CONTENT}")
  string(REGEX MATCH "[0-9.]+" thrift_VERSION "${THRIFT_VERSION_DEFINITION}")
  set(thrift_VERSION "${thrift_VERSION}" PARENT_SCOPE)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  thrift
  REQUIRED_VARS thrift_LIBRARIES thrift_INCLUDE_DIR
  VERSION_VAR thrift_VERSION)
mark_as_advanced(thrift_LIBRARIES thrift_INCLUDE_DIR)

if(thrift_FOUND AND NOT (TARGET thrift::libthrift))
  add_library(thrift::libthrift UNKNOWN IMPORTED)
  set_target_properties(
    thrift::libthrift
    PROPERTIES IMPORTED_LOCATION ${thrift_LIBRARIES}
	       INTERFACE_INCLUDE_DIRECTORIES ${thrift_INCLUDE_DIR})
endif()
