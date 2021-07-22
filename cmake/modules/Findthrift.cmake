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

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(thrift DEFAULT_MSG thrift_LIBRARIES
                                  thrift_INCLUDE_DIR)
mark_as_advanced(thrift_LIBRARIES thrift_INCLUDE_DIR)

if(thrift_FOUND AND NOT (TARGET thrift::libthrift))
  add_library(thrift::libthrift UNKNOWN IMPORTED)
  set_target_properties(
    thrift::libthrift
    PROPERTIES IMPORTED_LOCATION ${thrift_LIBRARIES}
	       INTERFACE_INCLUDE_DIRECTORIES ${thrift_INCLUDE_DIR})
endif()
