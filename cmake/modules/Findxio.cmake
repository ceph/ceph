# - Find libxio
# Find libxio transport library
#
# Xio_INCLUDE_DIR -  libxio include dir
# Xio_LIBRARIES - List of libraries
# Xio_FOUND - True if libxio found.

set(_xio_include_path ${HT_DEPENDENCY_INCLUDE_DIR})
set(_xio_lib_path ${HT_DEPENDENCY_LIB_DIR})
if (EXISTS ${WITH_XIO})
  list(APPEND _xio_include_path "${WITH_XIO}/include")
  list(APPEND _xio_lib_path "${WITH_XIO}/lib")
else()
  list(APPEND _xio_include_path /usr/include /usr/local/include /opt/accelio/include)
  list(APPEND _xio_lib_path /lib /usr/lib /usr/local/lib /opt/accelio/lib)
endif()

find_path(Xio_INCLUDE_DIR libxio.h NO_DEFAULT_PATH PATHS ${_xio_include_path})

find_library(Xio_LIBRARY NO_DEFAULT_PATH NAMES xio PATHS ${_xio_lib_path})
set(Xio_LIBRARIES ${Xio_LIBRARY})

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(xio DEFAULT_MSG Xio_LIBRARY Xio_INCLUDE_DIR)

if (Xio_FOUND)
  message(STATUS "Found Xio: ${Xio_INCLUDE_DIR} ${Xio_LIBRARY}")
else ()
  message(STATUS "Not Found Xio: ${Xio_INCLUDE_DIR} ${Xio_LIBRARY}")
  if (Xio_FIND_REQUIRED)
    message(STATUS "Looked for Xio libraries named ${Xio_NAMES}.")
    message(FATAL_ERROR "Could NOT find Xio library")
  endif ()
endif ()

mark_as_advanced(
  Xio_LIBRARY
  Xio_INCLUDE_DIR
  )
