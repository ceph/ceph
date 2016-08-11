# - Find libxio
# Find libxio transport library
#
# XIO_INCLUDE_DIR -  libxio include dir
# XIO_LIBRARIES - List of libraries
# XIO_FOUND - True if libxio found.

if(WITH_XIO AND EXISTS ${WITH_XIO})
  find_path(XIO_INCLUDE_DIR libxio.h HINTS "${WITH_XIO}/include")
  find_library(XIO_LIBRARY xio HINTS "${WITH_XIO}/lib")
else()
  find_path(XIO_INCLUDE_DIR libxio.h)
  find_library(XIO_LIBRARY xio)
endif()

set(XIO_LIBRARIES ${XIO_LIBRARY})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(xio DEFAULT_MSG XIO_LIBRARY XIO_INCLUDE_DIR)

mark_as_advanced(
  XIO_LIBRARY
  XIO_INCLUDE_DIR
  )
