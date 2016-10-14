# - Find AIO
#
# AIO_INCLUDE - Where to find libaio.h
# AIO_LIBS - List of libraries when using AIO.
# AIO_FOUND - True if AIO found.

find_path(AIO_INCLUDE_DIR
  libaio.h
  HINTS $ENV{AIO_ROOT}/include)

find_library(AIO_LIBRARIES
  aio
  HINTS $ENV{AIO_ROOT}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(aio DEFAULT_MSG AIO_LIBRARIES AIO_INCLUDE_DIR)

mark_as_advanced(AIO_INCLUDE_DIR AIO_LIBRARIES)
