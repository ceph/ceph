# - Find Qatzip
# Find the qatzip compression library and includes
#
# QATZIP_INCLUDE_DIR - where to find qatzip.h, etc.
# QATZIP_LIBRARIES - List of libraries when using qatzip.
# QATZIP_FOUND - True if qatzip found.

find_path(QATZIP_INCLUDE_DIR NAMES qatzip.h)

find_library(QATZIP_LIBRARIES NAMES qatzip)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(qatzip DEFAULT_MSG QATZIP_LIBRARIES QATZIP_INCLUDE_DIR)

mark_as_advanced(
  QATZIP_LIBRARIES
  QATZIP_INCLUDE_DIR)
