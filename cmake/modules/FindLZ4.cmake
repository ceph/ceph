# Try to find liblz4
#
# Once done, this will define
#
# LZ4_FOUND
# LZ4_INCLUDE_DIR
# LZ4_LIBRARY

find_path(LZ4_INCLUDE_DIR NAMES lz4.h)
find_library(LZ4_LIBRARY NAMES lz4)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LZ4 DEFAULT_MSG LZ4_LIBRARY LZ4_INCLUDE_DIR)

mark_as_advanced(LZ4_INCLUDE_DIR LZ4_LIBRARY)
