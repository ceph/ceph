# - Find Fio
# Find the fio includes
#
# FIO_INCLUDE_DIR - where to find fio.h
# FIO_FOUND - True if fio is found.

find_path(FIO_INCLUDE_DIR NAMES fio.h HINTS ENV FIO_ROOT_DIR)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(fio DEFAULT_MSG FIO_INCLUDE_DIR)

mark_as_advanced(FIO_INCLUDE_DIR)
