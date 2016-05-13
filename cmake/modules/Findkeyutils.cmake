# Try to find Keyutils
# Once done, this will define
#
# KEYUTILS_FOUND - system has keyutils
# KEYUTILS_INCLUDE_DIR - the keyutils include directories
# KEYUTILS_LIBRARIES - link these to use keyutils

if(KEYUTILS_INCLUDE_DIR AND KEYUTILS_LIBRARIES)
	set(KEYUTILS_FIND_QUIETLY TRUE)
endif(KEYUTILS_INCLUDE_DIR AND KEYUTILS_LIBRARIES)

INCLUDE(CheckCXXSymbolExists)

# include dir

find_path(KEYUTILS_INCLUDE_DIR keyutils.h NO_DEFAULT_PATH PATHS
  /usr/include
  /opt/local/include
  /usr/local/include
)


# finally the library itself
find_library(LIBKEYUTILS NAMES keyutils)
set(KEYUTILS_LIBRARIES ${LIBKEYUTILS})

# handle the QUIETLY and REQUIRED arguments and set KEYUTILS_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(keyutils DEFAULT_MSG KEYUTILS_LIBRARIES KEYUTILS_INCLUDE_DIR)

mark_as_advanced(KEYUTILS_LIBRARIES KEYUTILS_INCLUDE_DIR)
