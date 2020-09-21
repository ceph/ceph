# Try to find Keyutils
# Once done, this will define
#
# KEYUTILS_FOUND - system has keyutils
# KEYUTILS_INCLUDE_DIR - the keyutils include directories
# KEYUTILS_LIBRARIES - link these to use keyutils

find_path(KEYUTILS_INCLUDE_DIR keyutils.h PATHS
  /opt/local/include
  /usr/local/include
)

find_library(KEYUTILS_LIBRARIES NAMES keyutils)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(keyutils
  DEFAULT_MSG KEYUTILS_LIBRARIES KEYUTILS_INCLUDE_DIR)

mark_as_advanced(KEYUTILS_LIBRARIES KEYUTILS_INCLUDE_DIR)

if(KEYUTILS_FOUND AND NOT (TARGET keyutils::keyutils))
  add_library(keyutils::keyutils UNKNOWN IMPORTED)
  set_target_properties(keyutils::keyutils PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${KEYUTILS_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${KEYUTILS_LIBRARIES}")
endif()
