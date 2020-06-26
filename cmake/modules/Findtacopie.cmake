# Try to find tacopie
# Once done, this will define
#
# TACOPIE_FOUND - system has libtacopie
# TACOPIE_INCLUDE_DIR - the libtacopie include directories
# TACOPIE_LIBRARIES - link these to use libtacopie

# include dir

find_path(TACOPIE_INCLUDE_DIR tacopie NO_DEFAULT_PATH PATHS
  /usr/local/lib
  /usr/local/include
  /usr/local/include/tacopie
)


# finally the library itself
set(TACOPIE_LIBRARY_PATH /usr/local/lib)
find_library(LIBTACOPIE NAMES tacopie ${TACOPIE_LIBRARY_PATH})
set(TACOPIE_LIBRARIES ${LIBTACOPIE})

# handle the QUIETLY and REQUIRED arguments and set TACOPIE_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(tacopie DEFAULT_MSG TACOPIE_LIBRARIES TACOPIE_INCLUDE_DIR)

mark_as_advanced(TACOPIE_LIBRARIES TACOPIE_INCLUDE_DIR)

