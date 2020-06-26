# Try to find cpp_redis
# Once done, this will define
#
# CPP_REDIS_FOUND - system has libcpp_redis
# CPP_REDIS_INCLUDE_DIR - the libcpp_redis include directories
# CPP_REDIS_LIBRARIES - link these to use libcpp_redis

# include dir

find_path(CPP_REDIS_INCLUDE_DIR cpp_redis NO_DEFAULT_PATH PATHS
  /usr/local/lib
  /usr/local/include
  /usr/local/include/cpp_redis
)


# finally the library itself
set(CPP_REDIS_LIBRARY_PATH /usr/local/lib)
find_library(LIBCPP_REDIS NAMES cpp_redis ${CPP_REDIS_LIBRARY_PATH})
set(CPP_REDIS_LIBRARIES ${LIBCPP_REDIS})

# handle the QUIETLY and REQUIRED arguments and set CPP_REDIS_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(cpp_redis DEFAULT_MSG CPP_REDIS_LIBRARIES CPP_REDIS_INCLUDE_DIR)

mark_as_advanced(CPP_REDIS_LIBRARIES CPP_REDIS_INCLUDE_DIR)

