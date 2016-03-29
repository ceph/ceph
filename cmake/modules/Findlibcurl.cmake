# Try to find libcurl
# Once done, this will define
#
# CURL_FOUND - system has curl
# CURL_INCLUDE_DIR - the curl include directories
# CURL_LIBRARIES - link these to use curl
# HAVE_CURL_MULTI_WAIT - curl provides a curl_multi_wait() function

if(CURL_INCLUDE_DIR AND CURL_LIBRARIES)
	set(CURL_FIND_QUIETLY TRUE)
endif(CURL_INCLUDE_DIR AND CURL_LIBRARIES)

INCLUDE(CheckCXXSymbolExists)

# include dir

find_path(CURL_INCLUDE_DIR curl.h NO_DEFAULT_PATH PATHS
  /usr/include
  /usr/include/curl
  /opt/local/include
  /usr/local/include
)


# finally the library itself
find_library(LIBCURL NAMES curl)
set(CURL_LIBRARIES ${LIBCURL})


# check curl/multi.h for curl_multi_wait()
set(CMAKE_REQUIRED_INCLUDES ${CURL_INCLUDE_DIR})
set(CMAKE_REQUIRED_LIBRARIES ${CURL_LIBRARIES})
CHECK_SYMBOL_EXISTS(curl_multi_wait "curl.h" HAVE_CURL_MULTI_WAIT)

# handle the QUIETLY and REQUIRED arguments and set CURL_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libcurl DEFAULT_MSG CURL_LIBRARIES CURL_INCLUDE_DIR)

mark_as_advanced(CURL_LIBRARIES CURL_INCLUDE_DIR HAVE_CURL_MULTI_WAIT)
