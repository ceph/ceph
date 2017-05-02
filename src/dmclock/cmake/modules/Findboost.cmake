# - Find boost

find_path(BOOST_INCLUDE_DIR NAMES boost/variant.hpp
    PATHS /usr/include /usr/local/include ${BOOST_DIR}/include)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(boost
  REQUIRED_VARS BOOST_INCLUDE_DIR)

if(boost_FOUND)
  set(BOOST_FOUND 1)
endif()
if(BOOST_FOUND)
  set(BOOST_INCLUDES ${BOOST_INCLUDE_DIR})
endif()
