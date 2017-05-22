# - Find boost

include(FindPackageHandleStandardArgs)

find_path(Boost_INCLUDE_DIRS NAMES boost/variant.hpp
    PATHS /usr/include /usr/local/include ${BOOST_DIR}/include)

FIND_PACKAGE_HANDLE_STANDARD_ARGS(boost
  REQUIRED_VARS Boost_INCLUDE_DIRS)

if(boost_FOUND)
  set(BOOST_FOUND 1)
endif()
