find_path(SQLITE3_INCLUDE_DIR NAMES sqlite3.h PATHS /usr/include /usr/local/include)
find_library(SQLITE3_LIBRARY NAMES sqlite3)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SQLITE3 DEFAULT_MSG SQLITE3_LIBRARY SQLITE3_INCLUDE_DIR)

if(SQLITE3_FOUND)
  set(SQLITE3_LIBRARIES ${SQLITE3_LIBRARY})
  set(SQLITE3_INCLUDE_DIRS ${SQLITE3_INCLUDE_DIR})
endif()
