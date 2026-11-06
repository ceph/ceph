find_path(FoundationDB_INCLUDE_DIR NAMES foundationdb/fdb_c.h)
find_library(FoundationDB_LIBRARY NAMES fdb_c)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(FoundationDB
  REQUIRED_VARS FoundationDB_INCLUDE_DIR FoundationDB_LIBRARY
  REASON_FAILURE_MESSAGE "Install the foundationdb-clients package in the build environment.")

mark_as_advanced(FoundationDB_INCLUDE_DIR FoundationDB_LIBRARY)

if(FoundationDB_FOUND AND NOT TARGET FoundationDB::FoundationDB)
  add_library(FoundationDB::FoundationDB UNKNOWN IMPORTED)
  set_target_properties(FoundationDB::FoundationDB PROPERTIES
    IMPORTED_LOCATION "${FoundationDB_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${FoundationDB_INCLUDE_DIR}")
endif()
