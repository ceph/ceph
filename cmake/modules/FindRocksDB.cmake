# Find the native Rocksdb includes and library
# This module defines
#  ROCKSDB_INCLUDE_DIR, where to find rocksdb/db.h, Set when
#                       ROCKSDB_INCLUDE_DIR is found.
#  ROCKSDB_LIBRARIES, libraries to link against to use Rocksdb.
#  ROCKSDB_FOUND, If false, do not try to use Rocksdb.
#  ROCKSDB_VERSION_STRING
#  ROCKSDB_VERSION_MAJOR
#  ROCKSDB_VERSION_MINOR
#  ROCKSDB_VERSION_PATCH

find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h)

find_library(ROCKSDB_LIBRARIES rocksdb)

if(ROCKSDB_INCLUDE_DIR AND EXISTS "${ROCKSDB_INCLUDE_DIR}/rocksdb/version.h")
  foreach(ver "MAJOR" "MINOR" "PATCH")
    file(STRINGS "${ROCKSDB_INCLUDE_DIR}/rocksdb/version.h" ROCKSDB_VER_${ver}_LINE
      REGEX "^#define[ \t]+ROCKSDB_${ver}[ \t]+[0-9]+$")
    string(REGEX REPLACE "^#define[ \t]+ROCKSDB_${ver}[ \t]+([0-9]+)$"
      "\\1" ROCKSDB_VERSION_${ver} "${ROCKSDB_VER_${ver}_LINE}")
    unset(${ROCKSDB_VER_${ver}_LINE})
  endforeach()
  set(ROCKSDB_VERSION_STRING
    "${ROCKSDB_VERSION_MAJOR}.${ROCKSDB_VERSION_MINOR}.${ROCKSDB_VERSION_PATCH}")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RocksDB
  REQUIRED_VARS ROCKSDB_LIBRARIES ROCKSDB_INCLUDE_DIR
  VERSION_VAR ROCKSDB_VERSION_STRING)

mark_as_advanced(
  ROCKSDB_INCLUDE_DIR
  ROCKSDB_LIBRARIES)

if(RocksDB_FOUND)
  if(NOT TARGET RocksDB::RocksDB)
    add_library(RocksDB::RocksDB UNKNOWN IMPORTED)
    set_target_properties(RocksDB::RocksDB PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${ROCKSDB_INCLUDE_DIR}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
      IMPORTED_LOCATION "${ROCKSDB_LIBRARIES}"
      VERSION "${ROCKSDB_VERSION_STRING}")
  endif()
endif()

