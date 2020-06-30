# - Find LevelDB
#
# LEVELDB_INCLUDE_DIR - Where to find leveldb/db.h
# LEVELDB_LIBRARIES - List of libraries when using LevelDB.
# LEVELDB_FOUND - True if LevelDB found.

find_path(LEVELDB_INCLUDE_DIR leveldb/db.h
  HINTS $ENV{LEVELDB_ROOT}/include
  DOC "Path in which the file leveldb/db.h is located." )

find_library(LEVELDB_LIBRARIES leveldb
  HINTS $ENV{LEVELDB_ROOT}/lib
  DOC "Path to leveldb library." )

mark_as_advanced(LEVELDB_INCLUDE_DIR LEVELDB_LIBRARIES)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(leveldb DEFAULT_MSG LEVELDB_LIBRARIES LEVELDB_INCLUDE_DIR)
