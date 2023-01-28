cmake_minimum_required(VERSION 3.14.0)
project(sqlite_db)

find_package(SQLite3 REQUIRED)

set(sqlite_db_srcs
    sqliteDB.h
    sqliteDB.cc)

include_directories(${CMAKE_INCLUDE_DIR})

set(SQLITE_COMPILE_FLAGS "-DSQLITE_THREADSAFE=1")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SQLITE_COMPILE_FLAGS}")

add_library(sqlite_db STATIC ${sqlite_db_srcs})
target_link_libraries(sqlite_db sqlite3 dbstore_lib rgw_common)
