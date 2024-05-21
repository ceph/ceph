#need to update cmake version here
cmake_minimum_required(VERSION 3.14.0)
project(dbstore)

option(USE_SQLITE "Enable SQLITE DB" ON)

set (CMAKE_INCLUDE_DIR ${CMAKE_INCLUDE_DIR} "${CMAKE_CURRENT_SOURCE_DIR}/common")

set(dbstore_srcs
    common/dbstore_log.h
    common/dbstore.h
    common/dbstore.cc
    config/store.cc)
IF(USE_SQLITE)
  list(APPEND dbstore_srcs
      config/sqlite.cc
      sqlite/connection.cc
      sqlite/error.cc
      sqlite/statement.cc)
endif()

set(dbstore_mgr_srcs
    dbstore_mgr.h
    dbstore_mgr.cc
    )

add_library(dbstore_lib ${dbstore_srcs})
target_include_directories(dbstore_lib
    PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw"
    PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw/store/rados"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
set(link_targets Boost::context)
if(WITH_JAEGER)
  list(APPEND link_targets jaeger_base)
endif()
list(APPEND link_targets rgw_common)
target_link_libraries(dbstore_lib PUBLIC ${link_targets})

set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} dbstore_lib)

IF(USE_SQLITE)
  add_subdirectory(sqlite)
  set(CMAKE_INCLUDE_DIR ${CMAKE_INCLUDE_DIR} "${CMAKE_CURRENT_SOURCE_DIR}/sqlite")
  add_compile_definitions(SQLITE_ENABLED=1)
  set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} rgw_common)
  set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} sqlite_db)
  add_dependencies(sqlite_db dbstore_lib)
ENDIF()

# add pthread library
set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} pthread)

find_package(gtest QUIET)
if(WITH_TESTS)
    add_subdirectory(tests)
else()
	message(WARNING "Gtest not enabled")
endif()

include_directories(${CMAKE_INCLUDE_DIR})
add_library(dbstore STATIC ${dbstore_mgr_srcs})
target_link_libraries(dbstore ${CMAKE_LINK_LIBRARIES})

# testing purpose
set(dbstore_main_srcs
    dbstore_main.cc)

set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} dbstore)
add_executable(dbstore-bin ${dbstore_main_srcs})
add_dependencies(dbstore-bin dbstore)
target_link_libraries(dbstore-bin ${CMAKE_LINK_LIBRARIES})
